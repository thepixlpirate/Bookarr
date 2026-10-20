"""Readarr-compatible API for SeerrNG (ebooks and audiobooks).

Lets SeerrNG (https://github.com/snapetech-org/seerrng) use Bookarr as a "Readarr / Bookshelf"
service. Everything lives under its own address so it can never collide with Bookarr's own API:

    /readarr-compat/api/v1/...            (SeerrNG "URL Base": /readarr-compat)
    /readarr-compat/MediaCover/<id>/cover.jpg

Ebook vs audiobook: SeerrNG adds ?mediaType=ebook or ?mediaType=audiobook to every request it
sends (it does this for each Bookshelf server, based on whether that server is marked ebook or
audiobook). As a backup, /readarr-compat/ebook and /readarr-compat/audiobook also work as URL Bases
and force that type. Nothing sent without either is treated as an ebook request.

Safety rules this module follows:
  * OFF by default. When Settings > SeerrNG > "Enable Readarr compatibility" is off, every route
    here answers 404, exactly as if this module did not exist.
  * Every request must carry Bookarr's API key in the X-Api-Key header (401 otherwise).
  * Files are only ever deleted when SeerrNG asks with deleteFiles=true AND the separate
    "Allow SeerrNG to delete files" switch is on.
  * No new database tables or columns. SeerrNG's book IDs are built from what Bookarr already
    stores (where the book's details came from + that site's ID), e.g. "gb:<GoogleBooksId>",
    "ol:OL123W", "au:<AudibleASIN>", or "bookarr:<item id>" for books found by a folder scan.
    SeerrNG's optional request tags are kept as one row in Bookarr's existing settings table.
  * All real work (searching, adding, downloading, listing files, deleting) is done by calling
    Bookarr's existing functions in main.py.
"""
import asyncio, json, os, re, time, zlib, pathlib, datetime
from fastapi import APIRouter, Depends, HTTPException, Request, Header, Query
from fastapi.responses import JSONResponse, FileResponse

PREFIX = "/readarr-compat"
SETTINGS = "seerrng"  # settings section, shared with mylar_compat
SETTING_DEFAULTS = {"readarrEnabled": 0, "mylarEnabled": 0, "allowDeleteFiles": 0}
TAGS_KEY = "seerrng_readarr_tags"  # row name in Bookarr's existing kv (settings) table
BOOK_TYPES = ("ebook", "audiobook")
core = None  # Bookarr's main module, set by register()


def register(app, main_module):
    """Called once from main.py. Adds the setting defaults and the routes."""
    global core
    core = main_module
    core.DEFAULTS.setdefault(SETTINGS, dict(SETTING_DEFAULTS))
    for k, v in SETTING_DEFAULTS.items(): core.DEFAULTS[SETTINGS].setdefault(k, v)
    for sub, forced in (("", None), ("/ebook", "ebook"), ("/audiobook", "audiobook")):
        app.include_router(_router(forced), prefix=PREFIX + sub)


# ---------------------------------------------------------------- access checks
def _on(key):
    try: return int(core.cfg(SETTINGS).get(key) or 0) == 1
    except (TypeError, ValueError): return False

def _guard(x_api_key: str = Header(None)):
    if not _on("readarrEnabled"): raise HTTPException(404, "Not Found")  # behave as if this module was not there
    if not x_api_key or x_api_key != core.cfg("general")["apiKey"]: raise HTTPException(401, "Unauthorized")

def _kind(forced):
    def pick(mediaType: str = Query(None)):
        if forced: return forced
        return mediaType if mediaType in BOOK_TYPES else "ebook"
    return pick


# ---------------------------------------------------------------- id helpers
PREFIXES = {"googlebooks": "gb", "openlibrary": "ol", "audible": "au"}
REVERSE = {v: k for k, v in PREFIXES.items()}

def foreign_id(it):
    """Stable SeerrNG book id built from what Bookarr already stores. Bookarr can always resolve it again."""
    p, e = it.get("provider"), (it.get("ext_id") or "").strip()
    if p in PREFIXES and e:
        if p == "openlibrary": e = e.replace("/works/", "")
        return f"{PREFIXES[p]}:{e}"
    return f"bookarr:{it['id']}" if it.get("id") else None

def author_key(name):
    return "name:" + re.sub(r"\s+", "-", core.norm(name or "") or "unknown-author")

def author_num(name):
    return (zlib.crc32(author_key(name).encode()) & 0x7FFFFFFF) or 1

def first_author(name):
    return ((name or "").split(",")[0]).strip()

def isbn13(v):
    s = re.sub(r"[^0-9Xx]", "", v or "").upper()
    if len(s) == 13 and s.isdigit(): return s
    if len(s) == 10:
        b = "978" + s[:9]
        return b + str((10 - sum((3 if i % 2 else 1) * int(c) for i, c in enumerate(b)) % 10) % 10)
    return None

def parse_isbn(term):
    t = re.sub(r"^isbn[:\s]*", "", (term or "").strip(), flags=re.I)
    t = re.sub(r"[\s-]", "", t)
    return isbn13(t) if re.fullmatch(r"\d{9}[\dXx]|\d{13}", t) else None

def iso(v):
    if not v: return None
    s = str(v).replace(" ", "T")
    if re.fullmatch(r"\d{4}", s): s += "-01-01"
    if re.fullmatch(r"\d{4}-\d{2}", s): s += "-01"
    return s if "T" in s else s + "T00:00:00Z"


# ---------------------------------------------------------------- shaping data the way SeerrNG reads it
def _root(t): return core.cfg("library")[t]

def _cover_images(it, remote=None):
    imgs = []
    if it.get("id"): imgs.append({"coverType": "cover", "url": f"/MediaCover/{it['id']}/cover.jpg"})
    if remote or it.get("cover"): imgs.append({"coverType": "cover", "remoteUrl": remote or it.get("cover"), "url": remote or it.get("cover")})
    return imgs

def _editions(fid, title, isbn, monitored=True, book_id=None):
    ed = {"foreignEditionId": f"{fid}#ed", "title": title, "monitored": bool(monitored), "isEbook": True}
    if isbn: ed["isbn13"] = isbn
    if book_id: ed.update(id=book_id, bookId=book_id)
    return [ed]

def book_obj(it, t):
    """A Bookarr library item, shown to SeerrNG as a Readarr book."""
    meta = core.item_meta(it); fid = foreign_id(it); name = first_author(it.get("author")) or "Unknown Author"
    path = it.get("path"); has = bool(path and it.get("status") == "downloaded" and os.path.exists(path))
    size = 0
    if has:
        try: size = os.path.getsize(path)
        except OSError: pass
    n = 1 if has else 0
    stats = {"bookFileCount": n, "bookCount": 1, "totalBookCount": 1, "sizeOnDisk": size, "percentOfBooks": 100.0 if has else 0.0,
             ("audiobookFileCount" if t == "audiobook" else "ebookFileCount"): n}
    return {
        "id": it["id"], "title": it["title"], "titleSlug": fid, "foreignBookId": fid, "foreignEditionId": f"{fid}#ed", "mediaType": t,
        "monitored": bool(it.get("monitored")) or has, "anyEditionOk": True, "added": iso(it.get("added")),
        "releaseDate": iso(it.get("release_date") or it.get("year")), "overview": it.get("description") or "",
        "authorId": author_num(name), "author": {"id": author_num(name), "foreignAuthorId": author_key(name), "authorName": name},
        "authorTitle": name, "seriesTitle": it.get("series") or "", "genres": meta.get("genres") or [], "pageCount": meta.get("pages") or 0,
        "ratings": {"value": meta.get("rating") or 0, "votes": meta.get("ratings") or 0},
        "qualityProfileId": 1, "metadataProfileId": 1, "rootFolderPath": _root(t), "tags": [],
        "editions": _editions(fid, it["title"], isbn13(meta.get("isbn")), True, it["id"]),
        "images": _cover_images(it), "statistics": stats, "grabbed": it.get("status") == "wanted",
    }

def lookup_obj(d, t, isbn=None):
    """A search result (Google Books / Open Library / Audible), shown to SeerrNG as a Readarr lookup result."""
    fid = foreign_id(d) or f"bookarr:new:{core.norm(d.get('title'))}"
    name = first_author(d.get("author")) or "Unknown Author"
    meta = d.get("meta") if isinstance(d.get("meta"), dict) else core.item_meta(d)  # library rows store details as text
    out = {
        "title": d.get("title") or "", "titleSlug": fid, "foreignBookId": fid, "foreignEditionId": f"{fid}#ed", "mediaType": t,
        "author": {"foreignAuthorId": author_key(name), "authorName": name}, "authorTitle": name,
        "editions": _editions(fid, d.get("title") or "", isbn or isbn13(meta.get("isbn"))),
        "images": [{"coverType": "cover", "remoteUrl": d["cover"], "url": d["cover"]}] if d.get("cover") else [],
        "releaseDate": iso(d.get("release_date") or d.get("year")), "overview": d.get("description") or "",
        "genres": meta.get("genres") or [], "subjects": meta.get("subjects") or [], "pageCount": meta.get("pages") or 0,
        "ratings": {"value": meta.get("rating") or 0, "votes": meta.get("ratings") or 0}, "seriesTitle": d.get("series") or "",
        "monitored": False,
    }
    if meta.get("narrator"): out["narrators"] = [x.strip() for x in meta["narrator"].split(",") if x.strip()]
    if meta.get("runtime"): out["audiobookDuration"] = int(meta["runtime"]) * 60
    existing = core.find_existing({**d, "type": t})
    if existing: out.update(id=existing, monitored=True)
    return out

def _items(t, where="1=1", args=()):
    return core.rows(f"SELECT * FROM item WHERE type=? AND {where} ORDER BY id", (t, *args))

def _item(iid, t):
    r = core.rows("SELECT * FROM item WHERE id=? AND type=?", (iid, t))
    if not r: raise HTTPException(404, "Book not found")
    return r[0]

def _by_foreign(fid, t):
    if not fid: return None
    m = re.fullmatch(r"bookarr:(\d+)", fid)
    if m:
        r = core.rows("SELECT * FROM item WHERE id=? AND type=?", (int(m.group(1)), t)); return r[0] if r else None
    for it in _items(t, "provider IS NOT NULL AND ext_id IS NOT NULL"):
        if foreign_id(it) == fid: return it
    return None


# ---------------------------------------------------------------- looking things up (Bookarr's own metadata functions)
async def resolve(fid, t):
    """Turn a SeerrNG book id back into book details (library first, then the original site)."""
    it = _by_foreign(fid, t)
    if it: return dict(it)
    m = re.fullmatch(r"(gb|ol|au):(.+)", fid or "")
    if not m: return None
    p, e = REVERSE[m.group(1)], m.group(2)
    try:
        if p == "googlebooks":
            d = core.gb_item(await core.jget(f"https://www.googleapis.com/books/v1/volumes/{e}"), t)
        elif p == "audible":
            d = core.audible_item((await core.jget(f"https://api.audible.{core.cfg('metadata')['audibleRegion']}/1.0/catalog/products/{e}",
                                                   params={"response_groups": core.AUDIBLE_GROUPS}))["product"])
        else:
            w = await core.jget(f"https://openlibrary.org/works/{e}.json"); name = None
            ak = ((w.get("authors") or [{}])[0].get("author") or {}).get("key")
            if ak:
                try: name = (await core.jget(f"https://openlibrary.org{ak}.json")).get("name")
                except Exception: pass
            d = {"type": t, "title": w.get("title"), "author": name, "provider": "openlibrary", "ext_id": f"/works/{e}",
                 "cover": f"https://covers.openlibrary.org/b/id/{w['covers'][0]}-L.jpg" if w.get("covers") else None, "description": ""}
        d["type"] = t; return d
    except Exception as ex:
        core.log.info("SeerrNG lookup of %s failed: %s", fid, ex); return None

async def search(term, t):
    term = (term or "").strip()
    if not term: return []
    if re.fullmatch(r"(gb|ol|au|bookarr):.+", term):
        d = await resolve(term, t); return [lookup_obj(d, t)] if d else []
    isbn = parse_isbn(term)
    if isbn:
        books = await core.look_ebook(f"isbn:{isbn}")
        if t == "ebook": return [lookup_obj(d, t, isbn) for d in books]
        # Audible cannot search by ISBN: find the title from the ISBN first, then look for the audiobook.
        if not books: return []
        title, who = books[0]["title"], core.norm(first_author(books[0].get("author")))
        res = await core.look_audio(title)
        same = [d for d in res if core.norm(d["title"]).startswith(core.norm(title)) and (not who or who in core.norm(d.get("author")))]
        rest = [d for d in res if d not in same]
        return [lookup_obj(d, t, isbn) for d in same] + [lookup_obj(d, t) for d in rest]
    found = await (core.look_audio(term) if t == "audiobook" else core.look_ebook(term))
    return [lookup_obj(d, t) for d in found if d.get("title")]


# ---------------------------------------------------------------- commands (searches run in the background)
COMMANDS, _seq = {}, [0]

def _now(): return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _cmd_view(c): return {k: v for k, v in c.items() if not k.startswith("_")}

def new_command(name, body, t):
    _seq[0] += 1; cid = _seq[0]
    c = {"id": cid, "name": name, "commandName": name, "status": "queued", "queued": _now(), "trigger": "manual", "body": body, "_t": t}
    COMMANDS[cid] = c
    for old in sorted(COMMANDS)[:-200]: COMMANDS.pop(old, None)
    asyncio.create_task(_run(c)); return c

async def _search_ids(ids, t):
    sent, errors = 0, []
    for iid in ids:
        r = core.rows("SELECT * FROM item WHERE id=? AND type=?", (iid, t))
        if not r or r[0]["status"] != "missing": continue  # already downloading or downloaded
        try: sent += 1 if await core.search_item(r[0]) else 0
        except HTTPException as e: errors.append(str(e.detail))
        await asyncio.sleep(0.5)
    return sent, errors

async def _run(c):
    c["status"] = "started"; c["started"] = _now(); b, t, name = c["body"], c["_t"], c["name"]
    try:
        if name == "BookSearch":
            sent, errors = await _search_ids([int(i) for i in b.get("bookIds") or []], t)
        elif name in ("AuthorSearch", "MissingBookSearch"):
            its = _items(t, "monitored=1 AND status='missing'")
            if name == "AuthorSearch":
                its = [i for i in its if author_num(first_author(i["author"])) == int(b.get("authorId") or 0)]
            sent, errors = await _search_ids([i["id"] for i in its], t)
        elif name == "RefreshMonitoredDownloads":
            await core.check_downloads(); sent, errors = 0, []
        else:
            sent, errors = 0, []; c["message"] = f"{name} is not used by Bookarr; nothing to do"
        if errors and not sent: c.update(status="failed", result="unsuccessful", message="; ".join(errors)[:500])
        else: c.update(status="completed", result="successful", message=c.get("message") or f"Sent {sent} download(s)")
    except Exception as e:
        c.update(status="failed", result="unsuccessful", message=str(e)[:500]); core.log.warning("SeerrNG command %s failed: %s", name, e)
    c["ended"] = _now()


# ---------------------------------------------------------------- tags (kept in Bookarr's existing settings table)
def _tags():
    r = core.rows("SELECT v FROM kv WHERE k=?", (TAGS_KEY,))
    try: return json.loads(r[0]["v"]) if r else []
    except ValueError: return []

def _save_tags(tags):
    core.db.execute("REPLACE INTO kv VALUES(?,?)", (TAGS_KEY, json.dumps(tags))); core.db.commit()


# ---------------------------------------------------------------- queue / history
def _grabs(t, book_id=None):
    q = "SELECT * FROM grab WHERE type=?" + (" AND item_id=?" if book_id else "") + " ORDER BY id DESC LIMIT 1000"
    return core.rows(q, (t, book_id) if book_id else (t,))

def _hms(eta):
    if not eta: return "00:00:00"
    if re.fullmatch(r"\d+:\d{2}:\d{2}", eta): return eta.zfill(8)
    h = re.search(r"(\d+)h", eta); m = re.search(r"(\d+)m", eta); s = re.search(r"(\d+)s", eta)
    return "%02d:%02d:%02d" % (int(h.group(1)) if h else 0, int(m.group(1)) if m else 0, int(s.group(1)) if s else 0)

async def queue_records(t):
    live = [x for x in await core.live_progress() if x.get("type") == t]
    g = {r["id"]: r for r in core.rows("SELECT id, dl_id, protocol FROM grab WHERE state='sent'")}
    out = []
    for x in live:
        p = float(x.get("progress") or 0); size = 100_000_000  # Bookarr tracks percent done, not bytes, so report percent on a fixed scale
        done = p >= 100; prob = x.get("problem")
        out.append({"id": x["grab_id"], "bookId": x.get("item_id"), "book": {"id": x.get("item_id")}, "title": x["title"], "size": size,
                    "sizeleft": int(size * (1 - min(p, 100) / 100)), "timeleft": _hms(x.get("eta")), "estimatedCompletionTime": _now(),
                    "status": "completed" if done else "downloading" if x.get("status") == "Downloading" else "queued",
                    "trackedDownloadStatus": "warning" if prob else "ok",
                    "trackedDownloadState": "importFailed" if prob else "importPending" if done else "downloading",
                    "statusMessages": [{"title": x["title"], "messages": [prob]}] if prob else [],
                    "downloadId": (g.get(x["grab_id"]) or {}).get("dl_id") or "", "protocol": (g.get(x["grab_id"]) or {}).get("protocol") or "unknown",
                    "downloadClient": x.get("client") or x.get("kind") or "", "indexer": ""})
    return out

def history_records(t, book_id=None):
    out = []
    for g in _grabs(t, book_id):
        base = {"bookId": g["item_id"] or 0, "downloadId": g["dl_id"] or "", "sourceTitle": g["title"], "date": iso(g["created"])}
        out.append({**base, "id": g["id"] * 10 + 1, "eventType": "grabbed"})
        if g["state"] == "imported": out.append({**base, "id": g["id"] * 10 + 2, "eventType": "bookFileImported"})
        if g["state"] == "failed": out.append({**base, "id": g["id"] * 10 + 3, "eventType": "downloadFailed", "data": {"message": g["note"] or ""}})
    return sorted(out, key=lambda r: (r["date"] or "", r["id"]), reverse=True)

def _page(records, page, page_size):
    page, page_size = max(1, page or 1), min(max(1, page_size or 10), 1000)
    return {"page": page, "pageSize": page_size, "sortKey": "date", "sortDirection": "descending", "totalRecords": len(records),
            "records": records[(page - 1) * page_size: page * page_size]}


# ---------------------------------------------------------------- authors
def authors(t):
    names = {}
    for it in _items(t, "author IS NOT NULL AND author!=''"):
        n = first_author(it["author"]); names.setdefault(author_num(n), n)
    for s in core.rows("SELECT title FROM series WHERE provider='author' AND type=?", (t,)):
        names.setdefault(author_num(s["title"]), s["title"])
    return [{"id": i, "foreignAuthorId": author_key(n), "authorName": n, "sortName": n, "monitored": True, "path": os.path.join(_root(t), n),
             "rootFolderPath": _root(t), "qualityProfileId": 1, "metadataProfileId": 1, "images": [], "tags": [],
             "statistics": {"bookFileCount": sum(1 for it in _items(t, "author IS NOT NULL") if author_num(first_author(it["author"])) == i and it["status"] == "downloaded")}}
            for i, n in sorted(names.items(), key=lambda x: x[1].lower())]


# ---------------------------------------------------------------- the routes
def _router(forced):
    r = APIRouter(dependencies=[Depends(_guard)], include_in_schema=False)
    kind = _kind(forced)
    base = PREFIX + ("/" + forced if forced else "")
    v1 = "/api/v1"

    @r.get(v1 + "/system/status")
    def status(t: str = Depends(kind)):
        return {"appName": "Bookarr", "instanceName": "Bookarr", "version": "0.3.7", "urlBase": base, "isDocker": True, "isLinux": True,
                "authentication": "external", "branch": "main", "startTime": _now(), "mediaType": t}

    @r.get(v1 + "/system/capabilities")
    def capabilities(): raise HTTPException(404, "Not Found")  # SeerrNG treats this as optional

    @r.get(v1 + "/config/hardcover")
    def hardcover(): raise HTTPException(404, "Not Found")  # optional; Bookarr is not Chaptarr

    @r.get(v1 + "/config/development")
    def development(): return {"id": 1, "metadataSource": "Google Books / Open Library / Audible (Bookarr)"}

    @r.get(v1 + "/rootfolder")
    def rootfolder(t: str = Depends(kind)):
        p = _root(t)
        try: u = core.shutil.disk_usage(p); free, total = u.free, u.total
        except OSError: free = total = 0
        return [{"id": 1, "name": t.capitalize() + "s", "path": p, "freeSpace": free, "totalSpace": total, "accessible": os.path.isdir(p),
                 "defaultQualityProfileId": 1, "defaultMetadataProfileId": 1, "unmappedFolders": []}]

    @r.get(v1 + "/qualityProfile")
    @r.get(v1 + "/qualityprofile")
    def quality(): return [{"id": 1, "name": "Any (Bookarr quality settings)"}]

    @r.get(v1 + "/metadataProfile")
    @r.get(v1 + "/metadataprofile")
    def metadata_profiles(): return [{"id": 1, "name": "Standard"}]

    @r.get(v1 + "/tag")
    def tags(): return _tags()

    @r.post(v1 + "/tag")
    async def tag_add(request: Request):
        label = str((await request.json()).get("label") or "").strip()[:200]
        if not label: raise HTTPException(400, "label is required")
        ts = _tags(); hit = next((x for x in ts if x["label"].lower() == label.lower()), None)
        if hit: return hit
        tag = {"id": max([x["id"] for x in ts] + [0]) + 1, "label": label}; _save_tags(ts + [tag]); return JSONResponse(tag, 201)

    @r.put(v1 + "/tag/{tid}")
    async def tag_edit(tid: int, request: Request):
        label = str((await request.json()).get("label") or "").strip()[:200]; ts = _tags()
        for x in ts:
            if x["id"] == tid: x["label"] = label or x["label"]; _save_tags(ts); return x
        raise HTTPException(404, "Tag not found")

    @r.delete(v1 + "/tag/{tid}")
    def tag_del(tid: int): _save_tags([x for x in _tags() if x["id"] != tid]); return {}

    # ----- searching
    @r.get(v1 + "/book/lookup")
    async def book_lookup(term: str = "", t: str = Depends(kind)): return await search(term, t)

    @r.get(v1 + "/author/lookup")
    async def author_lookup(term: str = "", t: str = Depends(kind)):
        found, seen = [], set()
        try: res = await (core.look_audio(term) if t == "audiobook" else core.look_ebook(f'inauthor:"{term}"'))
        except Exception: res = []
        for d in res:
            n = first_author(d.get("author"))
            if n and author_key(n) not in seen: seen.add(author_key(n)); found.append(n)
        if term.strip() and author_key(term) not in seen: found.insert(0, term.strip())
        mine = {a["foreignAuthorId"]: a["id"] for a in authors(t)}
        return [{"foreignAuthorId": author_key(n), "authorName": n, "images": [], **({"id": mine[author_key(n)]} if author_key(n) in mine else {})}
                for n in found[:20]]

    # ----- authors in Bookarr
    @r.get(v1 + "/author")
    def author_list(t: str = Depends(kind)): return authors(t)

    @r.get(v1 + "/author/{aid}")
    def author_get(aid: int, t: str = Depends(kind)):
        a = next((x for x in authors(t) if x["id"] == aid), None)
        if not a: raise HTTPException(404, "Author not found")
        return a

    @r.delete(v1 + "/author/{aid}")
    def author_del(aid: int, deleteFiles: str = "false", t: str = Depends(kind)):
        files = deleteFiles.lower() == "true" and _on("allowDeleteFiles"); n = 0
        for it in _items(t, "author IS NOT NULL"):
            if author_num(first_author(it["author"])) == aid: core.remove_item(it["id"], files); n += 1
        for s in core.rows("SELECT id, title FROM series WHERE provider='author' AND type=?", (t,)):
            if author_num(s["title"]) == aid: core.series_del(s["id"])
        core.log.info("SeerrNG removed author %s (%d titles, files %s)", aid, n, "deleted" if files else "kept")
        return {}

    # ----- books in Bookarr
    @r.get(v1 + "/book")
    def book_list(authorId: int = None, bookIds: str = None, t: str = Depends(kind)):
        its = _items(t)
        if authorId: its = [i for i in its if author_num(first_author(i["author"])) == authorId]
        if bookIds:
            want = {int(x) for x in re.findall(r"\d+", bookIds)}; its = [i for i in its if i["id"] in want]
        return [book_obj(i, t) for i in its]

    @r.get(v1 + "/book/paged")
    def book_paged(offset: int = None, page: int = 1, pageSize: int = 50, includeUnmonitored: bool = True, t: str = Depends(kind)):
        pageSize = min(max(1, pageSize), 1000); start = offset if offset is not None else (max(1, page) - 1) * pageSize
        its = _items(t) if includeUnmonitored else _items(t, "monitored=1 OR status='downloaded'")
        recs = [book_obj(i, t) for i in its[start:start + pageSize]]
        return {"offset": start, "page": start // pageSize + 1, "pageSize": pageSize, "totalCount": len(its), "totalRecords": len(its), "records": recs}

    @r.get(v1 + "/book/{bid}")
    def book_get(bid: int, t: str = Depends(kind)): return book_obj(_item(bid, t), t)

    @r.post(v1 + "/book")
    async def book_add(request: Request, t: str = Depends(kind)):
        b = await request.json(); fid = (b.get("foreignBookId") or "").strip()
        if not fid and not b.get("title"): raise HTTPException(400, "foreignBookId or title is required")
        monitored = b.get("monitored") is not False; search_now = bool((b.get("addOptions") or {}).get("searchForNewBook"))
        it = _by_foreign(fid, t); created = False
        if not it:
            d = await resolve(fid, t) or {"type": t, "title": b.get("title"), "author": ((b.get("author") or {}).get("authorName")), "description": b.get("overview") or ""}
            d = {k: v for k, v in d.items() if k not in ("id", "issues", "num")}; d["type"] = t
            if not d.get("title"): raise HTTPException(400, "Could not find that book")
            if not d.get("description") or not d.get("meta"):
                try: d = await core.enrich(d)
                except Exception: pass
            iid = core.find_existing(d)  # Bookarr's own duplicate check (same title + author, or same source id)
            if not iid: iid = core.insert_item({**d, "monitored": 1 if monitored else 0, "status": "missing"}); created = True
            it = _item(iid, t)
        if monitored and not it["monitored"]:
            core.db.execute("UPDATE item SET monitored=1 WHERE id=?", (it["id"],)); core.db.commit(); it = _item(it["id"], t)
        if search_now and it["status"] == "missing": new_command("BookSearch", {"bookIds": [it["id"]]}, t)
        core.log.info("SeerrNG %s %s: %s", "added" if created else "requested existing", t, it["title"])
        return JSONResponse(book_obj(it, t), 201 if created else 200)

    @r.put(v1 + "/book/monitor")
    async def book_monitor(request: Request, t: str = Depends(kind)):
        b = await request.json()
        for bid in b.get("bookIds") or []:
            core.db.execute("UPDATE item SET monitored=? WHERE id=? AND type=?", (1 if b.get("monitored") else 0, int(bid), t))
        core.db.commit(); return JSONResponse([book_obj(_item(int(x), t), t) for x in b.get("bookIds") or []], 202)

    @r.put(v1 + "/book/{bid}")
    async def book_put(bid: int, request: Request, t: str = Depends(kind)):
        it = _item(bid, t); b = await request.json()
        mon = b.get("monitored", b.get(f"{t}Monitored"))
        if isinstance(mon, bool):
            core.db.execute("UPDATE item SET monitored=? WHERE id=?", (1 if mon else 0, bid)); core.db.commit()
        if (b.get("addOptions") or {}).get("searchForNewBook"): new_command("BookSearch", {"bookIds": [bid]}, t)
        return JSONResponse(book_obj(_item(bid, t), t), 202)

    @r.delete(v1 + "/book/{bid}")
    def book_del(bid: int, deleteFiles: str = "false", t: str = Depends(kind)):
        _item(bid, t); files = deleteFiles.lower() == "true" and _on("allowDeleteFiles")
        n = core.remove_item(bid, files)
        core.log.info("SeerrNG removed book %s (%s)", bid, f"deleted {n} file(s)" if files else "files kept")
        return {}

    @r.get(v1 + "/edition")
    def editions(bookId: int, t: str = Depends(kind)): return book_obj(_item(bookId, t), t)["editions"]

    @r.get(v1 + "/bookfile")
    def bookfiles(bookId: int = None, t: str = Depends(kind)):
        its = [_item(bookId, t)] if bookId else _items(t, "status='downloaded'")
        out = []
        for it in its:
            for n, f in enumerate(f for f in core.file_list(it) if f["exists"]):
                root = _root(t)
                out.append({"id": it["id"] * 1000 + n + 1, "bookId": it["id"], "authorId": author_num(first_author(it["author"])), "path": f["path"],
                            "relativePath": os.path.relpath(f["path"], root) if f["path"].startswith(root) else f["name"], "size": f["size"] or 0,
                            "dateAdded": iso(f.get("modified")), "quality": {"quality": {"id": 0, "name": f["ext"]}}})
        return out

    @r.get(v1 + "/calendar")
    def calendar(start: str = "0000", end: str = "9999", t: str = Depends(kind)):
        return [book_obj(i, t) for i in core.calendar(start[:10], end[:10]) if i["type"] == t]

    # ----- activity
    @r.get(v1 + "/queue")
    async def queue(page: int = 1, pageSize: int = 50, bookIds: str = None, t: str = Depends(kind)):
        recs = await queue_records(t)
        if bookIds: want = {int(x) for x in re.findall(r"\d+", bookIds)}; recs = [x for x in recs if x["bookId"] in want]
        return _page(recs, page, pageSize)

    @r.get(v1 + "/queue/details")
    async def queue_details(t: str = Depends(kind)): return await queue_records(t)

    @r.get(v1 + "/history")
    def history(page: int = 1, pageSize: int = 50, bookId: int = None, t: str = Depends(kind)):
        return _page(history_records(t, bookId), page, pageSize)

    @r.get(v1 + "/history/book")
    def history_book(bookId: int, t: str = Depends(kind)): return history_records(t, bookId)

    @r.post(v1 + "/command")
    async def command(request: Request, t: str = Depends(kind)):
        b = await request.json(); name = str(b.get("name") or "").strip()
        if not name: raise HTTPException(400, "name is required")
        return JSONResponse(_cmd_view(new_command(name, b, t)), 201)

    @r.get(v1 + "/command/{cid}")
    def command_get(cid: int):
        if cid not in COMMANDS: raise HTTPException(404, "Command not found")
        return _cmd_view(COMMANDS[cid])

    @r.get(v1 + "/command")
    def command_list(): return [_cmd_view(c) for c in COMMANDS.values()]

    # ----- covers (on the app root, not under /api/v1)
    @r.get("/MediaCover/{bid}/{name}")
    async def media_cover(bid: int, name: str):
        if not name.lower().startswith(("cover", "poster")): raise HTTPException(404, "Not Found")
        if not core.rows("SELECT 1 FROM item WHERE id=? AND type IN ('ebook','audiobook')", (bid,)): raise HTTPException(404, "Not Found")
        return await core.cover_file(bid)  # Bookarr's own cover cache; 404 when the title has no cover

    return r
