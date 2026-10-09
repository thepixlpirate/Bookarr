"""Bookarr: comics, ebooks, audiobooks. Indexers via Prowlarr, clients: SABnzbd + qBittorrent."""
import os, shutil, re, json, secrets, sqlite3, asyncio, pathlib, logging, base64, collections
from contextlib import asynccontextmanager
from urllib.parse import quote
import httpx
from fastapi import FastAPI, Depends, HTTPException, Header, Query, Request
from fastapi.responses import FileResponse, Response, PlainTextResponse
from fastapi.staticfiles import StaticFiles

DATA = pathlib.Path(os.getenv("DATA_DIR", "/config")); DATA.mkdir(parents=True, exist_ok=True)
db = sqlite3.connect(DATA / "bookarr.db", check_same_thread=False); db.row_factory = sqlite3.Row
db.executescript("""
CREATE TABLE IF NOT EXISTS item(id INTEGER PRIMARY KEY, type TEXT, title TEXT, author TEXT, series TEXT, year INT,
  monitored INT DEFAULT 1, status TEXT DEFAULT 'missing', path TEXT, added TEXT DEFAULT CURRENT_TIMESTAMP, release_date TEXT);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS client(id INTEGER PRIMARY KEY, name TEXT, kind TEXT, url TEXT, apikey TEXT,
  username TEXT, password TEXT, category TEXT, enabled INT DEFAULT 1);""")

db.executescript("""CREATE TABLE IF NOT EXISTS grab(id INTEGER PRIMARY KEY, item_id INT, type TEXT, title TEXT, protocol TEXT, client_id INT, dl_id TEXT,
  state TEXT DEFAULT 'sent', note TEXT, created TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS blocklist(title TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS series(id INTEGER PRIMARY KEY, type TEXT, title TEXT, author TEXT, provider TEXT, ext_id TEXT, cover TEXT, description TEXT, year INT, publisher TEXT, monitored INT DEFAULT 1, added TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS device(id INTEGER PRIMARY KEY, name TEXT, key TEXT, created TEXT DEFAULT CURRENT_TIMESTAMP, last_used TEXT);""")
for col in ("series_id INT", "num TEXT", "last_search TEXT", "description TEXT", "cover TEXT", "provider TEXT", "ext_id TEXT", "publisher TEXT"):
    try: db.execute(f"ALTER TABLE item ADD COLUMN {col}")
    except sqlite3.OperationalError: pass

LOGS = collections.deque(maxlen=500)
class Mem(logging.Handler):
    def emit(self, r): LOGS.append({"time": self.format(r).split("|")[0], "level": r.levelname, "message": r.getMessage()})
log = logging.getLogger("bookarr"); log.setLevel(logging.INFO)
h = Mem(); h.setFormatter(logging.Formatter("%(asctime)s|")); log.addHandler(h); log.addHandler(logging.StreamHandler())

TYPES = {"comic": 7030, "ebook": 7020, "audiobook": 3030}  # Newznab categories
DEFAULTS = {
    "general": {"logLevel": "info", "apiKey": ""},
    "indexer": {"prowlarrUrl": "", "prowlarrKey": ""},
    "library": {"comic": "/comics", "ebook": "/books", "audiobook": "/audiobooks", "downloads": "/downloads"},
    "media": {"autoSearchMinutes": 60, "minSeeders": 1, "importMode": "auto", "remotePath": "", "localPath": ""},
    "metadata": {"comicvineKey": "", "googleBooksKey": "", "audibleRegion": "com", "writeTags": 1},
    "quality": {"comic": "cbz,cbr,pdf", "ebook": "epub,azw3,mobi,pdf", "audiobook": "m4b,mp3", "comicMaxMB": 500, "ebookMaxMB": 200, "audiobookMaxMB": 3000, "requirePreferred": 0},
    "notifications": {"onGrab": 1, "onImport": 1, "onFail": 1, "discordWebhook": "", "pushoverUser": "", "pushoverToken": "", "telegramToken": "", "telegramChat": "", "webhookUrl": ""},
    "email": {"smtpHost": "", "smtpPort": 587, "user": "", "password": "", "from": "", "kindleAddress": ""},
    "security": {"authMode": "none", "username": "", "password": ""},
}
def cfg(sec):
    r = db.execute("SELECT v FROM kv WHERE k=?", (sec,)).fetchone()
    return {**DEFAULTS[sec], **(json.loads(r["v"]) if r else {})}
def setcfg(sec, val):
    db.execute("REPLACE INTO kv VALUES(?,?)", (sec, json.dumps({**cfg(sec), **val}))); db.commit()
if not cfg("general")["apiKey"]: setcfg("general", {"apiKey": secrets.token_hex(16)})

def auth(request: Request, x_api_key: str = Header(None), apikey: str = Query(None)):
    if (x_api_key or apikey) != cfg("general")["apiKey"]: raise HTTPException(401, "Invalid API key")
API = [Depends(auth)]
rows = lambda q, a=(): [dict(r) for r in db.execute(q, a).fetchall()]

@asynccontextmanager
async def life(app):
    ts = [asyncio.create_task(auto_search_loop()), asyncio.create_task(import_loop()), asyncio.create_task(maint_loop())]; yield
    for t in ts: t.cancel()
app = FastAPI(title="Bookarr", lifespan=life)

@app.middleware("http")
async def basic_auth(request: Request, call_next):
    s = cfg("security")
    if s["authMode"] == "basic" and not request.url.path.startswith(("/api/", "/opds", "/ping")):
        want = "Basic " + base64.b64encode(f'{s["username"]}:{s["password"]}'.encode()).decode()
        if request.headers.get("authorization") != want:
            return Response("Auth required", 401, {"WWW-Authenticate": 'Basic realm="Bookarr"'})
    return await call_next(request)

@app.get("/initialize.js")
def init(): return Response(f'window.BOOKARR={{apiKey:"{cfg("general")["apiKey"]}"}}', media_type="text/javascript")

# ---------- system / config ----------
@app.get("/api/v1/system/status", dependencies=API)
def status():
    return {"appName": "Bookarr", "version": "0.1.0", "counts": {t: rows("SELECT COUNT(*) c FROM item WHERE type=?", (t,))[0]["c"] for t in TYPES}}
@app.get("/api/v1/log", dependencies=API)
def get_log(): return list(LOGS)[::-1]
@app.get("/api/v1/config/{sec}", dependencies=API)
def get_cfg(sec: str):
    if sec not in DEFAULTS: raise HTTPException(404)
    return cfg(sec)
@app.put("/api/v1/config/{sec}", dependencies=API)
async def put_cfg(sec: str, request: Request):
    if sec not in DEFAULTS: raise HTTPException(404)
    setcfg(sec, await request.json()); return cfg(sec)

# ---------- download clients ----------
@app.get("/api/v1/downloadclient", dependencies=API)
def clients(): return rows("SELECT * FROM client")
@app.post("/api/v1/downloadclient", dependencies=API)
async def add_client(request: Request):
    b = await request.json()
    c = db.execute("INSERT INTO client(name,kind,url,apikey,username,password,category) VALUES(?,?,?,?,?,?,?)",
        (b.get("name"), b["kind"], b["url"].rstrip("/"), b.get("apikey", ""), b.get("username", ""), b.get("password", ""), b.get("category", ""))); db.commit()
    return {"id": c.lastrowid}
@app.delete("/api/v1/downloadclient/{cid}", dependencies=API)
def del_client(cid: int): db.execute("DELETE FROM client WHERE id=?", (cid,)); db.commit(); return {}

async def send_to_client(rel: dict, mtype: str, item_id: int = None):
    kind = "sabnzbd" if rel["protocol"] == "usenet" else "qbittorrent"
    c = db.execute("SELECT * FROM client WHERE kind=? AND enabled=1 LIMIT 1", (kind,)).fetchone()
    if not c: raise HTTPException(400, f"No enabled {kind} client configured")
    cat = c["category"] or {"comic": "comics", "ebook": "books", "audiobook": "audiobooks"}[mtype]
    gid = db.execute("INSERT INTO grab(item_id,type,title,protocol,client_id) VALUES(?,?,?,?,?)", (item_id, mtype, rel["title"], rel["protocol"], c["id"])).lastrowid
    try:
        async with httpx.AsyncClient(timeout=30) as x:
            if kind == "sabnzbd":
                r = await x.get(c["url"] + "/api", params={"mode": "addurl", "name": rel["downloadUrl"], "nzbname": rel["title"], "cat": cat, "apikey": c["apikey"], "output": "json"})
                dl = ((r.json().get("nzo_ids") or [None])[0]) if r.status_code == 200 else None
            else:
                await x.post(c["url"] + "/api/v2/auth/login", data={"username": c["username"], "password": c["password"]})
                dl = f"bookarr-{gid}"
                r = await x.post(c["url"] + "/api/v2/torrents/add", data={"urls": rel["downloadUrl"], "category": cat, "tags": dl})
            if r.status_code != 200 or not dl: raise HTTPException(502, f"{kind} did not accept the release")
    except Exception:
        db.execute("DELETE FROM grab WHERE id=?", (gid,)); db.commit(); raise
    db.execute("UPDATE grab SET dl_id=? WHERE id=?", (dl, gid))
    if item_id: db.execute("UPDATE item SET status='wanted' WHERE id=? AND status='missing'", (item_id,))
    db.commit(); log.info("Grabbed %s via %s", rel["title"], kind); await notify("onGrab", f"Grabbed: {rel['title']}")

@app.get("/api/v1/queue", dependencies=API)
async def queue():
    out = []
    for c in rows("SELECT * FROM client WHERE enabled=1 AND kind='sabnzbd'"):
        try:
            async with httpx.AsyncClient(timeout=10) as x:
                j = (await x.get(c["url"] + "/api", params={"mode": "queue", "apikey": c["apikey"], "output": "json"})).json()
            out += [{"title": s["filename"], "progress": s["percentage"], "status": s["status"], "client": c["name"]} for s in j["queue"]["slots"]]
        except Exception as e: log.warning("Queue fetch failed: %s", e)
    for g in rows("SELECT g.dl_id, g.title, c.url, c.username, c.password, c.name FROM grab g JOIN client c ON c.id=g.client_id WHERE g.state='sent' AND g.protocol!='usenet'"):
        try:
            async with httpx.AsyncClient(timeout=10) as x:
                await x.post(g["url"] + "/api/v2/auth/login", data={"username": g["username"], "password": g["password"]})
                for t in (await x.get(g["url"] + "/api/v2/torrents/info", params={"tag": g["dl_id"]})).json():
                    out.append({"title": t["name"], "progress": round(t["progress"] * 100), "status": t["state"], "client": g["name"]})
        except Exception as e: log.warning("Torrent queue fetch failed: %s", e)
    return out
@app.get("/api/v1/history", dependencies=API)
def history(): return rows("SELECT * FROM grab ORDER BY id DESC LIMIT 100")
@app.post("/api/v1/command/checkdownloads", dependencies=API)
async def cmd_check(): await check_downloads(); return {"ok": True}

# ---------- search (Prowlarr) ----------
async def prowlarr(term, mtype):
    c = cfg("indexer")
    if not c["prowlarrUrl"]: raise HTTPException(400, "Set the Prowlarr URL and API key in Settings > Indexers")
    async with httpx.AsyncClient(timeout=60) as x:
        r = await x.get(c["prowlarrUrl"].rstrip("/") + "/api/v1/search", params={"query": term, "categories": TYPES[mtype], "type": "search"}, headers={"X-Api-Key": c["prowlarrKey"]})
    return [{"title": i["title"], "type": mtype, "size": i.get("size", 0), "seeders": i.get("seeders"), "indexer": i.get("indexer"),
             "protocol": i.get("protocol"), "downloadUrl": i.get("downloadUrl") or i.get("magnetUrl"), "publishDate": i.get("publishDate")} for i in r.json()]

@app.get("/api/v1/search", dependencies=API)
async def search(term: str, types: str = "comic,ebook,audiobook"):
    res = await asyncio.gather(*[prowlarr(term, t) for t in types.split(",") if t in TYPES])
    return sorted([r for g in res for r in g], key=lambda r: -(r["seeders"] or 0))
@app.post("/api/v1/release", dependencies=API)
async def grab(request: Request):
    b = await request.json(); await send_to_client(b, b["type"], b.get("item_id")); return {"ok": True}

# ---------- library ----------
@app.get("/api/v1/item", dependencies=API)
def items(type: str = None):
    return rows("SELECT * FROM item WHERE (?1 IS NULL OR type=?1) ORDER BY added DESC", (type,))
FIELDS = ("series_id", "num", "type", "title", "author", "series", "year", "release_date", "description", "cover", "provider", "ext_id", "publisher")
@app.post("/api/v1/item", dependencies=API)
async def add_item(request: Request):
    b = await request.json(); ks = [k for k in FIELDS if k in b]
    c = db.execute(f"INSERT INTO item({','.join(ks)}) VALUES({','.join('?' * len(ks))})", [b[k] for k in ks]); db.commit()
    return {"id": c.lastrowid}
@app.put("/api/v1/item/{iid}", dependencies=API)
async def upd_item(iid: int, request: Request):
    for k, v in (await request.json()).items():
        if k in ("title", "author", "series", "year", "monitored", "status", "release_date"):
            db.execute(f"UPDATE item SET {k}=? WHERE id=?", (v, iid))
    db.commit(); return rows("SELECT * FROM item WHERE id=?", (iid,))[0]
@app.delete("/api/v1/item/{iid}", dependencies=API)
def del_item(iid: int, deleteFile: int = 0):
    r = rows("SELECT path FROM item WHERE id=?", (iid,))
    if deleteFile and r and r[0]["path"] and os.path.exists(r[0]["path"]): os.remove(r[0]["path"])
    db.execute("DELETE FROM item WHERE id=?", (iid,)); db.commit(); return {}
@app.get("/api/v1/item/{iid}/file", dependencies=API)
def item_file(iid: int):
    r = rows("SELECT path FROM item WHERE id=?", (iid,))
    if not r or not r[0]["path"]: raise HTTPException(404)
    return FileResponse(r[0]["path"])
@app.get("/api/v1/wanted/missing", dependencies=API)
def missing(): return rows("SELECT * FROM item WHERE monitored=1 AND status='missing' ORDER BY release_date DESC")
@app.get("/api/v1/wanted/newest", dependencies=API)
def newest(limit: int = 50): return rows("SELECT * FROM item ORDER BY added DESC LIMIT ?", (limit,))
@app.get("/api/v1/calendar", dependencies=API)
def calendar(start: str = "0000", end: str = "9999"):
    return rows("SELECT * FROM item WHERE release_date BETWEEN ? AND ? ORDER BY release_date", (start, end))

EXT = {"comic": {".cbz", ".cbr", ".pdf"}, "ebook": {".epub", ".mobi", ".azw3", ".pdf"}, "audiobook": {".m4b", ".mp3"}}
@app.post("/api/v1/library/scan", dependencies=API)
def scan():
    n = 0
    for t, ext in EXT.items():
        root = pathlib.Path(cfg("library")[t])
        if not root.exists(): continue
        for f in root.rglob("*"):
            if f.suffix.lower() in ext and not rows("SELECT 1 FROM item WHERE path=?", (str(f),)):
                db.execute("INSERT INTO item(type,title,series,path,status) VALUES(?,?,?,?, 'downloaded')", (t, f.stem, f.parent.name if f.parent != root else None, str(f))); n += 1
    db.commit(); log.info("Library scan added %d items", n); return {"added": n}

# ---------- metadata providers ----------
import re, zipfile
UA = {"User-Agent": "Bookarr/0.1"}
clean = lambda h: re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", h or "")).strip()
yr = lambda v: int(str(v)[:4]) if str(v or "")[:4].isdigit() else None
async def jget(url, **kw):
    async with httpx.AsyncClient(timeout=20, headers=UA, follow_redirects=True) as x:
        r = await x.get(url, **kw); r.raise_for_status(); return r.json()
async def look_openlibrary(term, t):
    j = await jget("https://openlibrary.org/search.json", params={"q": term, "limit": 12, "fields": "key,title,author_name,first_publish_year,cover_i"})
    return [{"type": t, "title": d["title"], "author": ", ".join(d.get("author_name", [])[:2]), "year": d.get("first_publish_year"), "description": "", "provider": "openlibrary", "ext_id": d["key"],
             "cover": f"https://covers.openlibrary.org/b/id/{d['cover_i']}-L.jpg" if d.get("cover_i") else None} for d in j.get("docs", [])]
async def look_comic(term):  # ComicVine volumes (needs free API key), falls back to Open Library
    k = cfg("metadata")["comicvineKey"]
    if not k: return await look_openlibrary(term, "comic")
    j = await jget("https://comicvine.gamespot.com/api/search/", params={"api_key": k, "format": "json", "resources": "volume", "query": term, "limit": 12})
    return [{"type": "comic", "title": v["name"], "series": v["name"], "year": yr(v.get("start_year")), "publisher": (v.get("publisher") or {}).get("name"),
             "description": clean(v.get("description")), "cover": (v.get("image") or {}).get("medium_url"), "provider": "comicvine", "ext_id": str(v["id"])} for v in j.get("results", [])]
async def look_ebook(term):  # Google Books, falls back to Open Library
    p = {"q": term, "maxResults": 12, "printType": "books"}
    if cfg("metadata")["googleBooksKey"]: p["key"] = cfg("metadata")["googleBooksKey"]
    try:
        j = await jget("https://www.googleapis.com/books/v1/volumes", params=p); out = []
        for v in j.get("items", []):
            i = v["volumeInfo"]
            out.append({"type": "ebook", "title": i.get("title"), "author": ", ".join(i.get("authors", [])), "year": yr(i.get("publishedDate")), "release_date": i.get("publishedDate"),
                        "publisher": i.get("publisher"), "description": clean(i.get("description")), "provider": "googlebooks", "ext_id": v["id"],
                        "cover": (i.get("imageLinks") or {}).get("thumbnail", "").replace("http://", "https://") or None})
        return out
    except Exception as e:
        log.warning("Google Books failed (%s); using Open Library", e); return await look_openlibrary(term, "ebook")
async def look_audio(term):  # Audible catalog
    j = await jget(f"https://api.audible.{cfg('metadata')['audibleRegion']}/1.0/catalog/products", params={"title": term, "num_results": 12, "products_sort_by": "Relevance",
        "response_groups": "media,product_desc,contributors,series,product_attrs"})
    return [{"type": "audiobook", "title": p["title"], "author": ", ".join(a["name"] for a in p.get("authors", [])), "series": ((p.get("series") or [{}])[0]).get("title"), "year": yr(p.get("release_date")),
             "release_date": p.get("release_date"), "publisher": p.get("publisher_name"), "description": clean(p.get("publisher_summary")), "provider": "audible", "ext_id": p["asin"],
             "cover": (p.get("product_images") or {}).get("500")} for p in j.get("products", [])]
LOOK = {"comic": look_comic, "ebook": look_ebook, "audiobook": look_audio}

@app.get("/api/v1/lookup", dependencies=API)
async def lookup(term: str, types: str = "comic,ebook,audiobook"):
    ts = [t for t in types.split(",") if t in LOOK]; out = []
    for t, r in zip(ts, await asyncio.gather(*[LOOK[t](term) for t in ts], return_exceptions=True)):
        if isinstance(r, Exception): log.warning("Lookup %s failed: %s", t, r)
        else: out += r
    return out
async def refresh_item(iid):
    it = rows("SELECT * FROM item WHERE id=?", (iid,))[0]; r = await LOOK[it["type"]](it["title"])
    if not r: return False
    for k in ("description", "cover", "author", "year", "publisher", "provider", "ext_id", "series", "release_date"):
        if r[0].get(k) and (k in ("description", "cover", "provider", "ext_id") or not it[k]): db.execute(f"UPDATE item SET {k}=? WHERE id=?", (r[0][k], iid))
    db.commit(); (COVERS / f"{iid}.img").unlink(missing_ok=True); return True
@app.post("/api/v1/item/{iid}/refresh", dependencies=API)
async def refresh(iid: int):
    if not await refresh_item(iid): raise HTTPException(404, "No metadata match found")
    return rows("SELECT * FROM item WHERE id=?", (iid,))[0]
@app.post("/api/v1/metadata/refresh", dependencies=API)
async def refresh_missing():
    todo = rows("SELECT id FROM item WHERE description IS NULL OR description=''"); n = 0
    for t in todo:
        try: n += await refresh_item(t["id"])
        except Exception as e: log.warning("Metadata refresh failed: %s", e)
        await asyncio.sleep(0.5)
    log.info("Metadata refreshed for %d of %d items", n, len(todo)); return {"updated": n, "checked": len(todo)}

# ---------- covers (remote cache, else first image inside CBZ/EPUB) ----------
COVERS = DATA / "covers"; COVERS.mkdir(exist_ok=True)
def local_cover(path):
    p = pathlib.Path(path)
    if p.suffix.lower() not in (".cbz", ".epub"): return None
    try:
        with zipfile.ZipFile(p) as z:
            n = sorted(n for n in z.namelist() if n.lower().endswith((".jpg", ".jpeg", ".png", ".webp")) and (p.suffix.lower() == ".cbz" or "cover" in n.lower()))
            return z.read(n[0]) if n else None
    except Exception: return None
async def cover_file(iid):
    f = COVERS / f"{iid}.img"
    if not f.exists():
        it = rows("SELECT cover, path FROM item WHERE id=?", (iid,)); data = None
        if it and it[0]["cover"]:
            try:
                async with httpx.AsyncClient(timeout=20, headers=UA, follow_redirects=True) as x: r = await x.get(it[0]["cover"])
                data = r.content if r.status_code == 200 else None
            except Exception: pass
        if not data and it and it[0]["path"]: data = local_cover(it[0]["path"])
        if not data: raise HTTPException(404)
        f.write_bytes(data)
    return Response(f.read_bytes(), media_type="image/png" if f.read_bytes()[:4] == b"\x89PNG" else "image/jpeg", headers={"Cache-Control": "max-age=86400"})
@app.get("/api/v1/cover/{iid}", dependencies=API)
async def cover_api(iid: int): return await cover_file(iid)

# ---------- OPDS 1.2 catalog for external readers ----------
from datetime import datetime, timezone
from xml.sax.saxutils import escape as xe
MIME = {".cbz": "application/vnd.comicbook+zip", ".cbr": "application/vnd.comicbook-rar", ".pdf": "application/pdf", ".epub": "application/epub+zip",
        ".mobi": "application/x-mobipocket-ebook", ".azw3": "application/vnd.amazon.ebook", ".m4b": "audio/mp4", ".mp3": "audio/mpeg"}
NAVCT = "application/atom+xml;profile=opds-catalog;kind=navigation"; ACQCT = "application/atom+xml;profile=opds-catalog;kind=acquisition"
PAGE = 50
from contextvars import ContextVar
OPDSKEY = ContextVar("opdskey", default="")
async def opds_auth(request: Request, apikey: str = Query(None)):
    s = cfg("security"); ds = {d["key"] for d in rows("SELECT key FROM device")}
    if apikey and (apikey == cfg("general")["apiKey"] or apikey in ds):
        OPDSKEY.set(apikey)
        if apikey in ds: db.execute("UPDATE device SET last_used=CURRENT_TIMESTAMP WHERE key=?", (apikey,)); db.commit()
        return
    if s["authMode"] == "none": return
    want = "Basic " + base64.b64encode(f'{s["username"]}:{s["password"]}'.encode()).decode()
    if request.headers.get("authorization") != want:
        raise HTTPException(401, "Auth required", headers={"WWW-Authenticate": 'Basic realm="Bookarr"'})
OA = [Depends(opds_auth)]
now = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def feed(title, fid, self_href, entries, ct, extra=""):
    xml = (f'<?xml version="1.0" encoding="UTF-8"?><feed xmlns="http://www.w3.org/2005/Atom" xmlns:pse="http://vaemendis.net/opds-pse/ns"><id>{fid}</id><title>{xe(title)}</title><updated>{now()}</updated>'
           f'<link rel="self" href="{xe(self_href)}" type="{ct}"/><link rel="start" href="/opds" type="{NAVCT}"/>'
           f'<link rel="search" href="/opds/opensearch.xml" type="application/opensearchdescription+xml"/>{extra}{entries}</feed>')
    k = OPDSKEY.get()
    if k: xml = re.sub(r'href="(/opds[^"]*)"', lambda m: f'href="{m.group(1)}{"&amp;" if "?" in m.group(1) else "?"}apikey={k}"', xml)
    return Response(xml, media_type=ct)
def acq(its):
    out = ""
    for i in its:
        ext = pathlib.Path(i["path"]).suffix.lower()
        pse = ""
        if ext == ".cbz":
            try: pse = f'<link rel="http://vaemendis.net/opds-pse/stream" type="image/jpeg" href="/opds/pse/{i["id"]}/{{pageNumber}}" pse:count="{len(cbz_pages(i["path"]))}"/>'
            except Exception: pass
        out += (f'<entry><id>bookarr:item:{i["id"]}</id><title>{xe(i["title"])}</title><updated>{(i["added"] or now()).replace(" ", "T")}Z</updated>'
                + (f'<author><name>{xe(i["author"])}</name></author>' if i["author"] else "")
                + (f'<category term="{xe(i["series"])}" label="Series"/>' if i["series"] else "")
                + f'<content type="text">{xe(i["description"] or i["type"])}</content><link rel="http://opds-spec.org/image" href="/opds/cover/{i["id"]}" type="image/jpeg"/><link rel="http://opds-spec.org/image/thumbnail" href="/opds/cover/{i["id"]}" type="image/jpeg"/>'
                + f'<link rel="http://opds-spec.org/acquisition" href="/opds/file/{i["id"]}" type="{MIME.get(ext, "application/octet-stream")}"/>{pse}</entry>')
    return out
def listing(title, fid, href, where, args, page):
    its = rows(f"SELECT * FROM item WHERE path IS NOT NULL AND {where} ORDER BY added DESC LIMIT ? OFFSET ?", (*args, PAGE + 1, page * PAGE))
    sep = "&amp;" if "?" in href else "?"
    nxt = f'<link rel="next" href="{xe(href)}{sep}page={page + 1}" type="{ACQCT}"/>' if len(its) > PAGE else ""
    return feed(title, fid, href, acq(its[:PAGE]), ACQCT, nxt)
@app.get("/opds", dependencies=OA)
def opds_root():
    nav = [("all", "All", "Everything downloaded"), ("new", "Recently added", "Newest 50 items"), ("comic", "Comics", "Comics and graphic novels"),
           ("ebook", "Ebooks", "Ebooks"), ("audiobook", "Audiobooks", "Audiobooks")]
    es = "".join(f'<entry><id>bookarr:nav:{k}</id><title>{t}</title><updated>{now()}</updated><content type="text">{d}</content><link rel="subsection" href="/opds/{k}" type="{ACQCT}"/></entry>' for k, t, d in nav)
    return feed("Bookarr", "bookarr:root", "/opds", es, NAVCT)
@app.get("/opds/opensearch.xml", dependencies=OA)
def opds_os():
    k = OPDSKEY.get(); ksfx = f"&amp;apikey={k}" if k else ""
    return Response('<?xml version="1.0"?><OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/"><ShortName>Bookarr</ShortName><Description>Search Bookarr</Description>'
                    f'<Url type="{ACQCT}" template="/opds/search?q={{searchTerms}}{ksfx}"/></OpenSearchDescription>', media_type="application/opensearchdescription+xml")
@app.get("/opds/search", dependencies=OA)
def opds_search(q: str = "", page: int = 0):
    return listing(f"Search: {q}", "bookarr:search", f"/opds/search?q={quote(q)}", "(title LIKE ?1 OR author LIKE ?1 OR series LIKE ?1)".replace("?1", "?"), (f"%{q}%",) * 3, page)
@app.get("/opds/file/{iid}", dependencies=OA)
def opds_file(iid: int):
    r = rows("SELECT path FROM item WHERE id=?", (iid,))
    if not r or not r[0]["path"] or not os.path.exists(r[0]["path"]): raise HTTPException(404)
    p = pathlib.Path(r[0]["path"]); return FileResponse(p, media_type=MIME.get(p.suffix.lower(), "application/octet-stream"), filename=p.name)
@app.get("/opds/cover/{iid}", dependencies=OA)
async def cover_opds(iid: int): return await cover_file(iid)
@app.get("/opds/{kind}", dependencies=OA)
def opds_list(kind: str, page: int = 0):
    names = {"all": "All", "new": "Recently added", "comic": "Comics", "ebook": "Ebooks", "audiobook": "Audiobooks"}
    if kind not in names: raise HTTPException(404)
    if kind in TYPES: return listing(names[kind], f"bookarr:{kind}", f"/opds/{kind}", "type=?", (kind,), page)
    return listing(names[kind], f"bookarr:{kind}", f"/opds/{kind}", "1=1", (), page)

# ---------- importer: completed downloads -> library ----------
BAD = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
safe = lambda v: BAD.sub("", v or "").strip(" .") or "Unknown"
def mapped(p):
    m = cfg("media")
    return m["localPath"] + p[len(m["remotePath"]):] if m["remotePath"] and p.startswith(m["remotePath"]) else p
def files_for(path, t):
    p = pathlib.Path(path)
    fs = [p] if p.is_file() else [f for f in sorted(p.rglob("*")) if f.is_file()] if p.exists() else []
    return [f for f in fs if f.suffix.lower() in EXT[t]]
def dest_for(t, it, title, src):
    root = pathlib.Path(cfg("library")[t])
    d = root / safe(it.get("series") or it.get("title") or title) if t == "comic" else root / safe(it.get("author") or "Unknown Author")
    if t == "audiobook": d = d / safe(it.get("title") or title)
    return d / safe(src.name)
def transfer(src, dst, protocol):
    dst.parent.mkdir(parents=True, exist_ok=True); n = 1
    while dst.exists(): dst = dst.with_name(f"{dst.stem} ({n}){dst.suffix}"); n += 1
    mode = cfg("media")["importMode"]; mode = ("hardlink" if protocol != "usenet" else "move") if mode == "auto" else mode
    if mode == "move": shutil.move(str(src), dst)
    else:
        try:
            if mode == "hardlink": os.link(src, dst)
            else: shutil.copy2(src, dst)
        except OSError: shutil.copy2(src, dst)
    return dst
def import_grab(g, src_path):
    t = g["type"]; it = (rows("SELECT * FROM item WHERE id=?", (g["item_id"],)) or [{}])[0]
    fs = files_for(mapped(src_path), t)
    if not fs: raise RuntimeError(f"no {t} files found in {src_path} (check the path mapping in Settings > Media)")
    ids = []
    for n, f in enumerate(fs):
        dst = transfer(f, dest_for(t, it, g["title"], f), g["protocol"])
        if n == 0 and it: db.execute("UPDATE item SET path=?, status='downloaded' WHERE id=?", (str(dst), it["id"])); ids.append(it["id"])
        else: ids.append(db.execute("INSERT INTO item(type,title,series,author,path,status) VALUES(?,?,?,?,?,'downloaded')", (t, dst.stem, it.get("series") or it.get("title"), it.get("author"), str(dst))).lastrowid)
    db.commit(); return ids
def fail_grab(g, note):
    db.execute("UPDATE grab SET state='failed', note=? WHERE id=?", (note, g["id"])); db.execute("INSERT OR IGNORE INTO blocklist VALUES(?)", (g["title"],))
    if g["item_id"]: db.execute("UPDATE item SET status='missing' WHERE id=? AND status='wanted'", (g["item_id"],))
    db.commit(); log.warning("Download failed: %s (%s)", g["title"], note); asyncio.ensure_future(notify("onFail", f"Download failed: {g['title']} ({note})"))
async def check_downloads():
    pend = rows("SELECT g.*, c.kind, c.url, c.apikey, c.username, c.password FROM grab g JOIN client c ON c.id=g.client_id WHERE g.state='sent' AND g.dl_id IS NOT NULL")
    for g in pend:
        done = None
        try:
            async with httpx.AsyncClient(timeout=20) as x:
                if g["kind"] == "sabnzbd":
                    j = (await x.get(g["url"] + "/api", params={"mode": "history", "nzo_ids": g["dl_id"], "apikey": g["apikey"], "output": "json"})).json()
                    sl = j["history"]["slots"]
                    if sl and sl[0]["status"] == "Completed": done = sl[0]["storage"]
                    elif sl and sl[0]["status"] == "Failed": fail_grab(g, sl[0].get("fail_message") or "SABnzbd reported failure"); continue
                else:
                    await x.post(g["url"] + "/api/v2/auth/login", data={"username": g["username"], "password": g["password"]})
                    ts = (await x.get(g["url"] + "/api/v2/torrents/info", params={"tag": g["dl_id"]})).json()
                    if ts and ts[0]["progress"] >= 1: done = ts[0].get("content_path") or os.path.join(ts[0]["save_path"], ts[0]["name"])
                    elif ts and ts[0]["state"] in ("error", "missingFiles"): fail_grab(g, ts[0]["state"]); continue
            if done:
                ids = import_grab(g, done); db.execute("UPDATE grab SET state='imported' WHERE id=?", (g["id"],)); db.commit(); log.info("Imported %s (%d file(s))", g["title"], len(ids))
                await notify("onImport", f"Imported: {g['title']}")
                for i in ids:
                    if not rows("SELECT description FROM item WHERE id=?", (i,))[0]["description"]:
                        try: await refresh_item(i)
                        except Exception: pass
                    if cfg("metadata")["writeTags"]:
                        try: write_tags(rows("SELECT * FROM item WHERE id=?", (i,))[0])
                        except Exception as e: log.warning("Tag writing failed: %s", e)
        except Exception as e:
            db.execute("UPDATE grab SET note=? WHERE id=?", (str(e), g["id"])); db.commit(); log.warning("Import check for %s: %s", g["title"], e)
async def import_loop():
    while True:
        await asyncio.sleep(60)
        try: await check_downloads()
        except Exception as e: log.warning("Importer: %s", e)

# ---------- auto search for monitored/missing ----------
# ======================= v0.2: series, quality, notifications, tags, health, duplicates, reading =======================
import time, smtplib, functools
from email.message import EmailMessage
from mutagen.mp4 import MP4
START = time.time()

# ---------- quality preferences + release picking ----------
def fmt_rank(title, t):
    pref = [f.strip().lower() for f in cfg("quality")[t].split(",") if f.strip()]
    for n, f in enumerate(pref):
        if re.search(rf"\b{re.escape(f)}\b", title.lower()): return n
    return len(pref)
def terms(it):
    if it.get("num") and it.get("series"):
        try: n = int(float(it["num"])); return [f'{it["series"]} {n:03d}', f'{it["series"]} {n}', f'{it["series"]} #{n}']
        except ValueError: pass
    return [it["title"]]
def matches(it, r):
    t = r["title"].lower(); words = re.findall(r"\w+", (it.get("series") or it["title"]).lower())[:3]
    if not all(w in t for w in words): return False
    if it.get("num") and it.get("series"):
        try: return re.search(rf"(?<!\d)0*{int(float(it['num']))}(?!\d)", t) is not None
        except ValueError: return True
    return True
async def search_item(it):
    db.execute("UPDATE item SET last_search=datetime('now') WHERE id=?", (it["id"],)); db.commit()
    q = cfg("quality"); cap = (q.get(it["type"] + "MaxMB") or 0) * 1048576; bl = {b["title"] for b in rows("SELECT title FROM blocklist")}
    for term in terms(it):
        rs = [r for r in await prowlarr(term, it["type"]) if r["downloadUrl"] and r["title"] not in bl and matches(it, r) and (not cap or (r["size"] or 0) <= cap)
              and (r["seeders"] is None or r["seeders"] >= cfg("media")["minSeeders"])]
        if q.get("requirePreferred"): rs = [r for r in rs if fmt_rank(r["title"], it["type"]) < len(q[it["type"]].split(","))]
        if rs:
            rs.sort(key=lambda r: (fmt_rank(r["title"], it["type"]), -(r["seeders"] or 0)))
            await send_to_client(rs[0], it["type"], it["id"]); return rs[0]["title"]
    return None
def due():
    return rows("SELECT * FROM item WHERE monitored=1 AND status='missing' AND (release_date IS NULL OR release_date<=date('now')) "
                "AND (last_search IS NULL OR last_search<datetime('now','-1 day')) ORDER BY last_search IS NOT NULL, release_date DESC LIMIT 40")
async def auto_search_loop():
    while True:
        await asyncio.sleep(10)
        try:
            if cfg("indexer")["prowlarrUrl"]:
                for it in due():
                    try: await search_item(it)
                    except HTTPException as e: log.warning("Search for %s: %s", it["title"], e.detail)
                    await asyncio.sleep(1)
        except Exception as e: log.warning("Auto search: %s", e)
        await asyncio.sleep(cfg("media")["autoSearchMinutes"] * 60)
@app.post("/api/v1/item/{iid}/search", dependencies=API)
async def item_search(iid: int):
    r = await search_item(rows("SELECT * FROM item WHERE id=?", (iid,))[0])
    if not r: raise HTTPException(404, "No matching release found. Check your indexers and quality settings.")
    return {"release": r}

# ---------- notifications ----------
async def notify(event, text, force=False):
    n = cfg("notifications")
    if not (force or n.get(event)): return
    try:
        async with httpx.AsyncClient(timeout=10) as x:
            if n["discordWebhook"]: await x.post(n["discordWebhook"], json={"content": text})
            if n["pushoverUser"] and n["pushoverToken"]: await x.post("https://api.pushover.net/1/messages.json", data={"token": n["pushoverToken"], "user": n["pushoverUser"], "title": "Bookarr", "message": text})
            if n["telegramToken"] and n["telegramChat"]: await x.post(f"https://api.telegram.org/bot{n['telegramToken']}/sendMessage", json={"chat_id": n["telegramChat"], "text": text})
            if n["webhookUrl"]: await x.post(n["webhookUrl"], json={"event": event, "message": text})
    except Exception as e: log.warning("Notification failed: %s", e)
@app.post("/api/v1/notification/test", dependencies=API)
async def notify_test(): await notify("onGrab", "Bookarr test notification", force=True); return {"ok": True}

# ---------- tag writing: ComicInfo.xml in CBZ, MP4 tags in M4B ----------
def write_tags(it):
    p = pathlib.Path(it["path"] or "")
    if not p.exists(): raise HTTPException(404, "File not found on disk")
    ext = p.suffix.lower(); d = (it.get("release_date") or "")[:10].split("-")
    if ext == ".cbz":
        f = {"Series": it.get("series") or it["title"], "Number": it.get("num"), "Title": it["title"], "Summary": it.get("description"), "Writer": it.get("author"), "Publisher": it.get("publisher"),
             "Year": d[0] if d[0:1] and d[0].isdigit() else it.get("year"), "Month": d[1] if len(d) > 1 else None, "Day": d[2] if len(d) > 2 else None}
        xml = '<?xml version="1.0" encoding="utf-8"?><ComicInfo>' + "".join(f"<{k}>{xe(str(v))}</{k}>" for k, v in f.items() if v) + "</ComicInfo>"
        tmp = p.with_name(p.name + ".tmp")  # new file + replace: never edits a seeding torrent's inode
        with zipfile.ZipFile(p) as zi, zipfile.ZipFile(tmp, "w") as zo:
            for e in zi.infolist():
                if e.filename.lower() != "comicinfo.xml": zo.writestr(e, zi.read(e.filename))
            zo.writestr("ComicInfo.xml", xml)
        os.replace(tmp, p)
    elif ext == ".m4b":
        if p.stat().st_nlink > 1:
            tmp = p.with_name(p.name + ".tmp"); shutil.copy2(p, tmp); os.replace(tmp, p)
        m = MP4(p); m["\xa9nam"] = [it["title"]]
        if it.get("author"): m["\xa9ART"] = [it["author"]]
        m["\xa9alb"] = [it.get("series") or it["title"]]
        if it.get("description"): m["desc"] = [it["description"][:250]]
        m.save()
    else: raise HTTPException(400, "Tag writing supports CBZ and M4B files")
@app.post("/api/v1/item/{iid}/tags", dependencies=API)
def item_tags(iid: int): write_tags(rows("SELECT * FROM item WHERE id=?", (iid,))[0]); return {"ok": True}

# ---------- series / author monitoring ----------
async def fetch_series_items(sr):
    p, out = sr["provider"], []
    if p == "comicvine":
        k = cfg("metadata")["comicvineKey"]
        if not k: raise HTTPException(400, "Add your ComicVine API key in Settings > Metadata to monitor comic series")
        off = 0
        while off < 1000:
            j = await jget("https://comicvine.gamespot.com/api/issues/", params={"api_key": k, "format": "json", "filter": f"volume:{sr['ext_id']}", "field_list": "id,issue_number,cover_date,store_date,image", "limit": 100, "offset": off})
            for v in j.get("results", []):
                out.append({"ext_id": str(v["id"]), "num": v.get("issue_number"), "title": f"{sr['title']} #{v.get('issue_number')}", "release_date": v.get("store_date") or v.get("cover_date"), "cover": (v.get("image") or {}).get("medium_url")})
            off += 100
            if off >= j.get("number_of_total_results", 0): break
    elif p == "audible":
        j = await jget(f"https://api.audible.{cfg('metadata')['audibleRegion']}/1.0/catalog/products", params={"title": sr["title"], "num_results": 50, "products_sort_by": "Relevance",
            "response_groups": "media,product_desc,contributors,series,product_attrs"})
        for v in j.get("products", []):
            s = (v.get("series") or [{}])[0]
            if (s.get("title") or "").lower() == sr["title"].lower():
                out.append({"ext_id": v["asin"], "num": s.get("sequence"), "title": v["title"], "release_date": v.get("release_date"), "cover": (v.get("product_images") or {}).get("500"),
                            "author": ", ".join(a["name"] for a in v.get("authors", [])), "description": clean(v.get("publisher_summary"))})
    elif p == "author":
        j = await jget("https://www.googleapis.com/books/v1/volumes", params={"q": f'inauthor:"{sr["title"]}"', "maxResults": 40, "orderBy": "newest", "printType": "books"})
        for v in j.get("items", []):
            i = v["volumeInfo"]
            out.append({"ext_id": v["id"], "title": i.get("title"), "release_date": i.get("publishedDate"), "author": sr["title"], "description": clean(i.get("description")),
                        "cover": (i.get("imageLinks") or {}).get("thumbnail", "").replace("http://", "https://") or None})
    return out
async def sync_series(sid):
    sr = rows("SELECT * FROM series WHERE id=?", (sid,))[0]; added = 0
    have = {}
    for e in rows("SELECT * FROM item WHERE series=? AND type=? AND series_id IS NULL AND status='downloaded'", (sr["title"], sr["type"])):
        m = re.findall(r"(?<!\d)0*(\d{1,4})(?!\d)", e["title"])
        if m: have[m[-1]] = e
    for i in await fetch_series_items(sr):
        if rows("SELECT 1 FROM item WHERE series_id=? AND ext_id=?", (sid, i["ext_id"])): continue
        try: n = str(int(float(i.get("num") or ""))) 
        except ValueError: n = None
        if n and n in have:
            db.execute("UPDATE item SET series_id=?, ext_id=?, num=?, release_date=?, cover=COALESCE(cover,?) WHERE id=?", (sid, i["ext_id"], i["num"], i.get("release_date"), i.get("cover"), have[n]["id"])); continue
        db.execute("INSERT INTO item(type,title,series,series_id,num,ext_id,release_date,cover,author,description,provider,monitored,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'missing')",
                   (sr["type"], i["title"], sr["title"], sid, i.get("num"), i["ext_id"], i.get("release_date"), i.get("cover"), i.get("author") or sr["author"], i.get("description"), sr["provider"], sr["monitored"])); added += 1
    db.commit(); log.info("Series %s synced, %d new items", sr["title"], added); return added
SQ = "SELECT s.*, COUNT(i.id) total, COALESCE(SUM(i.status='downloaded'),0) have FROM series s LEFT JOIN item i ON i.series_id=s.id"
@app.get("/api/v1/series", dependencies=API)
def series_list(): return rows(SQ + " GROUP BY s.id ORDER BY s.title")
@app.post("/api/v1/series", dependencies=API)
async def add_series(request: Request):
    b = await request.json()
    if rows("SELECT 1 FROM series WHERE provider=? AND ext_id=?", (b.get("provider"), str(b.get("ext_id")))): raise HTTPException(409, "Already monitored")
    sid = db.execute("INSERT INTO series(type,title,author,provider,ext_id,cover,description,year,publisher) VALUES(?,?,?,?,?,?,?,?,?)",
                     (b["type"], b["title"], b.get("author"), b.get("provider"), str(b.get("ext_id")), b.get("cover"), b.get("description"), b.get("year"), b.get("publisher"))).lastrowid; db.commit()
    try: added = await sync_series(sid)
    except Exception:
        db.execute("DELETE FROM series WHERE id=?", (sid,)); db.commit(); raise
    return {"id": sid, "added": added}
@app.get("/api/v1/series/{sid}", dependencies=API)
def series_get(sid: int):
    return {"series": rows(SQ + " WHERE s.id=? GROUP BY s.id", (sid,))[0], "items": rows("SELECT * FROM item WHERE series_id=? ORDER BY CAST(num AS REAL), release_date", (sid,))}
@app.put("/api/v1/series/{sid}", dependencies=API)
async def series_put(sid: int, request: Request):
    b = await request.json()
    if "monitored" in b:
        db.execute("UPDATE series SET monitored=? WHERE id=?", (b["monitored"], sid)); db.execute("UPDATE item SET monitored=? WHERE series_id=? AND status!='downloaded'", (b["monitored"], sid)); db.commit()
    return {"ok": True}
@app.delete("/api/v1/series/{sid}", dependencies=API)
def series_del(sid: int):
    db.execute("DELETE FROM item WHERE series_id=? AND status!='downloaded'", (sid,)); db.execute("UPDATE item SET series_id=NULL WHERE series_id=?", (sid,)); db.execute("DELETE FROM series WHERE id=?", (sid,)); db.commit(); return {}
@app.post("/api/v1/series/{sid}/refresh", dependencies=API)
async def series_refresh(sid: int): return {"added": await sync_series(sid)}
@app.post("/api/v1/series/{sid}/search", dependencies=API)
async def series_search(sid: int):
    sent = 0
    for it in rows("SELECT * FROM item WHERE series_id=? AND monitored=1 AND status='missing' AND (release_date IS NULL OR release_date<=date('now')) ORDER BY CAST(num AS REAL) LIMIT 50", (sid,)):
        try: sent += 1 if await search_item(it) else 0
        except HTTPException as e: raise HTTPException(e.status_code, e.detail)
        await asyncio.sleep(1)
    return {"sent": sent}

# ---------- health, system, backups ----------
BK = DATA / "backups"; BK.mkdir(exist_ok=True)
def make_backup():
    name = f"bookarr-{time.strftime('%Y%m%d-%H%M%S')}.zip"; tmp = BK / "tmp.db"; dst = sqlite3.connect(tmp); db.backup(dst); dst.close()
    with zipfile.ZipFile(BK / name, "w", zipfile.ZIP_DEFLATED) as z: z.write(tmp, "bookarr.db")
    tmp.unlink()
    for old in sorted(BK.glob("bookarr-*.zip"))[:-7]: old.unlink()
    return name
@app.get("/api/v1/system/backup", dependencies=API)
def backups(): return [{"name": f.name, "size": f.stat().st_size} for f in sorted(BK.glob("bookarr-*.zip"), reverse=True)]
@app.post("/api/v1/system/backup", dependencies=API)
def backup_now(): return {"name": make_backup()}
@app.get("/api/v1/system/backup/{name}", dependencies=API)
def backup_get(name: str):
    f = BK / pathlib.Path(name).name
    if not f.exists(): raise HTTPException(404)
    return FileResponse(f, filename=f.name)
@app.get("/ping")
def ping(): return {"ok": True}
@app.get("/api/v1/system/info", dependencies=API)
def sysinfo():
    lib = cfg("library"); disks = []
    for n, pth in (("Comics", lib["comic"]), ("Ebooks", lib["ebook"]), ("Audiobooks", lib["audiobook"]), ("Downloads", lib["downloads"]), ("Config", str(DATA))):
        if os.path.exists(pth): u = shutil.disk_usage(pth); disks.append({"name": n, "path": pth, "free": u.free, "total": u.total})
    up = int(time.time() - START)
    return {"version": "0.2.0", "uptime": f"{up // 3600}h {up % 3600 // 60}m", "disks": disks, "counts": status()["counts"]}
@app.get("/api/v1/health", dependencies=API)
async def health():
    out = []; add = lambda ok, name, msg: out.append({"ok": ok, "name": name, "message": msg})
    ind = cfg("indexer")
    if not ind["prowlarrUrl"]: add(False, "Indexers", "Set the Prowlarr URL and API key in Settings > Indexers")
    else:
        try:
            async with httpx.AsyncClient(timeout=8) as x: r = await x.get(ind["prowlarrUrl"].rstrip("/") + "/api/v1/system/status", headers={"X-Api-Key": ind["prowlarrKey"]})
            add(r.status_code == 200, "Prowlarr", "Connected" if r.status_code == 200 else f"Prowlarr answered {r.status_code}; check the API key")
        except Exception: add(False, "Prowlarr", f"Cannot reach {ind['prowlarrUrl']}")
    cl = rows("SELECT * FROM client WHERE enabled=1")
    if not cl: add(False, "Download clients", "Add SABnzbd or qBittorrent in Settings > Download clients")
    for c in cl:
        try:
            async with httpx.AsyncClient(timeout=8) as x:
                if c["kind"] == "sabnzbd": r = await x.get(c["url"] + "/api", params={"mode": "version", "apikey": c["apikey"], "output": "json"})
                else:
                    await x.post(c["url"] + "/api/v2/auth/login", data={"username": c["username"], "password": c["password"]}); r = await x.get(c["url"] + "/api/v2/app/version")
            add(r.status_code == 200, c["name"] or c["kind"], "Connected" if r.status_code == 200 else "Login failed; check credentials")
        except Exception: add(False, c["name"] or c["kind"], f"Cannot reach {c['url']}")
    for t in TYPES:
        pth = cfg("library")[t]; ok = os.path.isdir(pth) and os.access(pth, os.W_OK); add(ok, f"{t.capitalize()} folder", pth if ok else f"{pth} is missing or not writable; mount it in Docker")
    if not cfg("metadata")["comicvineKey"]: add(False, "ComicVine", "No API key set: comic series monitoring is disabled")
    for d in sysinfo()["disks"]:
        if d["name"] != "Config" and d["free"] < 20 * 1024 ** 3: add(False, f"Disk space ({d['name']})", "Less than 20 GB free")
    return out

# ---------- duplicates ----------
norm = lambda v: re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", re.sub(r"\(.*?\)|\[.*?\]", "", (v or "").lower()))).strip()
@app.get("/api/v1/duplicates", dependencies=API)
def duplicates():
    g = {}
    for i in rows("SELECT * FROM item WHERE path IS NOT NULL"): g.setdefault((i["type"], norm(i["series"]), str(i["num"] or norm(i["title"]))), []).append(i)
    return [v for v in g.values() if len(v) > 1]

# ---------- reading: OPDS page streaming, Kindle, device keys ----------
@functools.lru_cache(maxsize=256)
def cbz_pages(path):
    with zipfile.ZipFile(path) as z: return sorted(n for n in z.namelist() if n.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")) and not n.startswith("__MACOSX"))
@app.get("/opds/pse/{iid}/{page}", dependencies=OA)
def pse_page(iid: int, page: int):
    r = rows("SELECT path FROM item WHERE id=?", (iid,))
    if not r or not r[0]["path"] or not r[0]["path"].lower().endswith(".cbz"): raise HTTPException(404)
    names = cbz_pages(r[0]["path"])
    if not 0 <= page < len(names): raise HTTPException(404)
    with zipfile.ZipFile(r[0]["path"]) as z: data = z.read(names[page])
    return Response(data, media_type={"png": "image/png", "webp": "image/webp", "gif": "image/gif"}.get(names[page].lower().rsplit(".", 1)[-1], "image/jpeg"), headers={"Cache-Control": "max-age=3600"})
def smtp_send(it):
    e = cfg("email")
    if not (e["smtpHost"] and e["kindleAddress"]): raise HTTPException(400, "Set your SMTP server and Kindle address in Settings > Email")
    p = pathlib.Path(it["path"]); m = EmailMessage(); m["From"] = e["from"] or e["user"]; m["To"] = e["kindleAddress"]; m["Subject"] = it["title"]; m.set_content("Sent by Bookarr")
    m.add_attachment(p.read_bytes(), maintype="application", subtype="octet-stream", filename=p.name)
    with (smtplib.SMTP_SSL if int(e["smtpPort"]) == 465 else smtplib.SMTP)(e["smtpHost"], int(e["smtpPort"]), timeout=60) as s:
        if int(e["smtpPort"]) != 465: s.starttls()
        if e["user"]: s.login(e["user"], e["password"])
        s.send_message(m)
@app.post("/api/v1/item/{iid}/send", dependencies=API)
async def item_send(iid: int):
    it = rows("SELECT * FROM item WHERE id=?", (iid,))[0]
    if not it["path"] or not os.path.exists(it["path"]): raise HTTPException(404, "File not found on disk")
    await asyncio.to_thread(smtp_send, it); return {"ok": True}
@app.get("/api/v1/device", dependencies=API)
def devices(): return rows("SELECT * FROM device ORDER BY id")
@app.post("/api/v1/device", dependencies=API)
async def device_add(request: Request):
    b = await request.json(); db.execute("INSERT INTO device(name,key) VALUES(?,?)", (b.get("name") or "Device", secrets.token_hex(12))); db.commit(); return {"ok": True}
@app.delete("/api/v1/device/{did}", dependencies=API)
def device_del(did: int): db.execute("DELETE FROM device WHERE id=?", (did,)); db.commit(); return {}

# ---------- maintenance ----------
async def maint_loop():
    await asyncio.sleep(30)
    while True:
        try:
            fs = list(BK.glob("bookarr-*.zip"))
            if not fs or time.time() - max(f.stat().st_mtime for f in fs) > 7 * 86400: make_backup()
            for s in rows("SELECT id FROM series WHERE monitored=1"): await sync_series(s["id"]); await asyncio.sleep(2)
        except Exception as e: log.warning("Maintenance: %s", e)
        await asyncio.sleep(12 * 3600)

app.mount("/", StaticFiles(directory=pathlib.Path(__file__).parent / "static", html=True))
