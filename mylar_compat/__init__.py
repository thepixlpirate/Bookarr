"""Mylar3-compatible API for SeerrNG (comics).

SeerrNG sends comic requests to "Mylar3" servers, not to Readarr. This module lets SeerrNG use
Bookarr as its Mylar3 server. It lives under its own address:

    /mylar-compat/api?apikey=<Bookarr API key>&cmd=<command>      (SeerrNG "URL Base": /mylar-compat)

Mylar3 identifies a comic by its ComicVine volume number and an issue by its ComicVine issue
number. Bookarr already stores exactly those numbers for every comic series it follows, so no
extra storage is needed: a SeerrNG comic request simply becomes "follow this ComicVine series"
in Bookarr, using Bookarr's own series-following, searching and downloading.

Commands SeerrNG uses (from server/api/comics/mylar.ts in SeerrNG):
  getVersion, getIndex, getComic, addComic, delComic, downloadIssue

Safety rules:
  * OFF by default. When Settings > SeerrNG > "Enable Mylar3 compatibility" is off, every route
    here answers 404.
  * Mylar3 sends its key as ?apikey=..., so that is what is checked here (Bookarr's existing API
    key). Missing or wrong key -> 401.
  * delComic stops following the series but never deletes downloaded files (Mylar3 does not either).
"""
import asyncio, os, re
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse

PREFIX = "/mylar-compat"
SETTINGS = "seerrng"
SETTING_DEFAULTS = {"readarrEnabled": 0, "mylarEnabled": 0, "allowDeleteFiles": 0}
core = None


def register(app, main_module):
    global core
    core = main_module
    core.DEFAULTS.setdefault(SETTINGS, dict(SETTING_DEFAULTS))
    for k, v in SETTING_DEFAULTS.items(): core.DEFAULTS[SETTINGS].setdefault(k, v)
    app.include_router(_router(), prefix=PREFIX)


def _on():
    try: return int(core.cfg(SETTINGS).get("mylarEnabled") or 0) == 1
    except (TypeError, ValueError): return False

def ok(data=None): return JSONResponse({"success": True, "data": data})

def err(code, message): return JSONResponse({"success": False, "error": {"code": code, "message": message}}, status_code=code)


# ---------------------------------------------------------------- shaping Bookarr data like Mylar3
def _series(cvid):
    r = core.rows("SELECT * FROM series WHERE type='comic' AND provider='comicvine' AND ext_id=?", (str(cvid),))
    return r[0] if r else None

def _issues(sid): return core.rows("SELECT * FROM item WHERE series_id=? ORDER BY CAST(num AS REAL), release_date", (sid,))

def _issue_status(it):
    if it["status"] == "downloaded" and it["path"] and os.path.exists(it["path"]): return "Downloaded"
    if it["status"] == "wanted": return "Snatched"
    return "Wanted" if it["monitored"] else "Skipped"

def comic_obj(s):
    iss = _issues(s["id"]); nums = [i["num"] for i in iss if i["num"]]
    latest = max(nums, key=lambda n: float(n) if re.fullmatch(r"\d+(\.\d+)?", str(n)) else -1) if nums else None
    return {"id": str(s["ext_id"]), "name": s["title"], "imageURL": s["cover"] or "", "status": "Active" if s["monitored"] else "Paused",
            "publisher": s["publisher"] or "", "year": str(s["year"] or ""), "latestIssue": str(latest or ""), "totalIssues": len(iss),
            "haveIssues": sum(1 for i in iss if _issue_status(i) == "Downloaded")}

def issue_obj(it):
    d = (it["release_date"] or "")[:10]
    return {"id": str(it["ext_id"] or it["id"]), "name": it["title"], "number": str(it["num"] or ""), "releaseDate": d, "issueDate": d,
            "status": _issue_status(it)}


# ---------------------------------------------------------------- commands
async def add_comic(cvid):
    if not re.fullmatch(r"\d{1,12}", cvid or ""): return err(400, "id must be a ComicVine volume number")
    s = _series(cvid)
    if s:
        if not s["monitored"]: core.db.execute("UPDATE series SET monitored=1 WHERE id=?", (s["id"],)); core.db.commit()
        return ok({"message": "Already following this comic", "id": cvid})
    key = core.cfg("metadata")["comicvineKey"]
    if not key: return err(400, "Bookarr needs a ComicVine API key (Settings > Metadata) to add comics")
    try:
        v = (await core.jget(f"https://comicvine.gamespot.com/api/volume/4050-{cvid}/", params={"api_key": key, "format": "json",
             "field_list": "id,name,start_year,publisher,image,description,deck"})).get("results") or {}
    except Exception as e: return err(502, f"ComicVine lookup failed: {e}")
    if not v.get("name"): return err(404, "ComicVine has no volume with that id")
    try:
        sid, added, _ = await core.follow({"type": "comic", "title": v["name"], "provider": "comicvine", "ext_id": str(cvid), "year": core.yr(v.get("start_year")),
                                           "cover": (v.get("image") or {}).get("medium_url"), "publisher": (v.get("publisher") or {}).get("name"),
                                           "description": core.clean(v.get("description") or v.get("deck"))})
    except HTTPException as e: return err(e.status_code, str(e.detail))
    asyncio.create_task(_search_series(sid))
    core.log.info("SeerrNG added comic %s (%d issues)", v["name"], added)
    return ok({"message": f"Added {v['name']}", "id": cvid})

async def _search_series(sid):
    try: await core.series_search(sid)  # Bookarr's own "Search missing" for a series
    except Exception as e: core.log.warning("SeerrNG comic search: %s", getattr(e, "detail", e))


def _router():
    r = APIRouter(include_in_schema=False)

    async def handle(request: Request, apikey: str, cmd: str, id: str):
        if not _on(): raise HTTPException(404, "Not Found")
        if not apikey or apikey != core.cfg("general")["apiKey"]: return err(401, "Invalid API key")
        cmd = (cmd or "").strip()
        if cmd == "getVersion":
            return ok({"current_version": "bookarr-0.3.7", "latest_version": "bookarr-0.3.7", "commits_behind": 0, "install_type": "bookarr"})
        if cmd == "getIndex":
            return ok([comic_obj(s) for s in core.rows("SELECT * FROM series WHERE type='comic' AND provider='comicvine' ORDER BY title")])
        if cmd == "getComic":
            s = _series(id)
            if not s: return err(404, "Comic not found")
            return ok({"comic": [comic_obj(s)], "issues": [issue_obj(i) for i in _issues(s["id"])]})
        if cmd == "addComic": return await add_comic(id)
        if cmd == "delComic":
            s = _series(id)
            if s: core.series_del(s["id"]); core.log.info("SeerrNG stopped following comic %s (files kept)", s["title"])
            return ok({"message": "Removed"})
        if cmd == "downloadIssue":
            if not re.fullmatch(r"\d{1,20}", id or ""): return err(400, "Issue ids are numeric")
            it = core.rows("SELECT * FROM item WHERE type='comic' AND ext_id=? AND path IS NOT NULL", (id,))
            if not it or not os.path.exists(it[0]["path"]): return err(404, "That issue has no downloaded file yet")
            p = it[0]["path"]
            return FileResponse(p, media_type=core.MIME.get(os.path.splitext(p)[1].lower(), "application/octet-stream"), filename=os.path.basename(p))
        if cmd in ("pauseComic", "resumeComic"):
            s = _series(id)
            if not s: return err(404, "Comic not found")
            await core.series_put(s["id"], _FakeBody({"monitored": 1 if cmd == "resumeComic" else 0}))
            return ok({"message": "OK"})
        if cmd == "forceSearch":
            for s in core.rows("SELECT id FROM series WHERE type='comic' AND provider='comicvine' AND monitored=1"):
                asyncio.create_task(_search_series(s["id"]))
            return JSONResponse("OK")
        return err(400, f"Bookarr does not support the Mylar3 command '{cmd}'")

    @r.get("/api")
    async def api_get(request: Request, apikey: str = Query(None), cmd: str = Query(None), id: str = Query(None)):
        return await handle(request, apikey, cmd, id)

    @r.post("/api")
    async def api_post(request: Request, apikey: str = Query(None), cmd: str = Query(None), id: str = Query(None)):
        return await handle(request, apikey, cmd, id)

    return r


class _FakeBody:
    """Lets us call Bookarr's existing series update function, which reads a JSON request body."""
    def __init__(self, data): self._d = data
    async def json(self): return self._d
