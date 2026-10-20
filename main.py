"""Bookarr: comics, ebooks, audiobooks. Indexers via Prowlarr, clients: SABnzbd + qBittorrent."""
import os, shutil, re, json, secrets, sqlite3, asyncio, pathlib, logging, base64, collections, functools, time, tempfile
from mutagen import File as MutagenFile
from contextlib import asynccontextmanager
from urllib.parse import quote, urlsplit
import httpx
from fastapi import FastAPI, Depends, HTTPException, Header, Query, Request
from fastapi.responses import FileResponse, Response, PlainTextResponse
from fastapi.staticfiles import StaticFiles

DATA = pathlib.Path(os.getenv("DATA_DIR", "/config")); DATA.mkdir(parents=True, exist_ok=True)
import threading
class _Result:
    """A finished query: rows are read straight away, so another request can never read them by mistake."""
    def __init__(self, cur):
        self._rows = cur.fetchall() if cur.description else []; self.lastrowid = cur.lastrowid; self.rowcount = cur.rowcount
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return list(self._rows)
    def __iter__(self): return iter(self._rows)
class _SafeDB:
    """One shared database connection, used by one request at a time (Bookarr answers many requests at once, e.g. cover images)."""
    def __init__(self, path):
        self._c = sqlite3.connect(path, check_same_thread=False, timeout=30); self._c.row_factory = sqlite3.Row; self._lock = threading.RLock()
        self._c.execute("PRAGMA journal_mode=WAL")
    def execute(self, q, a=()):
        with self._lock: return _Result(self._c.execute(q, a))
    def executemany(self, q, a):
        with self._lock: return _Result(self._c.executemany(q, a))
    def executescript(self, q):
        with self._lock: return self._c.executescript(q)
    def commit(self):
        with self._lock: self._c.commit()
    def backup(self, dst):
        with self._lock: self._c.commit(); self._c.backup(dst)
db = _SafeDB(DATA / "bookarr.db")
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
for col in ("series_id INT", "num TEXT", "last_search TEXT", "description TEXT", "cover TEXT", "provider TEXT", "ext_id TEXT", "publisher TEXT", "meta TEXT"):
    try: db.execute(f"ALTER TABLE item ADD COLUMN {col}")
    except sqlite3.OperationalError: pass
# mon_mode for followed authors/series: 'future' = only books released after you follow, 'all' = every missing book, 'none' = just list them
try: db.execute("ALTER TABLE series ADD COLUMN mon_mode TEXT")
except sqlite3.OperationalError: pass
db.execute("UPDATE series SET mon_mode=CASE WHEN provider='author' THEN 'future' ELSE 'all' END WHERE mon_mode IS NULL"); db.commit()

LOGS = collections.deque(maxlen=500)
class Mem(logging.Handler):
    def emit(self, r): LOGS.append({"time": self.format(r).split("|")[0], "level": r.levelname, "message": r.getMessage()})
log = logging.getLogger("bookarr"); log.setLevel(logging.INFO)
h = Mem(); h.setFormatter(logging.Formatter("%(asctime)s|")); log.addHandler(h); log.addHandler(logging.StreamHandler())

TYPES = {"comic": 7030, "ebook": 7020, "audiobook": 3030}  # Newznab categories
DEFAULTS = {
    "general": {"logLevel": "info", "apiKey": ""},
    "indexer": {"prowlarrUrl": "", "prowlarrKey": ""},
    "library": {"comic": "/books/Comics", "ebook": "/books/Ebooks", "audiobook": "/books/Audiobooks", "downloads": "/downloads"},
    "media": {"autoSearchMinutes": 60, "minSeeders": 1, "importMode": "auto", "remotePath": "", "localPath": "", "catComic": "comics", "catEbook": "ebooks", "catAudiobook": "audiobooks",
              "downloadSubfolder": "Books"},
    "metadata": {"comicvineKey": "", "googleBooksKey": "", "audibleRegion": "com", "writeTags": 1},
    "quality": {"comic": "cbz,cbr,pdf", "ebook": "epub,azw3,mobi,pdf", "audiobook": "m4b,mp3", "comicMaxMB": 500, "ebookMaxMB": 200, "audiobookMaxMB": 3000, "requirePreferred": 0},
    "notifications": {"onGrab": 1, "onImport": 1, "onFail": 1, "discordWebhook": "", "pushoverUser": "", "pushoverToken": "", "telegramToken": "", "telegramChat": "", "webhookUrl": ""},
    "email": {"smtpHost": "", "smtpPort": 587, "user": "", "password": "", "from": "", "kindleAddress": "", "kindleFormat": "azw3"},
    "converter": {"transmuteUrl": "", "transmuteKey": ""},
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
    for k in ("comic", "ebook", "audiobook", "downloads"):
        try: os.makedirs(cfg("library")[k], exist_ok=True)
        except OSError as e: log.warning("Could not create %s folder: %s", k, e)
    ts = [asyncio.create_task(auto_search_loop()), asyncio.create_task(import_loop()), asyncio.create_task(maint_loop())]; yield
    for t in ts: t.cancel()
app = FastAPI(title="Bookarr", lifespan=life)
from fastapi.responses import JSONResponse
import traceback
@app.exception_handler(Exception)
async def unexpected(request: Request, e: Exception):
    log.error("Error on %s: %s\n%s", request.url.path, e, "".join(traceback.format_exception(e)[-4:]))
    msg = str(e) or e.__class__.__name__
    if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429: msg = "A metadata site is limiting requests right now (too many searches). Wait a few minutes, or add a free Google Books API key in Settings > Metadata."
    return JSONResponse({"detail": f"Something went wrong: {msg}. Details are in Settings > Logs."}, status_code=500)

@app.middleware("http")
async def basic_auth(request: Request, call_next):
    s = cfg("security")
    if s["authMode"] == "basic" and not request.url.path.startswith(("/api/", "/opds", "/ping", "/kindle", "/readarr-compat", "/mylar-compat")):
        want = "Basic " + base64.b64encode(f'{s["username"]}:{s["password"]}'.encode()).decode()
        if request.headers.get("authorization") != want:
            return Response("Auth required", 401, {"WWW-Authenticate": 'Basic realm="Bookarr"'})
    return await call_next(request)

@app.get("/initialize.js")
def init(): return Response(f'window.BOOKARR={{apiKey:"{cfg("general")["apiKey"]}"}}', media_type="text/javascript")

# ---------- system / config ----------
@app.get("/api/v1/system/status", dependencies=API)
def status():
    return {"appName": "Bookarr", "version": "0.3.0", "counts": {t: rows("SELECT COUNT(*) c FROM item WHERE type=?", (t,))[0]["c"] for t in TYPES}}
@app.get("/api/v1/log", dependencies=API)
def get_log(): return list(LOGS)[::-1]
@app.get("/api/v1/config/{sec}", dependencies=API)
def get_cfg(sec: str):
    if sec not in DEFAULTS: raise HTTPException(404)
    return cfg(sec)
@app.put("/api/v1/config/{sec}", dependencies=API)
async def put_cfg(sec: str, request: Request):
    if sec not in DEFAULTS: raise HTTPException(404)
    b = await request.json()
    if sec == "indexer" and b.get("prowlarrUrl"): b["prowlarrUrl"] = norm_url(b["prowlarrUrl"])
    if sec == "converter" and b.get("transmuteUrl"): b["transmuteUrl"] = norm_url(b["transmuteUrl"])
    setcfg(sec, b); return cfg(sec)
@app.post("/api/v1/indexer/test", dependencies=API)
async def test_indexer():
    c = cfg("indexer"); url = norm_url(c["prowlarrUrl"])
    if not url: return {"ok": False, "message": "Enter your Prowlarr URL first."}
    try:
        async with httpx.AsyncClient(timeout=10) as x:
            r = await x.get(url + "/api/v1/system/status", headers={"X-Api-Key": c["prowlarrKey"]})
            if r.status_code in (401, 403): return {"ok": False, "message": "Reached Prowlarr, but it rejected the API key."}
            if r.status_code != 200: return {"ok": False, "message": f"Prowlarr returned {r.status_code}. Check the URL."}
            n = len((await x.get(url + "/api/v1/indexer", headers={"X-Api-Key": c["prowlarrKey"]})).json())
            return {"ok": True, "message": f"Connected to Prowlarr {r.json().get('version', '')} with {n} indexer(s)"}
    except httpx.ConnectError: return {"ok": False, "message": f"Cannot connect to {url}. Check the address and port."}
    except Exception as e: return {"ok": False, "message": f"Failed: {e}"}

# ---------- download clients ----------
@app.get("/api/v1/downloadclient", dependencies=API)
def clients(): return rows("SELECT * FROM client")
async def qb_login(x, c):
    k = c.keys(); u = norm_url(c["url"])
    return await x.post(u + "/api/v2/auth/login", data={"username": ((c["username"] if "username" in k else "") or "").strip(), "password": ((c["password"] if "password" in k else "") or "").strip()},
                        headers={"Referer": u, "Origin": u}, follow_redirects=False)
# qBittorrent up to 5.1 answers a good login with 200 "Ok."; 5.2 and newer answer 204 with an empty body. Both are success.
qb_login_ok = lambda r: r.status_code == 204 or (r.status_code == 200 and r.text.strip() == "Ok.")
def norm_url(u):
    u = (u or "").strip()
    if not u: return ""
    if not re.match(r"https?://", u, re.I): u = "http://" + u
    p = urlsplit(u); return f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
for _c in rows("SELECT id,url FROM client"):
    if norm_url(_c["url"]) != _c["url"]: db.execute("UPDATE client SET url=? WHERE id=?", (norm_url(_c["url"]), _c["id"]))
db.commit()
async def test_client(c):
    url = norm_url(c["url"])
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as x:
            if c["kind"] == "sabnzbd":
                v = (await x.get(url + "/api", params={"mode": "version", "output": "json"})).json().get("version")
                if not v: return {"ok": False, "message": "That address answered, but it does not look like SABnzbd."}
                q = (await x.get(url + "/api", params={"mode": "queue", "limit": 1, "output": "json", "apikey": c.get("apikey", "")})).json()
                if "error" in q: return {"ok": False, "message": f"Reached SABnzbd {v}, but it said: {q['error']}"}
                return {"ok": True, "message": f"Connected to SABnzbd {v}"}
            r = await qb_login(x, c)
            if r.status_code in (301, 302, 303, 307, 308):
                to = r.headers.get("location", "")
                if to.startswith(("http://", "https://")) and "/api/v2" in to: to = to.split("/api/v2")[0]
                return {"ok": False, "message": f"{url} sends visitors on to {to or 'another address'}. Enter that address instead" + (" (with https://)." if to.startswith("https://") else ".") +
                        " For a server on your own network, the local address (http://192.168.x.x:8080) is the most reliable."}
            if r.status_code == 403: return {"ok": False, "message": "qBittorrent refused the login (too many failed attempts, or its Web UI blocks this address). Wait a few minutes and check its Web UI settings."}
            if r.text.strip() == "Fails." or r.status_code == 401: return {"ok": False, "message": f"Reached qBittorrent, but it rejected the login. Bookarr sent username \"{(c.get('username') or '').strip()}\" and a password of {len((c.get('password') or '').strip())} characters. If that is not what you expect, tap Edit and re-enter them."}
            # Whatever the login said, the real test is whether the API now answers with a version number.
            v = await x.get(url + "/api/v2/app/version", headers={"Referer": url})
            if v.status_code == 200 and re.match(r"v?\d+\.\d+", v.text.strip()):
                return {"ok": True, "message": "Connected to qBittorrent " + v.text.strip()}
            if qb_login_ok(r) and v.status_code == 403:
                return {"ok": False, "message": "qBittorrent accepted the login but then refused requests. In qBittorrent's Web UI settings, turn off \"Host header validation\" or add this address to its allowed list."}
            body = re.sub(r"\s+", " ", r.text)[:80]
            hint = " It answered with a web page, which usually means a login screen (Cloudflare Access, Authelia) or a different app is in front of it. Use the local address instead, like http://192.168.0.185:8080." if "<html" in r.text.lower() else ""
            return {"ok": False, "message": f"That address did not answer like qBittorrent's API (HTTP {r.status_code}{', said: ' + body if body and not hint else ''}).{hint} Use just http://ip:port, with nothing after the port."}
    except ValueError: return {"ok": False, "message": "That address answered, but not like " + ("SABnzbd" if c["kind"] == "sabnzbd" else "qBittorrent") + ". Check the port."}
    except httpx.ConnectError: return {"ok": False, "message": f"Cannot connect to {url}. Check the address and port."}
    except httpx.TimeoutException: return {"ok": False, "message": f"{url} did not answer in time."}
    except Exception as e: return {"ok": False, "message": f"Failed: {e}"}
@app.post("/api/v1/downloadclient", dependencies=API)
async def add_client(request: Request):
    b = await request.json(); b["url"] = norm_url(b["url"])
    c = db.execute("INSERT INTO client(name,kind,url,apikey,username,password,category) VALUES(?,?,?,?,?,?,?)",
        (b.get("name"), b["kind"], b["url"], (b.get("apikey") or "").strip(), (b.get("username") or "").strip(), (b.get("password") or "").strip(), b.get("category", ""))); db.commit()
    res = await test_client(b); log.info("Added %s client %s: %s", b["kind"], b.get("name"), res["message"])
    return {"id": c.lastrowid, **res}
@app.post("/api/v1/downloadclient/test", dependencies=API)
async def test_new_client(request: Request): return await test_client(await request.json())
@app.post("/api/v1/downloadclient/{cid}/test", dependencies=API)
async def test_saved_client(cid: int):
    r = rows("SELECT * FROM client WHERE id=?", (cid,))
    if not r: raise HTTPException(404)
    return await test_client(r[0])
@app.put("/api/v1/downloadclient/{cid}", dependencies=API)
async def edit_client(cid: int, request: Request):
    b = await request.json(); b["url"] = norm_url(b["url"])
    db.execute("UPDATE client SET name=?,kind=?,url=?,apikey=?,username=?,password=?,category=? WHERE id=?",
        (b.get("name"), b["kind"], b["url"], (b.get("apikey") or "").strip(), (b.get("username") or "").strip(), (b.get("password") or "").strip(), b.get("category", ""), cid)); db.commit()
    return {"id": cid, **await test_client(b)}
@app.delete("/api/v1/downloadclient/{cid}", dependencies=API)
def del_client(cid: int): db.execute("DELETE FROM client WHERE id=?", (cid,)); db.commit(); return {}

FOLDER = {"comic": "Comics", "ebook": "Ebooks", "audiobook": "Audiobooks"}
CATKEY = {"comic": "catComic", "ebook": "catEbook", "audiobook": "catAudiobook"}
def cat_plan():
    m = cfg("media"); sub = (m.get("downloadSubfolder") or "").strip("/ ")
    return [(m[CATKEY[t]], f"{sub}/{FOLDER[t]}" if sub else FOLDER[t]) for t in FOLDER]
@app.post("/api/v1/downloadclient/{cid}/setup", dependencies=API)
async def setup_client_folders(cid: int):
    """Create the comics/ebooks/audiobooks categories in the client so each downloads into downloads/Books/<Type>."""
    r = rows("SELECT * FROM client WHERE id=?", (cid,))
    if not r: raise HTTPException(404)
    c = r[0]; url = norm_url(c["url"]); done = []
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as x:
        if c["kind"] == "sabnzbd":
            for cat, rel in cat_plan():  # SABnzbd: a relative folder is placed inside its "Completed Download Folder"
                j = (await x.get(url + "/api", params={"mode": "set_config", "section": "categories", "name": cat, "dir": rel, "apikey": c["apikey"], "output": "json"})).json()
                if isinstance(j, dict) and j.get("error"): raise HTTPException(502, f"SABnzbd said: {j['error']}")
                done.append(f"{cat} → (completed folder)/{rel}")
        else:
            lr = await qb_login(x, c)
            if lr.status_code == 403 or lr.text.strip() == "Fails.": raise HTTPException(502, "qBittorrent rejected the login. Fix the client first (tap Test).")
            base = ((await x.get(url + "/api/v2/app/preferences", headers={"Referer": url})).json().get("save_path") or "/downloads").rstrip("/")
            for cat, rel in cat_plan():
                path = f"{base}/{rel}"
                a = await x.post(url + "/api/v2/torrents/createCategory", data={"category": cat, "savePath": path}, headers={"Referer": url})
                if a.status_code == 409:  # already exists: point it at the right folder
                    a = await x.post(url + "/api/v2/torrents/editCategory", data={"category": cat, "savePath": path}, headers={"Referer": url})
                if a.status_code not in (200, 204): raise HTTPException(502, f"qBittorrent would not save the {cat} category (HTTP {a.status_code}).")
                done.append(f"{cat} → {path}")
    if c["category"]: db.execute("UPDATE client SET category='' WHERE id=?", (cid,)); db.commit()
    log.info("Set up download folders in %s: %s", c["name"] or c["kind"], "; ".join(done))
    return {"ok": True, "folders": done}

async def send_to_client(rel: dict, mtype: str, item_id: int = None):
    kind = "sabnzbd" if rel["protocol"] == "usenet" else "qbittorrent"
    c = db.execute("SELECT * FROM client WHERE kind=? AND enabled=1 LIMIT 1", (kind,)).fetchone()
    if not c: raise HTTPException(400, f"No enabled {kind} client configured")
    cat = c["category"] or cfg("media")[{"comic": "catComic", "ebook": "catEbook", "audiobook": "catAudiobook"}[mtype]]
    gid = db.execute("INSERT INTO grab(item_id,type,title,protocol,client_id) VALUES(?,?,?,?,?)", (item_id, mtype, rel["title"], rel["protocol"], c["id"])).lastrowid
    try:
        async with httpx.AsyncClient(timeout=30) as x:
            if kind == "sabnzbd":
                r = await x.get(c["url"] + "/api", params={"mode": "addurl", "name": rel["downloadUrl"], "nzbname": rel["title"], "cat": cat, "apikey": c["apikey"], "output": "json"})
                dl = ((r.json().get("nzo_ids") or [None])[0]) if r.status_code == 200 else None
            else:
                await qb_login(x, c)
                dl = f"bookarr-{gid}"
                # autoTMM makes qBittorrent save into the category's own folder (Books/Comics etc.)
                r = await x.post(c["url"] + "/api/v2/torrents/add", data={"urls": rel["downloadUrl"], "category": cat, "tags": dl, "autoTMM": "true"}, headers={"Referer": c["url"]})
                if r.status_code in (200, 202, 204) and r.text.strip() == "Fails.": r.status_code = 400
            if r.status_code not in (200, 202, 204) or not dl: raise HTTPException(502, f"{kind} did not accept the release")
    except Exception:
        db.execute("DELETE FROM grab WHERE id=?", (gid,)); db.commit(); raise
    db.execute("UPDATE grab SET dl_id=? WHERE id=?", (dl, gid))
    if item_id: db.execute("UPDATE item SET status='wanted' WHERE id=? AND status='missing'", (item_id,))
    db.commit(); log.info("Grabbed %s via %s", rel["title"], kind); await notify("onGrab", f"Grabbed: {rel['title']}")
    return c["name"] or kind

KIND = {"sabnzbd": "SABnzbd", "qbittorrent": "qBittorrent"}
SAB_ST = {"Completed": "Finished, moving to your library", "Failed": "Failed", "Extracting": "Unpacking", "Verifying": "Checking files", "Repairing": "Repairing",
          "Moving": "Finishing up", "Running": "Finishing up", "Queued": "Waiting in line", "QuickCheck": "Checking files", "Fetching": "Getting extra blocks", "Paused": "Paused"}
QB_ST = {"downloading": "Downloading", "forcedDL": "Downloading", "metaDL": "Getting torrent info", "stalledDL": "Stalled (no one sharing right now)", "queuedDL": "Waiting in line",
         "pausedDL": "Paused", "stoppedDL": "Paused", "checkingDL": "Checking files", "checkingResumeData": "Checking files", "allocating": "Preparing", "moving": "Moving",
         "error": "Error", "missingFiles": "Files missing"}
def secs(n):
    if n is None or n < 0 or n >= 8640000: return None
    return f"{n // 3600}h {n % 3600 // 60}m" if n >= 3600 else f"{n // 60}m {n % 60}s" if n >= 60 else f"{n}s"
async def live_progress():
    """Where every Bookarr download stands right now: percent done, time left, and which app is downloading it."""
    gs = rows("SELECT g.*, c.kind, c.url, c.apikey, c.username, c.password, c.name cname FROM grab g JOIN client c ON c.id=g.client_id WHERE g.state='sent' AND g.dl_id IS NOT NULL ORDER BY g.id")
    out, groups = {}, collections.defaultdict(list)
    for g in gs:
        groups[g["client_id"]].append(g)
        out[g["id"]] = {"grab_id": g["id"], "item_id": g["item_id"], "title": g["title"], "type": g["type"], "client": g["cname"] or KIND[g["kind"]], "kind": KIND[g["kind"]],
                        "progress": None, "status": "Sent", "eta": None, "speed": None, "problem": g["note"], "created": g["created"]}
    for glist in groups.values():
        c = glist[0]; url = norm_url(c["url"])
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as x:
                if c["kind"] == "sabnzbd":
                    q = (await x.get(url + "/api", params={"mode": "queue", "apikey": c["apikey"], "output": "json", "limit": 500})).json()["queue"]
                    qs = {s["nzo_id"]: s for s in q.get("slots", [])}; hist = None
                    for g in glist:
                        o = out[g["id"]]; s = qs.get(g["dl_id"])
                        if s:
                            o.update(progress=float(s.get("percentage") or 0), status="Downloading" if s.get("status") == "Downloading" else SAB_ST.get(s.get("status"), s.get("status")),
                                     eta=s.get("timeleft") if s.get("status") == "Downloading" else None, speed=(str(q.get("speed") or "").strip() + "B/s") if s.get("status") == "Downloading" and q.get("speed") else None)
                            continue
                        if hist is None: hist = {h["nzo_id"]: h for h in (await x.get(url + "/api", params={"mode": "history", "apikey": c["apikey"], "output": "json", "limit": 200})).json()["history"]["slots"]}
                        h = hist.get(g["dl_id"])
                        if h: o.update(progress=100.0, status=SAB_ST.get(h["status"], h["status"]))
                else:
                    await qb_login(x, c)
                    for g in glist:
                        ts = (await x.get(url + "/api/v2/torrents/info", params={"tag": g["dl_id"]}, headers={"Referer": url})).json()
                        if not ts: continue
                        t = ts[0]; done = t["progress"] >= 1
                        out[g["id"]].update(progress=round(t["progress"] * 100, 1), status="Finished, moving to your library" if done else QB_ST.get(t["state"], t["state"]),
                                            eta=None if done else secs(t.get("eta")), speed=f"{t['dlspeed'] / 1048576:.1f} MB/s" if t.get("dlspeed") and not done else None)
        except Exception as e:
            for g in glist: out[g["id"]]["status"] = f"Could not reach {c['cname'] or KIND[c['kind']]}"
            log.info("Progress check for %s failed: %s", c["cname"], e)
    return list(out.values())
@app.get("/api/v1/queue", dependencies=API)
async def queue(item: int = None):
    return [q for q in await live_progress() if item is None or q["item_id"] == item]
HISTQ = "SELECT g.*, c.name client_name, c.kind client_kind FROM grab g LEFT JOIN client c ON c.id=g.client_id"
@app.get("/api/v1/history", dependencies=API)
def history(): return rows(HISTQ + " ORDER BY g.id DESC LIMIT 200")
@app.delete("/api/v1/history", dependencies=API)
def history_clear(scope: str = "finished", blocklist: int = 0):
    """finished = imported + failed (default). all = also forget downloads still running (Bookarr will not import those)."""
    n = db.execute("DELETE FROM grab" + ("" if scope == "all" else " WHERE state!='sent'")).rowcount
    if blocklist: db.execute("DELETE FROM blocklist")
    db.commit(); log.info("Cleared %d history entries", n); return {"cleared": n}
@app.delete("/api/v1/item/{iid}/history", dependencies=API)
def item_history_clear(iid: int, scope: str = "finished"):
    """Clear the download history shown on one title's page. finished = keep downloads still running."""
    n = db.execute("DELETE FROM grab WHERE item_id=?" + ("" if scope == "all" else " AND state!='sent'"), (iid,)).rowcount
    db.commit(); return {"cleared": n}
@app.delete("/api/v1/history/{gid}", dependencies=API)
def history_del(gid: int): db.execute("DELETE FROM grab WHERE id=?", (gid,)); db.commit(); return {}
@app.post("/api/v1/command/checkdownloads", dependencies=API)
async def cmd_check(): await check_downloads(); return {"ok": True}

# ---------- search (Prowlarr) ----------
async def prowlarr(term, mtype):
    c = cfg("indexer")
    if not c["prowlarrUrl"]: raise HTTPException(400, "Set the Prowlarr URL and API key in Settings > Indexers")
    url = norm_url(c["prowlarrUrl"])
    try:
        async with httpx.AsyncClient(timeout=60) as x:
            r = await x.get(url + "/api/v1/search", params={"query": term, "categories": TYPES[mtype], "type": "search"}, headers={"X-Api-Key": c["prowlarrKey"]})
    except httpx.ConnectError: raise HTTPException(502, f"Cannot connect to Prowlarr at {url}. Check the address and port in Settings > Indexers.")
    except httpx.TimeoutException: raise HTTPException(502, "Prowlarr took too long to answer the search.")
    except httpx.HTTPError as e: raise HTTPException(502, f"Prowlarr request failed: {e}")
    if r.status_code in (401, 403): raise HTTPException(502, "Prowlarr rejected the API key. Check it in Settings > Indexers.")
    if r.status_code != 200: raise HTTPException(502, f"Prowlarr returned {r.status_code}. Check the URL in Settings > Indexers.")
    return [{"title": i["title"], "type": mtype, "size": i.get("size", 0), "seeders": i.get("seeders"), "indexer": i.get("indexer"),
             "protocol": i.get("protocol"), "downloadUrl": i.get("downloadUrl") or i.get("magnetUrl"), "publishDate": i.get("publishDate"),
             "infoUrl": i.get("infoUrl") or (i.get("guid") if str(i.get("guid", "")).startswith("http") else None), "files": i.get("files"), "grabs": i.get("grabs")} for i in r.json()]

def age_days(v):
    try: return max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(v.replace("Z", "+00:00"))).days)
    except Exception: return None
@app.get("/api/v1/search", dependencies=API)
async def search(term: str, types: str = "comic,ebook,audiobook", strict: int = 1, item_id: int = None):
    res = await asyncio.gather(*[prowlarr(term, t) for t in types.split(",") if t in TYPES]); out = []; hidden = 0
    it = (rows("SELECT * FROM item WHERE id=?", (item_id,)) or [None])[0] if item_id else None
    for r in (x for g in res for x in g):
        r["score"] = relevance(term, r["title"]); r["age"] = age_days(r.get("publishDate"))
        r["fmt"] = detect_fmt(r["title"], r["type"]); r["pack"] = is_pack(term, r["title"])
        r["skip"] = skip_reason(r, it, term)
        if strict and r["score"] == 0: hidden += 1
        else: out.append(r)
    ok = [r for r in out if not r["skip"]]
    if ok: min(ok, key=lambda r: pick_key(r, term)).update(pick=True)  # the one "Search releases" / auto search would grab
    order = " → ".join(sorted({f.strip().upper() for t in types.split(",") if t in TYPES for f in cfg("quality")[t].split(",") if f.strip()}, key=lambda f: min(
        [cfg("quality")[t].upper().replace(" ", "").split(",").index(f) for t in types.split(",") if t in TYPES and f in cfg("quality")[t].upper().replace(" ", "").split(",")] or [9])))
    return {"results": sorted(out, key=lambda r: (not r.get("pick"), -r["score"], -(r["seeders"] or 0))), "hidden": hidden, "order": order}

# ---------- peek inside a release before downloading (reads the NZB or .torrent file list) ----------
def bdecode(b, i=0):
    c = b[i:i + 1]
    if c == b"i": j = b.index(b"e", i); return int(b[i + 1:j]), j + 1
    if c == b"l":
        i += 1; out = []
        while b[i:i + 1] != b"e": v, i = bdecode(b, i); out.append(v)
        return out, i + 1
    if c == b"d":
        i += 1; out = {}
        while b[i:i + 1] != b"e": k, i = bdecode(b, i); v, i = bdecode(b, i); out[k] = v
        return out, i + 1
    j = b.index(b":", i); n = int(b[i:j]); return b[j + 1:j + 1 + n], j + 1 + n
def nzb_files(data):
    import xml.etree.ElementTree as ET
    out = []
    for f in ET.fromstring(data).iter():
        if not f.tag.endswith("}file") and f.tag != "file": continue
        subj = f.get("subject", ""); m = re.search(r'"([^"]+)"', subj)
        name = m.group(1) if m else subj
        size = sum(int(sg.get("bytes", 0)) for sg in f.iter() if sg.tag.endswith("segment"))
        out.append({"name": name, "size": size})
    return out
def torrent_files(data):
    info = bdecode(data)[0][b"info"]; base = info.get(b"name", b"").decode("utf-8", "replace")
    if b"files" in info: return [{"name": "/".join(x.decode("utf-8", "replace") for x in f[b"path"]), "size": f[b"length"]} for f in info[b"files"]]
    return [{"name": base, "size": info.get(b"length", 0)}]
JUNK = re.compile(r"\.(par2|nfo|sfv|nzb|txt|url|jpg|jpeg|png|db)$|^sample", re.I)
@app.post("/api/v1/release/peek", dependencies=API)
async def release_peek(request: Request):
    b = await request.json(); url = b.get("downloadUrl") or ""
    magnet = "This torrent is only a magnet link, so its file list is not known until qBittorrent starts it."
    if url.startswith("magnet:"): return {"files": [], "message": magnet}
    try:
        async with httpx.AsyncClient(timeout=30, headers=UA) as x:
            r = await x.get(url); hops = 0
            while r.status_code in (301, 302, 303, 307, 308) and hops < 5:
                loc = r.headers.get("location", "")
                if loc.startswith("magnet:"): return {"files": [], "message": magnet}
                r = await x.get(str(r.url.join(loc))); hops += 1
    except httpx.HTTPError as e: raise HTTPException(502, f"Could not fetch the release from the indexer: {e}")
    if r.status_code != 200: raise HTTPException(502, f"The indexer answered HTTP {r.status_code}. You may have reached its daily download limit.")
    data = r.content
    try: fs = nzb_files(data) if b"<nzb" in data[:4000] else torrent_files(data) if data[:1] == b"d" else None
    except Exception: fs = None
    if fs is None: raise HTTPException(502, "The indexer did not send a file list Bookarr can read.")
    fs.sort(key=lambda f: -f["size"]); main = [f for f in fs if not JUNK.search(f["name"])]
    exts = collections.Counter(pathlib.Path(f["name"]).suffix.lower().lstrip(".") or "?" for f in main)
    packed = any(re.search(r"\.(rar|r\d\d|7z|zip|part\d+\.rar)$", f["name"], re.I) for f in main)
    obf = main and all(re.fullmatch(r"[a-z0-9]{16,}(\.\w+)?", pathlib.Path(f["name"]).name, re.I) for f in main)
    msg = ("Files are scrambled names (common on usenet), so the real names show up after downloading. " if obf else "") + \
          ("The book is packed inside an archive (RAR/ZIP); the downloader unpacks it, so the exact book files only show after downloading. " if packed else "")
    return {"files": fs[:200], "count": len(fs), "summary": ", ".join(f"{n} × {e.upper()}" for e, n in exts.most_common(6)), "message": msg.strip(),
            "extras": len(fs) - len(main)}
@app.post("/api/v1/release", dependencies=API)
async def grab(request: Request):
    b = await request.json(); return {"ok": True, "client": await send_to_client(b, b["type"], b.get("item_id"))}

# ---------- library ----------
@app.get("/api/v1/item", dependencies=API)
def items(type: str = None):
    out = rows("SELECT * FROM item WHERE (?1 IS NULL OR type=?1) ORDER BY added DESC", (type,))
    for i in out: i["file_size"] = os.path.getsize(i["path"]) if i["path"] and os.path.exists(i["path"]) else None
    return out
FIELDS = ("series_id", "num", "type", "title", "author", "series", "year", "release_date", "description", "cover", "provider", "ext_id", "publisher", "meta", "monitored", "status")
def find_existing(b):
    if b.get("provider") and b.get("ext_id"):
        r = rows("SELECT id FROM item WHERE provider=? AND ext_id=? AND type=?", (b["provider"], str(b["ext_id"]), b["type"]))
        if r: return r[0]["id"]
    nt = norm(b.get("title"))
    for r in rows("SELECT id, title, author FROM item WHERE type=?", (b["type"],)):
        if norm(r["title"]) == nt and (not b.get("author") or not r["author"] or norm(r["author"].split(",")[0]) == norm(b["author"].split(",")[0])): return r["id"]
    return None
def insert_item(b):
    b = dict(b)
    if isinstance(b.get("meta"), dict): b["meta"] = json.dumps(b["meta"])
    ks = [k for k in FIELDS if k in b]
    c = db.execute(f"INSERT INTO item({','.join(ks)}) VALUES({','.join('?' * len(ks))})", [b[k] for k in ks]); db.commit()
    return c.lastrowid
@app.post("/api/v1/item", dependencies=API)
async def add_item(request: Request):
    b = await request.json(); e = find_existing(b)
    return {"id": e or insert_item(b), "existing": bool(e)}
@app.put("/api/v1/item/{iid}", dependencies=API)
async def upd_item(iid: int, request: Request):
    for k, v in (await request.json()).items():
        if k in ("title", "author", "series", "year", "monitored", "status", "release_date"):
            db.execute(f"UPDATE item SET {k}=? WHERE id=?", (v, iid))
    db.commit(); return rows("SELECT * FROM item WHERE id=?", (iid,))[0]
@app.delete("/api/v1/item/{iid}", dependencies=API)
def del_item(iid: int, deleteFile: int = 0): return {"deleted_files": remove_item(iid, bool(deleteFile))}
def remove_item(iid, delete_files):
    """Remove a title from Bookarr. With delete_files, also delete every file it owns (converted copies and audiobook parts too)."""
    r = rows("SELECT * FROM item WHERE id=?", (iid,)); n = 0
    if not r: return 0
    if delete_files and r[0]["path"]:
        folders = set()
        for f in file_list(r[0]):
            if f["exists"]:
                try: os.remove(f["path"]); n += 1; folders.add(os.path.dirname(f["path"]))
                except OSError as e: log.warning("Could not delete %s: %s", f["path"], e)
        for d in folders: prune_empty(d)
    db.execute("DELETE FROM item WHERE id=?", (iid,)); db.commit(); drop_cover(iid)
    log.info("Removed %s%s", r[0]["title"], f" and deleted {n} file(s)" if n else "")
    return n
@app.post("/api/v1/item/bulk", dependencies=API)
async def item_bulk(request: Request):
    """Act on many titles at once. Body: {"ids": [...], "action": "delete" | "monitor" | "unmonitor", "deleteFiles": true/false}"""
    b = await request.json(); ids = [int(i) for i in b.get("ids", [])]; a = b.get("action")
    if a == "delete":
        files = sum(remove_item(i, bool(b.get("deleteFiles"))) for i in ids)
        return {"removed": len(ids), "deleted_files": files}
    if a in ("monitor", "unmonitor"):
        db.executemany("UPDATE item SET monitored=? WHERE id=?", [(1 if a == "monitor" else 0, i) for i in ids]); db.commit()
        return {"updated": len(ids)}
    raise HTTPException(400, "Unknown action")
def add_size(its):
    for i in its: i["file_size"] = os.path.getsize(i["path"]) if i.get("path") and os.path.exists(i["path"]) else None
    return its
def file_list(it):
    p = it.get("path")
    if not p: return []
    ps, d = [p], os.path.dirname(p)
    if it["type"] == "audiobook" and os.path.isdir(d): ps = [os.path.join(d, f) for f in sorted(os.listdir(d)) if pathlib.Path(f).suffix.lower() in EXT["audiobook"]] or [p]
    elif os.path.isdir(d):  # converted copies sit next to the original with the same name: Book.epub, Book.azw3, Book.pdf
        stem = pathlib.Path(p).stem
        ps += [os.path.join(d, f) for f in sorted(os.listdir(d)) if pathlib.Path(f).stem == stem and os.path.join(d, f) != p
               and pathlib.Path(f).suffix.lower() in ALLFMT[it["type"]]]
    out = []
    for q in ps:
        ex = os.path.exists(q); st = os.stat(q) if ex else None
        out.append({"name": os.path.basename(q), "folder": os.path.dirname(q), "path": q, "ext": pathlib.Path(q).suffix.lstrip(".").upper(), "size": st.st_size if st else None,
                    "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).strftime("%Y-%m-%d") if st else None, "exists": ex, "primary": q == p,
                    "info": file_info(q) if ex else ""})
    return out
@functools.lru_cache(maxsize=512)
def _finfo(q, mtime):
    ext = pathlib.Path(q).suffix.lower()
    try:
        if ext == ".cbz": return f"{len(cbz_pages(q))} pages"
        if ext in (".m4b", ".m4a", ".mp3"):
            a = MutagenFile(q)
            if a and a.info:
                s = int(a.info.length); br = getattr(a.info, "bitrate", 0)
                return f"{s // 3600}h {s % 3600 // 60}m" + (f", {br // 1000} kbps" if br else "")
        if ext == ".epub":
            with zipfile.ZipFile(q) as z: n = sum(1 for x in z.namelist() if x.lower().endswith((".xhtml", ".html", ".htm")))
            return f"{n} sections" if n else ""
        if ext == ".pdf":
            with open(q, "rb") as f: n = len(re.findall(rb"/Type\s*/Page[^s]", f.read(30_000_000)))
            return f"{n} pages" if n else ""
    except Exception: pass
    return ""
def file_info(q):
    try: return _finfo(q, os.path.getmtime(q))
    except OSError: return ""
def prune_empty(d):  # remove now-empty series/author folders, never the library roots themselves
    roots = [str(pathlib.Path(cfg("library")[k]).resolve()) for k in ("comic", "ebook", "audiobook")]; p = pathlib.Path(d).resolve()
    while str(p) not in roots and any(str(p).startswith(r + os.sep) for r in roots):
        try: p.rmdir()
        except OSError: break
        p = p.parent
@app.get("/api/v1/item/{iid}", dependencies=API)
def item_get(iid: int):
    r = rows("SELECT * FROM item WHERE id=?", (iid,))
    if not r: raise HTTPException(404, "That title is no longer in Bookarr.")
    it = add_size(r)[0]; it["files"] = file_list(it); it["history"] = rows(HISTQ + " WHERE g.item_id=? ORDER BY g.id DESC LIMIT 20", (iid,))
    it["meta"] = item_meta(it); it["convert_to"] = CONVERT_TO.get(it["type"], [])
    it["follow"] = rows("SELECT id, title, provider FROM series WHERE id=?", (it["series_id"],)) if it.get("series_id") else []
    if not it["follow"] and it.get("author"):
        it["follow"] = rows("SELECT id, title, provider FROM series WHERE provider='author' AND type=? AND lower(title)=lower(?)", (it["type"], it["author"].split(",")[0].strip()))
    return it
@app.get("/api/v1/item/{iid}/file", dependencies=API)
def item_file(iid: int, path: str = None):
    r = rows("SELECT * FROM item WHERE id=?", (iid,))
    if not r or not r[0]["path"]: raise HTTPException(404)
    p = path if path and path in [f["path"] for f in file_list(r[0])] else r[0]["path"]
    if not os.path.exists(p): raise HTTPException(404, "The file is missing on disk.")
    return FileResponse(p, filename=os.path.basename(p))
@app.delete("/api/v1/item/{iid}/file", dependencies=API)
def item_file_delete(iid: int, path: str, unmonitor: int = 0):
    r = rows("SELECT * FROM item WHERE id=?", (iid,))
    if not r or path not in [f["path"] for f in file_list(r[0])] + [r[0]["path"]]: raise HTTPException(400, "That file does not belong to this title.")
    if os.path.exists(path): os.remove(path)
    db.execute("UPDATE item SET path=NULL, status='missing', monitored=CASE WHEN ?=1 THEN 0 ELSE monitored END WHERE path=?", (unmonitor, path)); db.commit()
    prune_empty(os.path.dirname(path)); log.info("Deleted file %s", path); return {"ok": True}
@app.get("/api/v1/wanted/missing", dependencies=API)
def missing(): return rows("SELECT * FROM item WHERE monitored=1 AND status='missing' ORDER BY release_date DESC")
@app.get("/api/v1/wanted/newest", dependencies=API)
def newest(limit: int = 50): return rows("SELECT * FROM item ORDER BY added DESC LIMIT ?", (limit,))
@app.get("/api/v1/calendar", dependencies=API)
def calendar(start: str = "0000", end: str = "9999"):
    return rows("SELECT * FROM item WHERE release_date BETWEEN ? AND ? ORDER BY release_date", (start, end))

EXT = {"comic": {".cbz", ".cbr", ".pdf"}, "ebook": {".epub", ".mobi", ".azw3", ".pdf"}, "audiobook": {".m4b", ".mp3", ".m4a"}}
# every format a title's files may come in, including converted copies
ALLFMT = {"comic": {".cbz", ".cbr", ".cb7", ".pdf", ".epub"}, "ebook": {".epub", ".kepub", ".mobi", ".azw3", ".pdf", ".fb2", ".txt", ".docx"}, "audiobook": {".m4b", ".mp3", ".m4a"}}
# what Transmute can turn each type into (Calibre for ebooks, its comic converter for comics, ffmpeg for audio)
CONVERT_TO = {"ebook": ["epub", "azw3", "mobi", "pdf", "kepub", "fb2"], "comic": ["cbz", "pdf"], "audiobook": ["m4b", "mp3", "m4a"]}
@app.post("/api/v1/library/scan", dependencies=API)
def scan():
    n = 0
    for t, ext in EXT.items():
        root = pathlib.Path(cfg("library")[t])
        if not root.exists(): continue
        known = {str(pathlib.Path(r["path"]).with_suffix("")) for r in rows("SELECT path FROM item WHERE path IS NOT NULL AND type=?", (t,))}
        for f in sorted(root.rglob("*")):
            if t != "audiobook" and str(f.with_suffix("")) in known: continue  # converted copy of a title already in Bookarr
            if f.suffix.lower() in ext and not rows("SELECT 1 FROM item WHERE path=?", (str(f),)):
                known.add(str(f.with_suffix("")))
                db.execute("INSERT INTO item(type,title,series,path,status) VALUES(?,?,?,?, 'downloaded')", (t, f.stem, f.parent.name if f.parent != root else None, str(f))); n += 1
    db.commit(); log.info("Library scan added %d items", n); return {"added": n}

# ---------- metadata providers ----------
import re, zipfile
UA = {"User-Agent": "Bookarr/0.3"}
def clean(h):  # HTML -> plain text, keeping paragraph breaks so long synopses stay readable
    h = re.sub(r"(?i)<\s*(br|/p|/div|/li|/h\d)\s*/?>", "\n", h or "")
    h = re.sub(r"<[^>]+>", " ", h)
    h = h.replace("&nbsp;", " ").replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'").replace("&lt;", "<").replace("&gt;", ">")
    return "\n".join(l for l in (re.sub(r"[ \t]+", " ", x).strip() for x in h.split("\n")) if l).strip()
yr = lambda v: int(str(v)[:4]) if str(v or "")[:4].isdigit() else None
def uniq(xs, n=12):
    out = []
    for x in xs:
        x = (x or "").strip()
        if x and x.lower() not in [o.lower() for o in out]: out.append(x)
    return out[:n]
LANG = {"en": "English", "eng": "English", "es": "Spanish", "spa": "Spanish", "fr": "French", "fre": "French", "de": "German", "ger": "German", "it": "Italian", "ita": "Italian",
        "ja": "Japanese", "jpn": "Japanese", "pt": "Portuguese", "por": "Portuguese", "nl": "Dutch", "ru": "Russian", "zh": "Chinese"}
async def jget(url, **kw):
    async with httpx.AsyncClient(timeout=20, headers=UA, follow_redirects=True) as x:
        r = await x.get(url, **kw); r.raise_for_status(); return r.json()
async def look_openlibrary(term, t):
    j = await jget("https://openlibrary.org/search.json", params={"q": term, "limit": 12,
        "fields": "key,title,subtitle,author_name,first_publish_year,cover_i,subject,number_of_pages_median,language,isbn,ratings_average,ratings_count,publisher"})
    return [{"type": t, "title": d["title"], "author": ", ".join(d.get("author_name", [])[:2]), "year": d.get("first_publish_year"), "description": "", "provider": "openlibrary", "ext_id": d["key"],
             "publisher": (d.get("publisher") or [None])[0],
             "cover": f"https://covers.openlibrary.org/b/id/{d['cover_i']}-L.jpg" if d.get("cover_i") else None,
             "meta": {"subtitle": d.get("subtitle"), "subjects": uniq(d.get("subject", []), 10), "pages": d.get("number_of_pages_median"),
                      "language": ", ".join(uniq([LANG.get(l, l) for l in d.get("language", [])], 3)), "isbn": (d.get("isbn") or [None])[0],
                      "rating": round(d["ratings_average"], 1) if d.get("ratings_average") else None, "ratings": d.get("ratings_count")}} for d in j.get("docs", [])]
async def look_comic(term):  # ComicVine volumes (needs free API key), falls back to Open Library
    k = cfg("metadata")["comicvineKey"]
    if not k: return await look_openlibrary(term, "comic")
    j = await jget("https://comicvine.gamespot.com/api/search/", params={"api_key": k, "format": "json", "resources": "volume", "query": term, "limit": 12})
    return [{"type": "comic", "title": v["name"], "series": v["name"], "year": yr(v.get("start_year")), "publisher": (v.get("publisher") or {}).get("name"), "issues": v.get("count_of_issues"),
             "description": clean(v.get("description") or v.get("deck")), "cover": (v.get("image") or {}).get("medium_url"), "provider": "comicvine", "ext_id": str(v["id"]),
             "meta": {"issues": v.get("count_of_issues")}} for v in j.get("results", [])]
def gb_item(v, t="ebook"):
    i = v.get("volumeInfo", {}); ids = {x.get("type"): x.get("identifier") for x in i.get("industryIdentifiers", [])}
    cats = [p.strip() for c in i.get("categories", []) for p in c.split("/")]
    return {"type": t, "title": i.get("title"), "author": ", ".join(i.get("authors", [])), "year": yr(i.get("publishedDate")), "release_date": i.get("publishedDate"),
            "publisher": i.get("publisher"), "description": clean(i.get("description")), "provider": "googlebooks", "ext_id": v["id"],
            "cover": ((i.get("imageLinks") or {}).get("thumbnail", "").replace("http://", "https://").replace("&edge=curl", "") or None),
            "meta": {"subtitle": i.get("subtitle"), "genres": uniq([c for c in cats if c.lower() not in ("general",)], 8), "pages": i.get("pageCount"),
                     "language": LANG.get(i.get("language"), i.get("language")), "isbn": ids.get("ISBN_13") or ids.get("ISBN_10"),
                     "rating": i.get("averageRating"), "ratings": i.get("ratingsCount"), "maturity": "Mature" if i.get("maturityRating") == "MATURE" else None}}
async def look_ebook(term):  # Google Books, falls back to Open Library
    p = {"q": term, "maxResults": 12, "printType": "books"}
    if cfg("metadata")["googleBooksKey"]: p["key"] = cfg("metadata")["googleBooksKey"]
    try:
        j = await jget("https://www.googleapis.com/books/v1/volumes", params=p)
        return [gb_item(v) for v in j.get("items", [])]
    except Exception as e:
        log.warning("Google Books failed (%s); using Open Library", e); return await look_openlibrary(term, "ebook")
AUDIBLE_GROUPS = "media,product_desc,contributors,series,product_attrs,product_extended_attrs,category_ladders,rating"
def audible_item(p):
    rt = p.get("runtime_length_min"); cats = [c["name"] for l in p.get("category_ladders", []) for c in l.get("ladder", [])]
    rat = ((p.get("rating") or {}).get("overall_distribution") or {})
    return {"type": "audiobook", "title": p["title"], "author": ", ".join(a["name"] for a in p.get("authors", [])), "series": ((p.get("series") or [{}])[0]).get("title"),
            "num": ((p.get("series") or [{}])[0]).get("sequence"), "year": yr(p.get("release_date")), "release_date": p.get("release_date"), "publisher": p.get("publisher_name"),
            "description": clean(p.get("publisher_summary") or p.get("merchandising_summary")), "provider": "audible", "ext_id": p["asin"],
            "cover": (p.get("product_images") or {}).get("500"),
            "meta": {"subtitle": p.get("subtitle"), "narrator": ", ".join(n["name"] for n in p.get("narrators", [])), "runtime": rt, "genres": uniq(cats, 8),
                     "language": (p.get("language") or "").capitalize() or None, "abridged": p.get("format_type"),
                     "rating": rat.get("display_average_rating"), "ratings": rat.get("num_ratings")}}
async def look_audio(term):  # Audible catalog
    j = await jget(f"https://api.audible.{cfg('metadata')['audibleRegion']}/1.0/catalog/products", params={"title": term, "num_results": 12, "products_sort_by": "Relevance", "response_groups": AUDIBLE_GROUPS})
    return [audible_item(p) for p in j.get("products", [])]
LOOK = {"comic": look_comic, "ebook": look_ebook, "audiobook": look_audio}

async def enrich(d):
    """Fill in the full synopsis, genres, page count etc. from the provider's detail page (search results are often thin)."""
    p, eid, m = d.get("provider"), d.get("ext_id"), dict(d.get("meta") or {})
    try:
        if p == "openlibrary" and eid:
            w = await jget(f"https://openlibrary.org{eid}.json")
            desc = w.get("description"); desc = desc.get("value") if isinstance(desc, dict) else desc
            if desc and not d.get("description"): d["description"] = clean(desc).split("\n----------")[0]
            m["subjects"] = uniq((m.get("subjects") or []) + w.get("subjects", []), 10)
        elif p == "googlebooks" and eid:
            g = gb_item(await jget(f"https://www.googleapis.com/books/v1/volumes/{eid}"), d.get("type", "ebook"))
            if g["description"] and len(g["description"]) > len(d.get("description") or ""): d["description"] = g["description"]
            for k, v in g["meta"].items():
                if v and not m.get(k): m[k] = v
            if g["meta"].get("genres"): m["genres"] = g["meta"]["genres"]
        elif p == "audible" and eid and not m.get("runtime"):
            a = audible_item((await jget(f"https://api.audible.{cfg('metadata')['audibleRegion']}/1.0/catalog/products/{eid}", params={"response_groups": AUDIBLE_GROUPS}))["product"])
            if a["description"] and not d.get("description"): d["description"] = a["description"]
            for k in ("cover", "series", "num", "publisher", "release_date", "author"):
                if a.get(k) and not d.get(k): d[k] = a[k]
            m = {**a["meta"], **{k: v for k, v in m.items() if v}}
        elif p == "comicvine" and eid and cfg("metadata")["comicvineKey"]:
            v = (await jget(f"https://comicvine.gamespot.com/api/volume/4050-{eid}/", params={"api_key": cfg("metadata")["comicvineKey"], "format": "json",
                 "field_list": "description,deck,count_of_issues,characters,concepts,people"})).get("results") or {}
            if not d.get("description"): d["description"] = clean(v.get("description") or v.get("deck"))
            m.update({"issues": v.get("count_of_issues"), "characters": uniq([c["name"] for c in v.get("characters", [])], 10),
                      "subjects": uniq([c["name"] for c in v.get("concepts", [])], 8), "creators": uniq([c["name"] for c in v.get("people", [])], 6)})
    except Exception as e: log.info("Detail lookup for %s skipped: %s", d.get("title"), e)
    d["meta"] = {k: v for k, v in m.items() if v not in (None, "", [], 0)}
    return d

@app.get("/api/v1/lookup", dependencies=API)
async def lookup(term: str, types: str = "comic,ebook,audiobook"):
    ts = [t for t in types.split(",") if t in LOOK]; out = []
    for t, r in zip(ts, await asyncio.gather(*[LOOK[t](term) for t in ts], return_exceptions=True)):
        if isinstance(r, Exception): log.warning("Lookup %s failed: %s", t, r)
        else: out += r
    return out
@app.post("/api/v1/lookup/details", dependencies=API)
async def lookup_details(request: Request): return await enrich(await request.json())
def item_meta(it):
    try: return json.loads(it.get("meta") or "{}")
    except ValueError: return {}
async def refresh_item(iid):
    it = rows("SELECT * FROM item WHERE id=?", (iid,))[0]
    if it.get("provider") and it.get("ext_id") and it["provider"] != "author":
        r = [{k: it[k] for k in ("type", "title", "provider", "ext_id", "description")} | {"meta": item_meta(it)}]
    else:
        r = await LOOK[it["type"]](" ".join(x for x in (it["title"], (it.get("author") or "").split(",")[0]) if x))
        if not r: r = await LOOK[it["type"]](it["title"])
    if not r: return False
    best = await enrich(r[0])
    for k in ("description", "cover", "author", "year", "publisher", "provider", "ext_id", "series", "release_date"):
        if best.get(k) and (k in ("description", "cover", "provider", "ext_id") or not it[k]): db.execute(f"UPDATE item SET {k}=? WHERE id=?", (best[k], iid))
    db.execute("UPDATE item SET meta=? WHERE id=?", (json.dumps({**item_meta(it), **best.get("meta", {})}), iid))
    db.commit(); drop_cover(iid); return True
@app.post("/api/v1/item/{iid}/refresh", dependencies=API)
async def refresh(iid: int):
    if not await refresh_item(iid): raise HTTPException(404, "No metadata match found")
    return rows("SELECT * FROM item WHERE id=?", (iid,))[0]
@app.post("/api/v1/metadata/refresh", dependencies=API)
async def refresh_missing():
    todo = rows("SELECT id FROM item WHERE description IS NULL OR description='' OR meta IS NULL"); n = 0
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
for _f in COVERS.glob("*.img"):  # old cache files were named only by number and could be reused by a new title with the same number
    if "-" not in _f.stem: _f.unlink(missing_ok=True)
def cover_key(iid):
    it = rows("SELECT cover, path FROM item WHERE id=?", (iid,))
    if not it: return None
    import hashlib; return COVERS / f"{iid}-{hashlib.md5(((it[0]['cover'] or '') + '|' + (it[0]['path'] or '')).encode()).hexdigest()[:10]}.img"
def drop_cover(iid):
    for f in COVERS.glob(f"{iid}-*.img"): f.unlink(missing_ok=True)
async def cover_file(iid):
    f = cover_key(iid)
    if not f: raise HTTPException(404)
    if not f.exists():
        drop_cover(iid)
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
        ".mobi": "application/x-mobipocket-ebook", ".azw3": "application/vnd.amazon.ebook", ".m4b": "audio/mp4", ".mp3": "audio/mpeg", ".m4a": "audio/mp4",
        ".kepub": "application/kepub+zip", ".fb2": "application/x-fictionbook+xml", ".cb7": "application/x-cb7", ".txt": "text/plain"}
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
def plain_copy(src, dst):
    """Copy only the file's contents. NTFS and other unassigned drives refuse permission/owner/timestamp changes
    ("Operation not permitted"), which made the old copy fail after the data was already written."""
    tmp = dst.with_name(dst.name + ".part")
    try:
        shutil.copyfile(src, tmp); os.replace(tmp, dst)
    finally:
        if tmp.exists(): tmp.unlink(missing_ok=True)
    try: os.utime(dst, (os.stat(src).st_atime, os.stat(src).st_mtime))
    except OSError: pass
def transfer(src, dst, protocol):
    dst.parent.mkdir(parents=True, exist_ok=True)
    mode = cfg("media")["importMode"]; mode = ("hardlink" if protocol != "usenet" else "move") if mode == "auto" else mode
    try: same = dst.exists() and dst.stat().st_size == src.stat().st_size
    except OSError: same = False
    if same:  # an earlier try already put this exact file there: use it instead of making "Book (1).epub"
        if mode == "move":
            try: os.remove(src)
            except OSError: pass
        return dst
    n = 1
    while dst.exists(): dst = dst.with_name(f"{dst.stem} ({n}){dst.suffix}"); n += 1
    if mode == "move":
        try: os.rename(src, dst)  # same drive: instant
        except OSError:
            plain_copy(src, dst)
            try: os.remove(src)
            except OSError as e: log.warning("Copied %s but could not remove the download copy: %s", src.name, e)
    elif mode == "hardlink":
        try: os.link(src, dst)
        except OSError: plain_copy(src, dst)  # different drives cannot share a file, so copy (the torrent keeps seeding the original)
    else: plain_copy(src, dst)
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
    db.commit()
    # usenet: remove the leftover job folder (nfo/par2 files) so downloads/Books/<Type> stays empty
    base = pathlib.Path(mapped(src_path)); mode = cfg("media")["importMode"]
    if g["protocol"] == "usenet" and mode in ("auto", "move") and base.is_dir() and base.parent.name in FOLDER.values():
        left = [f for f in base.rglob("*") if f.is_file() and f.suffix.lower() in ALLFMT[t] | EXT[t]]
        if not left: shutil.rmtree(base, ignore_errors=True)
    return ids
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
                    await qb_login(x, g)
                    ts = (await x.get(g["url"] + "/api/v2/torrents/info", params={"tag": g["dl_id"]})).json()
                    if ts and ts[0]["progress"] >= 1: done = ts[0].get("content_path") or os.path.join(ts[0]["save_path"], ts[0]["name"])
                    elif ts and ts[0]["state"] in ("error", "missingFiles"): fail_grab(g, ts[0]["state"]); continue
            if done:
                ids = import_grab(g, done); db.execute("UPDATE grab SET state='imported', note=NULL WHERE id=?", (g["id"],)); db.commit(); log.info("Imported %s (%d file(s))", g["title"], len(ids))
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
STOP = {"the", "a", "an", "of", "and", "comic", "comics", "ebook", "audiobook"}
toks = lambda v: re.findall(r"[a-z0-9]+", (v or "").lower().replace("&", " and "))
PACK = re.compile(r"\b(complete|collection|collections|pack|bundle|omnibus|megapack|library)\b|\d{1,4}\s*-\s*\d{1,4}", re.I)
PACKWORDS = re.compile(r"\b(complete|collection|collections|pack|bundle|omnibus|megapack|library|books|novels|anthology|box ?set|boxset|series|trilogy|saga|works)\b", re.I)
def is_pack(term, title):
    """A release that probably holds several books (e.g. "Romance books - ..."), not counting words that are part of what you searched for."""
    q = set(toks(term))
    return any(not set(toks(m.group(0))) <= q for m in PACKWORDS.finditer(title)) or bool(re.search(r"\b\d{1,4}\s*-\s*\d{1,4}\b", title) and not re.search(r"\d\s*-\s*\d", term))
FMTS = {"comic": ["cbz", "cbr", "cb7", "pdf", "epub"], "ebook": ["epub", "azw3", "mobi", "pdf", "kepub", "azw", "fb2", "txt"], "audiobook": ["m4b", "mp3", "m4a", "flac", "aac"]}
def detect_fmt(title, t):
    f = [x.upper() for x in FMTS.get(t, []) if re.search(rf"\b{x}\b", title.lower())]
    return ", ".join(f) or None
def skip_reason(r, it, term):
    """Why automatic grabbing would pass over this release (None = it is allowed)."""
    q = cfg("quality"); t = r["type"]; cap = (q.get(t + "MaxMB") or 0) * 1048576
    if not r.get("downloadUrl"): return "No download link"
    if rows("SELECT 1 FROM blocklist WHERE title=?", (r["title"],)): return "Failed before (blocklisted)"
    if it and not matches(it, r): return "Does not match this title closely enough"
    if not it and relevance(term, r["title"]) == 0: return "Does not match the search closely enough"
    if cap and (r["size"] or 0) > cap: return f"Bigger than your {q.get(t + 'MaxMB')} MB limit"
    if r.get("seeders") is not None and r["seeders"] < cfg("media")["minSeeders"]: return "Not enough seeders"
    if q.get("requirePreferred") and fmt_rank(r["title"], t) >= len(q[t].split(",")): return "Not one of your preferred formats"
    return None
def pick_key(r, term):
    # single book first, then your preferred format order (Settings > Quality), then most seeders, then newest
    return (is_pack(term, r["title"]), fmt_rank(r["title"], r["type"]), -(r.get("seeders") or 0), r.get("age") if r.get("age") is not None else 99999)
def relevance(term, title):
    """0 = not a match. Every word in the search must be in the release and every number must be a number in it (so 'Deadpool 5' skips 'Deadpool 50')."""
    q = [t for t in toks(term) if t not in STOP]
    if not q: return 1.0
    tt = toks(title); ts = set(tt)
    if any(w not in ts for w in q if not w.isdigit()): return 0.0
    tn = {int(t) for t in tt if t.isdigit()}
    nums = [int(t) for t in q if t.isdigit()]
    if any(n not in tn for n in nums): return 0.0
    score = 1.0 + (1.0 if " ".join(q) in " ".join(tt) else 0.0)
    if nums and PACK.search(title): score -= 0.6
    return max(score, 0.1)
def matches(it, r):
    num = str(it.get("num") or "")
    term = f'{it["series"]} {int(float(num))}' if it.get("series") and re.fullmatch(r"\d+(\.\d+)?", num) else " ".join(toks(it["title"])[:6])
    return relevance(term, r["title"]) > 0
async def search_item(it):
    db.execute("UPDATE item SET last_search=datetime('now') WHERE id=?", (it["id"],)); db.commit()
    q = cfg("quality"); cap = (q.get(it["type"] + "MaxMB") or 0) * 1048576; bl = {b["title"] for b in rows("SELECT title FROM blocklist")}
    for term in terms(it):
        rs = [r for r in await prowlarr(term, it["type"]) if r["downloadUrl"] and r["title"] not in bl and matches(it, r) and (not cap or (r["size"] or 0) <= cap)
              and (r["seeders"] is None or r["seeders"] >= cfg("media")["minSeeders"])]
        if q.get("requirePreferred"): rs = [r for r in rs if fmt_rank(r["title"], it["type"]) < len(q[it["type"]].split(","))]
        if rs:
            for r in rs: r["age"] = age_days(r.get("publishDate"))
            rs.sort(key=lambda r: pick_key(r, term))
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
        name = sr["title"]; nm = norm(name); seen = set()
        same = lambda names: any(norm(a) == nm for a in names)
        junk = re.compile(r"\b(summary|study guide|workbook|analysis of|sparknotes|cliffsnotes|boxed set|box set)\b", re.I)
        if sr["type"] == "audiobook":
            j = await jget(f"https://api.audible.{cfg('metadata')['audibleRegion']}/1.0/catalog/products", params={"author": name, "num_results": 50, "products_sort_by": "-ReleaseDate", "response_groups": AUDIBLE_GROUPS})
            for v in j.get("products", []):
                a = audible_item(v)
                if not same(x["name"] for x in v.get("authors", [])) or norm(a["title"]) in seen or junk.search(a["title"]): continue
                seen.add(norm(a["title"])); out.append(a)
        else:
            p2 = {"q": f'inauthor:"{name}"', "maxResults": 40, "orderBy": "newest", "printType": "books"}
            if cfg("metadata")["googleBooksKey"]: p2["key"] = cfg("metadata")["googleBooksKey"]
            try:
                j = await jget("https://www.googleapis.com/books/v1/volumes", params=p2)
                for v in j.get("items", []):
                    a = gb_item(v, sr["type"])
                    if not a["title"] or not same(v["volumeInfo"].get("authors", [])) or norm(a["title"]) in seen or junk.search(a["title"]): continue
                    seen.add(norm(a["title"])); a["author"] = name; out.append(a)
            except Exception as e:  # Google Books limits searches without an API key; Open Library below takes over
                log.warning("Google Books author lookup for %s failed (%s); using Open Library", name, e)
            if not out:
                j = await jget("https://openlibrary.org/search.json", params={"author": name, "limit": 60, "sort": "new",
                    "fields": "key,title,subtitle,author_name,first_publish_year,cover_i,subject,number_of_pages_median,language,isbn,ratings_average,ratings_count,publisher"})
                for d in j.get("docs", []):
                    if not same(d.get("author_name", [])) or norm(d["title"]) in seen or junk.search(d["title"]): continue
                    seen.add(norm(d["title"]))
                    out.append({"ext_id": d["key"], "title": d["title"], "author": name, "provider": "openlibrary", "release_date": str(d["first_publish_year"]) if d.get("first_publish_year") else None,
                                "cover": f"https://covers.openlibrary.org/b/id/{d['cover_i']}-L.jpg" if d.get("cover_i") else None, "publisher": (d.get("publisher") or [None])[0],
                                "meta": {"subjects": uniq(d.get("subject", []), 10), "pages": d.get("number_of_pages_median"), "isbn": (d.get("isbn") or [None])[0],
                                         "rating": round(d["ratings_average"], 1) if d.get("ratings_average") else None, "ratings": d.get("ratings_count")}})
    return out
def want_monitored(sr, release_date):
    mode = sr.get("mon_mode") or "all"
    if not sr["monitored"] or mode == "none": return 0
    if mode == "all": return 1
    return 1 if release_date and str(release_date)[:10] >= str(sr["added"])[:10] else 0  # 'future': only books that come out after you followed
async def sync_series(sid):
    sr = rows("SELECT * FROM series WHERE id=?", (sid,))[0]; added = 0
    have = {}
    for e in rows("SELECT * FROM item WHERE series=? AND type=? AND series_id IS NULL AND status='downloaded'", (sr["title"], sr["type"])):
        m = re.findall(r"(?<!\d)0*(\d{1,4})(?!\d)", e["title"])
        if m: have[m[-1]] = e
    linked = {norm(r["title"]) for r in rows("SELECT title FROM item WHERE series_id=?", (sid,))}
    loose = {}  # books you already added by hand: link them to the author instead of adding them twice
    if sr["provider"] == "author":
        for e in rows("SELECT * FROM item WHERE type=? AND series_id IS NULL AND author IS NOT NULL", (sr["type"],)):
            if norm(sr["title"]) in [norm(a) for a in e["author"].split(",")]: loose[norm(e["title"])] = e
    for i in await fetch_series_items(sr):
        if rows("SELECT 1 FROM item WHERE series_id=? AND ext_id=?", (sid, i["ext_id"])): continue
        if sr["provider"] == "author":
            if norm(i["title"]) in linked: continue
            if norm(i["title"]) in loose:
                db.execute("UPDATE item SET series_id=? WHERE id=?", (sid, loose.pop(norm(i["title"]))["id"])); linked.add(norm(i["title"])); continue
        try: n = str(int(float(i.get("num") or "")))
        except ValueError: n = None
        if n and n in have and sr["provider"] != "author":
            db.execute("UPDATE item SET series_id=?, ext_id=?, num=?, release_date=?, cover=COALESCE(cover,?) WHERE id=?", (sid, i["ext_id"], i["num"], i.get("release_date"), i.get("cover"), have[n]["id"])); continue
        insert_item({"type": sr["type"], "title": i["title"], "series": i.get("series") if sr["provider"] == "author" else sr["title"], "series_id": sid, "num": i.get("num"),
                     "ext_id": i["ext_id"], "release_date": i.get("release_date"), "year": yr(i.get("release_date")), "cover": i.get("cover"), "author": i.get("author") or sr["author"],
                     "description": i.get("description"), "publisher": i.get("publisher"), "meta": i.get("meta") or {},
                     "provider": i.get("provider") or sr["provider"], "monitored": want_monitored(sr, i.get("release_date")), "status": "missing"})
        linked.add(norm(i["title"])); added += 1
    db.commit(); log.info("%s %s synced, %d new items", "Author" if sr["provider"] == "author" else "Series", sr["title"], added); return added
SQ = ("SELECT s.*, COUNT(i.id) total, COALESCE(SUM(i.status='downloaded'),0) have, COALESCE(SUM(i.monitored=1 AND i.status!='downloaded'),0) wanted, MAX(i.release_date) latest "
      "FROM series s LEFT JOIN item i ON i.series_id=s.id")
@app.get("/api/v1/series", dependencies=API)
def series_list(type: str = None, kind: str = None):
    """kind=author for followed authors, kind=series for series (ComicVine runs, Audible series)."""
    w = " WHERE (?1 IS NULL OR s.type=?1) AND (?2 IS NULL OR (?2='author' AND s.provider='author') OR (?2='series' AND s.provider!='author'))"
    return rows(SQ + w + " GROUP BY s.id ORDER BY s.title", (type, kind))
async def follow(b):
    """Start following a series or author. Returns (id, number of books added)."""
    e = rows("SELECT id FROM series WHERE provider=? AND (ext_id=? OR (provider='author' AND type=? AND lower(title)=lower(?)))",
             (b.get("provider"), str(b.get("ext_id")), b["type"], b["title"]))
    if e: return e[0]["id"], await sync_series(e[0]["id"]), True
    mode = b.get("mon_mode") or ("future" if b.get("provider") == "author" else "all")
    sid = db.execute("INSERT INTO series(type,title,author,provider,ext_id,cover,description,year,publisher,mon_mode) VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (b["type"], b["title"], b.get("author"), b.get("provider"), str(b.get("ext_id")), b.get("cover"), b.get("description"), b.get("year"), b.get("publisher"), mode)).lastrowid; db.commit()
    try: added = await sync_series(sid)
    except Exception as e:
        if b.get("provider") != "author":
            db.execute("DELETE FROM series WHERE id=?", (sid,)); db.commit(); raise
        log.warning("Following %s, but their book list could not be fetched yet: %s", b["title"], e); added = 0  # retried by the 12-hour refresh
    return sid, added, False
@app.post("/api/v1/series", dependencies=API)
async def add_series(request: Request):
    b = await request.json(); sid, added, existed = await follow(b)
    if existed: raise HTTPException(409, "Already monitored")
    return {"id": sid, "added": added}
@app.get("/api/v1/series/{sid}", dependencies=API)
def series_get(sid: int):
    r = rows(SQ + " WHERE s.id=? GROUP BY s.id", (sid,))
    if not r: raise HTTPException(404, "That series or author is no longer followed.")
    return {"series": r[0], "items": add_size(rows("SELECT * FROM item WHERE series_id=? ORDER BY CAST(num AS REAL), release_date DESC", (sid,)))}
@app.put("/api/v1/series/{sid}", dependencies=API)
async def series_put(sid: int, request: Request):
    b = await request.json()
    if "monitored" in b: db.execute("UPDATE series SET monitored=? WHERE id=?", (b["monitored"], sid))
    if b.get("mon_mode") in ("future", "all", "none"): db.execute("UPDATE series SET mon_mode=? WHERE id=?", (b["mon_mode"], sid))
    sr = rows("SELECT * FROM series WHERE id=?", (sid,))[0]
    for it in rows("SELECT id, release_date FROM item WHERE series_id=? AND status!='downloaded'", (sid,)):
        db.execute("UPDATE item SET monitored=? WHERE id=?", (want_monitored(sr, it["release_date"]), it["id"]))
    db.commit(); return {"ok": True}

# ---------- one-step add: what the buttons on a search result do ----------
@app.post("/api/v1/add", dependencies=API)
async def add_flow(request: Request):
    """mode = search (add, monitor, download the best release now)
              author (same, plus follow the author for their other/new books)
              series (follow the whole comic/audiobook series)
              manual (add + monitor only; you then pick a release yourself)"""
    b = await request.json(); mode = b.pop("mode", "search"); b.pop("issues", None); out = {}
    if mode == "series":
        sid, added, existed = await follow(b); return {"series_id": sid, "added": added, "existing": existed}
    if not b.get("description") or not b.get("meta"): b = await enrich(b)
    iid = find_existing(b); out["existing"] = bool(iid)
    if not iid: iid = insert_item({**b, "monitored": 1, "status": "missing"})
    else: db.execute("UPDATE item SET monitored=1 WHERE id=?", (iid,)); db.commit()
    out["id"] = iid
    if mode == "author" and b.get("author"):
        name = b["author"].split(",")[0].strip()
        try:
            sid, added, existed = await follow({"type": b["type"], "title": name, "author": name, "provider": "author", "ext_id": name})
            db.execute("UPDATE item SET series_id=COALESCE(series_id, ?) WHERE id=?", (sid, iid)); db.commit()
            out["author"] = {"id": sid, "name": name, "added": added, "existing": existed}
        except Exception as e:
            log.warning("Could not follow %s: %s", name, e); out["author_error"] = f"Could not follow {name} right now ({e}). The book itself was added."
    it = rows("SELECT * FROM item WHERE id=?", (iid,))[0]
    if mode in ("search", "author") and it["status"] == "missing":  # already downloading or downloaded: do not grab twice
        try: out["release"] = await search_item(it)
        except HTTPException as e: out["search_error"] = e.detail
    return out
@app.delete("/api/v1/series/{sid}", dependencies=API)
def series_del(sid: int):
    sr = rows("SELECT provider FROM series WHERE id=?", (sid,))
    # authors: forget the books you never picked; keep anything you monitored or downloaded. series: forget every issue not downloaded.
    db.execute("DELETE FROM item WHERE series_id=? AND " + ("status='missing' AND monitored=0" if sr and sr[0]["provider"] == "author" else "status!='downloaded'"), (sid,)); db.execute("UPDATE item SET series_id=NULL WHERE series_id=?", (sid,)); db.execute("DELETE FROM series WHERE id=?", (sid,)); db.commit(); return {}
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
    return {"version": "0.3.0", "uptime": f"{up // 3600}h {up % 3600 // 60}m", "disks": disks, "counts": status()["counts"]}
@app.get("/api/v1/health", dependencies=API)
async def health():
    out = []; add = lambda ok, name, msg: out.append({"ok": ok, "name": name, "message": msg})
    ind = cfg("indexer")
    if not ind["prowlarrUrl"]: add(False, "Indexers", "Set the Prowlarr URL and API key in Settings > Indexers")
    else:
        try:
            async with httpx.AsyncClient(timeout=8) as x: r = await x.get(norm_url(ind["prowlarrUrl"]) + "/api/v1/system/status", headers={"X-Api-Key": ind["prowlarrKey"]})
            add(r.status_code == 200, "Prowlarr", "Connected" if r.status_code == 200 else f"Prowlarr answered {r.status_code}; check the API key")
        except Exception: add(False, "Prowlarr", f"Cannot reach {ind['prowlarrUrl']}")
    cl = rows("SELECT * FROM client WHERE enabled=1")
    if not cl: add(False, "Download clients", "Add SABnzbd or qBittorrent in Settings > Download clients")
    for c in cl:
        try:
            async with httpx.AsyncClient(timeout=8) as x:
                if c["kind"] == "sabnzbd": r = await x.get(c["url"] + "/api", params={"mode": "version", "apikey": c["apikey"], "output": "json"})
                else:
                    await qb_login(x, c); r = await x.get(c["url"] + "/api/v2/app/version")
            add(r.status_code == 200, c["name"] or c["kind"], "Connected" if r.status_code == 200 else "Login failed; check credentials")
        except Exception: add(False, c["name"] or c["kind"], f"Cannot reach {c['url']}")
    for t in TYPES:
        pth = cfg("library")[t]; ok = os.path.isdir(pth) and os.access(pth, os.W_OK); add(ok, f"{t.capitalize()} folder", pth if ok else f"{pth} is missing or not writable; mount it in Docker")
    if not cfg("metadata")["comicvineKey"]: add(False, "ComicVine", "No API key set: comic series monitoring is disabled")
    if cfg("converter")["transmuteUrl"]:
        t = await converter_test(); add(t["ok"], "Transmute converter", t["message"])
    else: add(True, "Transmute converter", "Not set up (optional). Needed to convert formats and to make Kindle copies of EPUB-only books.")
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
# ---------- converting files with Transmute (http://your-transmute:3313, API key from its Settings > API keys) ----------
JOBS = {}
def tm_headers():
    k = (cfg("converter")["transmuteKey"] or "").strip(); return {"Authorization": f"Bearer {k}"} if k else {}
@app.post("/api/v1/converter/test", dependencies=API)
async def converter_test():
    url = norm_url(cfg("converter")["transmuteUrl"])
    if not url: return {"ok": False, "message": "Enter your Transmute address first, like http://192.168.0.185:3313"}
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as x:
            r = await x.get(url + "/api/converters", headers=tm_headers())
            if r.status_code == 401: return {"ok": False, "message": "Reached Transmute, but it rejected the API key. Create one in Transmute under Settings > API keys and paste it here."}
            if r.status_code != 200 or "json" not in r.headers.get("content-type", ""): return {"ok": False, "message": f"That address answered (HTTP {r.status_code}) but not like Transmute. Use just http://ip:port."}
            j = r.json(); n = len(j.get("converters", j) if isinstance(j, dict) else j)
            return {"ok": True, "message": f"Connected to Transmute ({n} converters available)"}
    except httpx.ConnectError: return {"ok": False, "message": f"Cannot connect to {url}. Check the address and port."}
    except Exception as e: return {"ok": False, "message": f"Failed: {e}"}
async def tm_convert(src, fmt):
    """Upload src to Transmute, convert it to fmt, save the result next to the original (Book.epub -> Book.azw3). Returns the new path."""
    src = pathlib.Path(src); fmt = fmt.lower().lstrip("."); dst = src.with_suffix("." + fmt)
    if dst.exists(): return dst
    url = norm_url(cfg("converter")["transmuteUrl"])
    if not url: raise HTTPException(400, "Converting needs Transmute. Add its address and API key in Settings > Converter.")
    async with httpx.AsyncClient(timeout=httpx.Timeout(3600, connect=15), headers=tm_headers(), follow_redirects=True) as x:
        with open(src, "rb") as f: r = await x.post(url + "/api/files", files={"file": (src.name, f, "application/octet-stream")})
        if r.status_code == 401: raise HTTPException(502, "Transmute rejected the API key (Settings > Converter).")
        if r.status_code == 422: raise HTTPException(400, f"Transmute cannot convert {src.suffix.upper().lstrip('.')} files.")
        if r.status_code != 200: raise HTTPException(502, f"Transmute upload failed (HTTP {r.status_code}).")
        up = r.json()["metadata"]; fid = up["id"]; cid = None
        try:
            ok = up.get("compatible_formats") or {}
            if ok and fmt not in ok: raise HTTPException(400, f"Transmute cannot turn {src.suffix.upper().lstrip('.')} into {fmt.upper()}. It can make: {', '.join(sorted(ok)).upper()}.")
            c = await x.post(url + "/api/conversions", json={"id": fid, "output_format": fmt})
            if c.status_code != 200: raise HTTPException(502, "Transmute: " + str((c.json() if "json" in c.headers.get("content-type", "") else {}).get("detail") or f"HTTP {c.status_code}"))
            cid = c.json()["id"]; tmp = dst.with_name(dst.name + ".part")
            async with x.stream("GET", f"{url}/api/files/{cid}") as d:
                if d.status_code != 200: raise HTTPException(502, f"Could not download the converted file from Transmute (HTTP {d.status_code}).")
                with open(tmp, "wb") as out:
                    async for chunk in d.aiter_bytes(1 << 20): out.write(chunk)
            os.replace(tmp, dst)
        finally:  # tidy up Transmute's copies
            for path in ([f"/api/conversions/{cid}"] if cid else []) + [f"/api/files/{fid}"]:
                try: await x.delete(url + path)
                except Exception: pass
    log.info("Converted %s to %s", src.name, fmt.upper()); return dst
async def run_job(jid):
    j = JOBS[jid]
    try:
        j["result"] = str(await tm_convert(j["path"], j["to"])); j["state"] = "done"; j["message"] = f"Made {pathlib.Path(j['result']).name}"
    except HTTPException as e: j["state"] = "failed"; j["message"] = e.detail
    except Exception as e: j["state"] = "failed"; j["message"] = str(e)
    if j["state"] == "failed": log.warning("Conversion of %s failed: %s", pathlib.Path(j["path"]).name, j["message"])
@app.post("/api/v1/item/{iid}/convert", dependencies=API)
async def item_convert(iid: int, request: Request):
    b = await request.json(); r = rows("SELECT * FROM item WHERE id=?", (iid,))
    if not r: raise HTTPException(404)
    fs = [f["path"] for f in file_list(r[0]) if f["exists"]]; path = b.get("path") or r[0]["path"]
    if path not in fs: raise HTTPException(400, "That file does not belong to this title.")
    if (b.get("to") or "").lower() not in CONVERT_TO.get(r[0]["type"], []): raise HTTPException(400, "Pick one of: " + ", ".join(CONVERT_TO.get(r[0]["type"], [])))
    if not norm_url(cfg("converter")["transmuteUrl"]): raise HTTPException(400, "Converting needs Transmute. Add its address and API key in Settings > Converter.")
    jid = secrets.token_hex(4); JOBS[jid] = {"id": jid, "item_id": iid, "path": path, "to": b["to"].lower(), "state": "running", "message": "Converting…", "started": time.time()}
    asyncio.create_task(run_job(jid)); return JOBS[jid]
@app.get("/api/v1/convert/jobs", dependencies=API)
def convert_jobs(item: int = None):
    for k in [k for k, j in JOBS.items() if j["state"] != "running" and time.time() - j["started"] > 3600]: JOBS.pop(k)
    return [j for j in JOBS.values() if item is None or j["item_id"] == item]

# ---------- Kindle: email to Send-to-Kindle, or download a Kindle-ready file (USB or the Kindle's own web browser) ----------
EMAIL_OK = (".epub", ".pdf", ".docx", ".txt")  # what Amazon's Send to Kindle accepts (it no longer takes MOBI/AZW3)
def kindle_device_order():
    return [".azw3", ".mobi"] if (cfg("email").get("kindleFormat") or "azw3").lower() != "mobi" else [".mobi", ".azw3"]
async def kindle_ready(it, for_email):
    """Pick (or make) the file a Kindle can open. Converts with Transmute when needed."""
    fs = {pathlib.Path(f["path"]).suffix.lower(): f["path"] for f in file_list(it) if f["exists"]}
    if not fs: raise HTTPException(404, "There is no downloaded file for this title yet.")
    if it["type"] == "audiobook": raise HTTPException(400, "Kindles cannot play audiobook files.")
    want = ([".epub", ".pdf"] if it["type"] == "ebook" else [".pdf"]) if for_email else (kindle_device_order() + [".pdf"] if it["type"] == "ebook" else [".pdf"])
    for w in want:
        if w in fs: return pathlib.Path(fs[w])
    src = next((fs[e] for e in (".epub", ".azw3", ".mobi", ".kepub", ".fb2", ".cbz", ".cbr", ".cb7", ".pdf") if e in fs), next(iter(fs.values())))
    target = want[0].lstrip(".")
    try: return await tm_convert(src, target)
    except HTTPException as e:
        raise HTTPException(e.status_code, f"This title is only available as {', '.join(x.upper().lstrip('.') for x in fs)}, and making a Kindle copy ({target.upper()}) needs the converter: {e.detail}")
def smtp_send(it, p):
    e = cfg("email"); p = pathlib.Path(p)
    if p.stat().st_size > 50 * 1048576: raise HTTPException(400, f"{p.name} is {p.stat().st_size // 1048576} MB. Send to Kindle only takes files up to 50 MB; use the Kindle download page or USB instead.")
    m = EmailMessage(); m["From"] = e["from"] or e["user"]; m["To"] = e["kindleAddress"]; m["Subject"] = it["title"]; m.set_content("Sent by Bookarr")
    mt = (MIME.get(p.suffix.lower()) or "application/octet-stream").split("/")
    m.add_attachment(p.read_bytes(), maintype=mt[0], subtype=mt[1], filename=p.name)
    with (smtplib.SMTP_SSL if int(e["smtpPort"]) == 465 else smtplib.SMTP)(e["smtpHost"], int(e["smtpPort"]), timeout=120) as s:
        if int(e["smtpPort"]) != 465: s.starttls()
        if e["user"]: s.login(e["user"], e["password"])
        s.send_message(m)
@app.post("/api/v1/item/{iid}/send", dependencies=API)
async def item_send(iid: int):
    e = cfg("email")
    if not (e["smtpHost"] and e["kindleAddress"]): raise HTTPException(400, "Set your SMTP server and Kindle address in Settings > Kindle")
    it = rows("SELECT * FROM item WHERE id=?", (iid,))[0]; p = await kindle_ready(it, True)
    await asyncio.to_thread(smtp_send, it, p); log.info("Sent %s to Kindle", p.name); return {"ok": True, "file": p.name}
@app.post("/api/v1/kindle/send", dependencies=API)
async def kindle_send_many(request: Request):
    """Send several titles to Kindle in one go. Body: {"ids": [1,2,3]}"""
    ok, bad = [], []
    for iid in (await request.json()).get("ids", [])[:50]:
        try: await item_send(iid); ok.append(iid)
        except HTTPException as e: bad.append({"id": iid, "error": e.detail})
        await asyncio.sleep(1)
    return {"sent": len(ok), "failed": bad}
@app.get("/api/v1/item/{iid}/kindle", dependencies=API)
async def item_kindle(iid: int):
    p = await kindle_ready(rows("SELECT * FROM item WHERE id=?", (iid,))[0], False)
    return FileResponse(p, media_type=MIME.get(p.suffix.lower(), "application/octet-stream"), filename=p.name)

KPAGE = 25
@app.get("/kindle", dependencies=OA)
def kindle_page(q: str = "", t: str = "ebook", sort: str = "title", page: int = 0):
    """Plain page for the Kindle's own web browser: tap a title and the Kindle downloads it."""
    k = OPDSKEY.get(); ks = f"&apikey={quote(k)}" if k else ""
    t = t if t in ("ebook", "comic") else "ebook"
    order = {"title": "title COLLATE NOCASE", "author": "author COLLATE NOCASE, title COLLATE NOCASE", "new": "added DESC"}.get(sort, "title COLLATE NOCASE")
    its = rows(f"SELECT id,title,author,series,num FROM item WHERE type=? AND path IS NOT NULL AND status='downloaded' AND (title LIKE ? OR author LIKE ? OR series LIKE ?) ORDER BY {order} LIMIT ? OFFSET ?",
               (t, f"%{q}%", f"%{q}%", f"%{q}%", KPAGE + 1, page * KPAGE))
    link = lambda **kw: "/kindle?" + "&".join(f"{a}={quote(str(v))}" for a, v in {"q": q, "t": t, "sort": sort, "page": 0, **kw}.items()) + ks
    tab = lambda v, l: f'<b>{l}</b>' if t == v else f'<a href="{xe(link(t=v))}">{l}</a>'
    srt = lambda v, l: f'<b>{l}</b>' if sort == v else f'<a href="{xe(link(sort=v))}">{l}</a>'
    li = "".join(f'<li><a href="/kindle/get/{i["id"]}?x=1{ks}">{xe(i["title"])}</a><br><small>{xe(i["author"] or i["series"] or "")}</small></li>' for i in its[:KPAGE]) or "<li>Nothing here yet.</li>"
    nav = (f'<a href="{xe(link(page=page - 1))}">&laquo; Previous</a> ' if page else "") + (f'<a href="{xe(link(page=page + 1))}">Next &raquo;</a>' if len(its) > KPAGE else "")
    html = (f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Bookarr for Kindle</title>'
            '<style>body{font-family:serif;font-size:20px;margin:16px;color:#000;background:#fff}a{color:#000}li{margin:0 0 18px}small{color:#444}input{font-size:20px}</style></head><body>'
            f'<h1>Bookarr</h1><p>{tab("ebook", "Ebooks")} &nbsp; {tab("comic", "Comics")}</p>'
            f'<form action="/kindle"><input type="hidden" name="t" value="{t}">' + (f'<input type="hidden" name="apikey" value="{xe(k)}">' if k else "") +
            f'<input name="q" value="{xe(q)}" placeholder="Search"> <input type="submit" value="Go"></form>'
            f'<p>Sort: {srt("title", "Title")} &middot; {srt("author", "Author")} &middot; {srt("new", "Newest")}</p>'
            f'<p><small>Tap a title to download it to this Kindle. The first download of a title can take up to a minute while Bookarr makes a Kindle copy.</small></p><ol start="{page * KPAGE + 1}">{li}</ol><p>{nav}</p></body></html>')
    return Response(html, media_type="text/html")
@app.get("/kindle/get/{iid}", dependencies=OA)
async def kindle_get(iid: int):
    r = rows("SELECT * FROM item WHERE id=?", (iid,))
    if not r: raise HTTPException(404)
    try: p = await kindle_ready(r[0], False)
    except HTTPException as e:
        return Response(f'<!doctype html><html><body style="font-size:20px;font-family:serif"><p>{xe(e.detail)}</p><p><a href="javascript:history.back()">Back</a></p></body></html>', status_code=e.status_code, media_type="text/html")
    return FileResponse(p, media_type=MIME.get(p.suffix.lower(), "application/octet-stream"), filename=p.name)
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

# ---------- optional SeerrNG connectors (each in its own folder; both OFF until enabled in Settings > SeerrNG) ----------
import sys as _sys, readarr_compat, mylar_compat
readarr_compat.register(app, _sys.modules[__name__]); mylar_compat.register(app, _sys.modules[__name__])

SD = pathlib.Path(__file__).parent / "static"
try:
    SD.mkdir(exist_ok=True)
    if (SD.parent / "index.html").exists(): shutil.copy(SD.parent / "index.html", SD / "index.html")
except OSError: pass
app.mount("/", StaticFiles(directory=SD, html=True))
