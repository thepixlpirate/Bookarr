"""Shared test setup for Bookarr.

Every test run uses a brand-new, throwaway data folder and library folder, so tests
never touch a real Bookarr database or real books. Internet lookups (Google Books,
Open Library, Audible, ComicVine) and indexer searches are replaced with small fake
answers, so the tests run offline and always give the same results.

Run them from the repository folder with:  python -m pytest -q
"""
import os, sys, pathlib, tempfile, shutil, zipfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
TMP = pathlib.Path(tempfile.mkdtemp(prefix="bookarr-tests-"))
os.environ["DATA_DIR"] = str(TMP / "config")
sys.path.insert(0, str(ROOT))

import pytest
from fastapi.testclient import TestClient

import main  # noqa: E402  (must come after DATA_DIR is set)

LIB = TMP / "books"
DL = TMP / "downloads"
main.setcfg("library", {"comic": str(LIB / "Comics"), "ebook": str(LIB / "Ebooks"), "audiobook": str(LIB / "Audiobooks"), "downloads": str(DL)})
main.setcfg("metadata", {"comicvineKey": "test-cv-key"})
main.setcfg("indexer", {"prowlarrUrl": "http://prowlarr.invalid", "prowlarrKey": "x"})
for k in ("comic", "ebook", "audiobook"): os.makedirs(main.cfg("library")[k], exist_ok=True)
os.makedirs(DL, exist_ok=True)
KEY = main.cfg("general")["apiKey"]


# ---------- fake internet ----------
GB = {
    "gb1": {"title": "Haunting Adeline", "authors": ["H. D. Carlton"], "publishedDate": "2021-08-01", "description": "<p>The Manipulator.</p>",
            "categories": ["Fiction / Romance / Dark"], "pageCount": 624, "language": "en", "industryIdentifiers": [{"type": "ISBN_13", "identifier": "9781957635002"}],
            "imageLinks": {"thumbnail": "http://books.example/hauntingadeline.jpg"}},
    "gb2": {"title": "Hunting Adeline", "authors": ["H. D. Carlton"], "publishedDate": "2022-01-01", "industryIdentifiers": [{"type": "ISBN_13", "identifier": "9781957635019"}]},
    "gb3": {"title": "Mistborn", "authors": ["Brandon Sanderson"], "publishedDate": "2006-07-17", "industryIdentifiers": [{"type": "ISBN_10", "identifier": "0765311780"}]},
}
AUDIBLE = [
    {"asin": "B0AUDIO01", "title": "Haunting Adeline", "authors": [{"name": "H. D. Carlton"}], "narrators": [{"name": "Teddy Hamilton"}], "release_date": "2021-09-01",
     "runtime_length_min": 1200, "publisher_summary": "Audio edition.", "product_images": {"500": "http://audio.example/ha.jpg"}},
    {"asin": "B0AUDIO02", "title": "Mistborn", "authors": [{"name": "Brandon Sanderson"}], "release_date": "2008-01-01"},
]
CV_VOLUME = {"id": 4050, "name": "Deadpool", "start_year": "2024", "publisher": {"name": "Marvel"}, "image": {"medium_url": "http://cv.example/dp.jpg"},
             "description": "<p>Merc with a mouth.</p>", "count_of_issues": 3}
CV_ISSUES = [{"id": 9001 + n, "issue_number": str(n + 1), "store_date": f"2024-0{n + 1}-01", "image": {"medium_url": None}} for n in range(3)]


def _match(q, title, authors):
    q = q.lower()
    if q.startswith("isbn:"): return False
    return any(w in (title + " " + " ".join(authors)).lower() for w in q.replace('"', " ").replace("inauthor:", " ").split() if len(w) > 2)


async def fake_jget(url, **kw):
    p = kw.get("params", {}) or {}
    if "googleapis.com/books/v1/volumes/" in url:
        vid = url.rsplit("/", 1)[-1]
        if vid not in GB: raise RuntimeError("404")
        return {"id": vid, "volumeInfo": GB[vid]}
    if "googleapis.com/books/v1/volumes" in url:
        q = p.get("q", "")
        if q.lower().startswith("isbn:"):
            isbn = q.split(":", 1)[1].replace("-", "")
            from readarr_compat import isbn13  # Google matches either ISBN form, like the real site
            hits = [k for k, v in GB.items() if any(isbn13(i["identifier"]) == isbn13(isbn) for i in v.get("industryIdentifiers", []))]
        else:
            hits = [k for k, v in GB.items() if _match(q, v["title"], v["authors"])]
        return {"items": [{"id": k, "volumeInfo": GB[k]} for k in hits]}
    if "openlibrary.org" in url: return {"docs": []}
    if "api.audible" in url:
        if "/catalog/products/" in url:
            asin = url.rsplit("/", 1)[-1]
            return {"product": next(a for a in AUDIBLE if a["asin"] == asin)}
        term = p.get("title") or p.get("author") or ""
        return {"products": [a for a in AUDIBLE if _match(term, a["title"], [x["name"] for x in a["authors"]])]}
    if "comicvine" in url:
        if "/volume/4050-" in url: return {"results": CV_VOLUME}
        if "/issues/" in url: return {"results": CV_ISSUES, "number_of_total_results": len(CV_ISSUES)}
        if "/search/" in url: return {"results": [CV_VOLUME]}
    raise RuntimeError(f"no fake for {url}")


async def fake_prowlarr(term, mtype):
    return [{"title": f"{term} (2021) EPUB", "type": mtype, "size": 3_000_000, "seeders": 5, "indexer": "FakeIdx", "protocol": "torrent",
             "downloadUrl": "magnet:?xt=urn:btih:fake", "publishDate": "2025-01-01T00:00:00Z", "infoUrl": None}]


SENT = []
async def fake_send(rel, mtype, item_id=None):
    """Stands in for handing a release to SABnzbd/qBittorrent: records a grab exactly like the real one does."""
    cid = main.rows("SELECT id FROM client") or [{"id": main.db.execute("INSERT INTO client(name,kind,url) VALUES('Fake qB','qbittorrent','http://qb.invalid')").lastrowid}]
    gid = main.db.execute("INSERT INTO grab(item_id,type,title,protocol,client_id,dl_id) VALUES(?,?,?,?,?,?)",
                          (item_id, mtype, rel["title"], rel["protocol"], cid[0]["id"], "bookarr-test")).lastrowid
    if item_id: main.db.execute("UPDATE item SET status='wanted' WHERE id=? AND status='missing'", (item_id,))
    main.db.commit(); SENT.append((rel["title"], mtype, item_id, gid)); return "Fake qB"


async def fake_live_progress():
    out = []
    for g in main.rows("SELECT * FROM grab WHERE state='sent'"):
        out.append({"grab_id": g["id"], "item_id": g["item_id"], "title": g["title"], "type": g["type"], "client": "Fake qB", "kind": "qBittorrent",
                    "progress": 40.0, "status": "Downloading", "eta": "2m 0s", "speed": "1.0 MB/s", "problem": None, "created": g["created"]})
    return out


main.jget = fake_jget
main.prowlarr = fake_prowlarr
main.send_to_client = fake_send
main.live_progress = fake_live_progress


@pytest.fixture(scope="session")
def core(): return main


@pytest.fixture(scope="session")
def client(): return TestClient(main.app, raise_server_exceptions=False)


@pytest.fixture(scope="session")
def H(): return {"X-Api-Key": KEY}


@pytest.fixture
def make_file():
    """Create a small real book file in the library and return its path."""
    def _make(rel, kind="epub"):
        p = LIB / rel; p.parent.mkdir(parents=True, exist_ok=True)
        if kind in ("epub", "cbz"):
            with zipfile.ZipFile(p, "w") as z: z.writestr("page1.jpg" if kind == "cbz" else "a.xhtml", b"x" * 100)
        else: p.write_bytes(b"x" * 100)
        return str(p)
    return _make


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(TMP, ignore_errors=True)
