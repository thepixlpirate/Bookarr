"""Tests for the Readarr-compatible API that SeerrNG uses for ebooks and audiobooks."""
import time, zipfile, pathlib
import pytest
from fastapi.testclient import TestClient
from conftest import KEY, LIB, SENT

V1 = "/readarr-compat/api/v1"
HK = {"X-Api-Key": KEY}


@pytest.fixture(scope="module")
def live(core):
    # A client that keeps one event loop running, so background searches can finish (like the real server).
    with TestClient(core.app, raise_server_exceptions=False) as c: yield c


def setting(core, **kw): core.setcfg("seerrng", kw)


def wait_command(live, cid, t="ebook"):
    for _ in range(50):
        c = live.get(f"{V1}/command/{cid}?mediaType={t}", headers=HK).json()
        if c["status"] in ("completed", "failed"): return c
        time.sleep(0.05)
    raise AssertionError("command did not finish")


def test_off_by_default_everything_404(live, core):
    assert core.cfg("seerrng")["readarrEnabled"] == 0
    for path in (f"{V1}/system/status", f"{V1}/book", f"{V1}/book/lookup?term=x", "/readarr-compat/MediaCover/1/cover.jpg",
                 "/readarr-compat/audiobook/api/v1/system/status"):
        assert live.get(path, headers=HK).status_code == 404, path
        assert live.get(path).status_code == 404, path  # even without a key it just looks like nothing is there
    assert live.post(f"{V1}/book", headers=HK, json={"title": "x"}).status_code == 404


def test_api_key_required_when_on(live, core):
    setting(core, readarrEnabled=1)
    assert live.get(f"{V1}/system/status").status_code == 401
    assert live.get(f"{V1}/system/status", headers={"X-Api-Key": "wrong"}).status_code == 401
    assert live.get(f"{V1}/system/status?apikey={KEY}").status_code == 401  # must be the header
    assert live.get(f"{V1}/system/status", headers=HK).status_code == 200


def test_setup_screen_calls(live):
    s = live.get(f"{V1}/system/status?mediaType=ebook", headers=HK).json()
    assert s["appName"] == "Bookarr" and s["version"] and s["urlBase"] == "/readarr-compat"
    assert live.get(f"{V1}/system/capabilities", headers=HK).status_code == 404
    assert live.get(f"{V1}/config/hardcover", headers=HK).status_code == 404
    assert live.get(f"{V1}/rootfolder?mediaType=ebook", headers=HK).json()[0]["path"].endswith("Ebooks")
    assert live.get(f"{V1}/rootfolder?mediaType=audiobook", headers=HK).json()[0]["path"].endswith("Audiobooks")
    assert live.get("/readarr-compat/audiobook/api/v1/rootfolder", headers=HK).json()[0]["path"].endswith("Audiobooks")
    assert live.get(f"{V1}/qualityProfile", headers=HK).json()[0]["id"] == 1
    assert live.get(f"{V1}/metadataProfile", headers=HK).json()[0]["id"] == 1
    assert live.get(f"{V1}/tag", headers=HK).json() == []
    t = live.post(f"{V1}/tag", headers=HK, json={"label": "1-alex"}).json()
    assert t == {"id": 1, "label": "1-alex"} and live.get(f"{V1}/tag", headers=HK).json() == [t]
    assert live.post(f"{V1}/tag", headers=HK, json={"label": "1-alex"}).json()["id"] == 1  # no duplicate tags


def addable(r):  # the same rule SeerrNG uses (server/utils/bookshelfLookup.ts)
    return r["foreignBookId"] and r["title"] and r["author"]["foreignAuthorId"] and any(e["foreignEditionId"] for e in r["editions"])


def test_lookup_by_title_and_isbn(live):
    r = live.get(f"{V1}/book/lookup?term=Haunting Adeline&mediaType=ebook", headers=HK).json()
    assert r and all(addable(x) for x in r) and r[0]["foreignBookId"] == "gb:gb1"
    assert r[0]["images"][0]["coverType"] == "cover" and r[0]["images"][0]["remoteUrl"]
    r = live.get(f"{V1}/book/lookup?term=isbn:9781957635002&mediaType=ebook", headers=HK).json()
    assert r[0]["editions"][0]["isbn13"] == "9781957635002"
    r = live.get(f"{V1}/book/lookup?term=0765311780&mediaType=ebook", headers=HK).json()  # ISBN-10 -> ISBN-13
    assert r[0]["title"] == "Mistborn" and r[0]["editions"][0]["isbn13"] == "9780765311788"
    r = live.get(f"{V1}/book/lookup?term=gb:gb2&mediaType=ebook", headers=HK).json()  # our own ids resolve again
    assert len(r) == 1 and r[0]["title"] == "Hunting Adeline"


def test_audiobook_lookup(live):
    r = live.get(f"{V1}/book/lookup?term=Haunting Adeline&mediaType=audiobook", headers=HK).json()
    assert r[0]["foreignBookId"] == "au:B0AUDIO01" and r[0]["mediaType"] == "audiobook" and addable(r[0])
    r = live.get(f"{V1}/book/lookup?term=isbn:9781957635002&mediaType=audiobook", headers=HK).json()
    assert r[0]["foreignBookId"] == "au:B0AUDIO01" and r[0]["editions"][0]["isbn13"] == "9781957635002"


def test_author_lookup_and_list(live):
    r = live.get(f"{V1}/author/lookup?term=H. D. Carlton", headers=HK).json()
    assert r[0]["authorName"] == "H. D. Carlton" and r[0]["foreignAuthorId"]


def test_add_book_searches_and_never_duplicates(live, core):
    lk = live.get(f"{V1}/book/lookup?term=Haunting Adeline&mediaType=ebook", headers=HK).json()[0]
    body = {**lk, "monitored": True, "qualityProfileId": 1, "metadataProfileId": 1, "rootFolderPath": "/books/Ebooks", "tags": [],
            "addOptions": {"searchForNewBook": True}}
    # An earlier test followed this author, which already listed the book (unmonitored). Start from a clean slate here.
    for x in core.rows("SELECT id FROM item WHERE type='ebook' AND title='Haunting Adeline'"): core.remove_item(x["id"], False)
    before = len(SENT)
    r = live.post(f"{V1}/book?mediaType=ebook", headers=HK, json=body)
    assert r.status_code == 201
    b = r.json(); assert b["id"] > 0 and b["foreignBookId"] == "gb:gb1" and b["monitored"] is True
    for _ in range(50):
        if len(SENT) > before: break
        time.sleep(0.05)
    assert SENT[-1][2] == b["id"] and SENT[-1][1] == "ebook", "Bookarr's normal search/download ran"
    it = core.rows("SELECT * FROM item WHERE id=?", (b["id"],))[0]
    assert it["type"] == "ebook" and it["status"] == "wanted"
    again = live.post(f"{V1}/book?mediaType=ebook", headers=HK, json=body)
    assert again.status_code == 200 and again.json()["id"] == b["id"]
    assert len(core.rows("SELECT id FROM item WHERE type='ebook' AND title='Haunting Adeline'")) == 1


def test_existing_unmonitored_book_gets_monitored_not_duplicated(live, core):
    have = core.rows("SELECT id FROM item WHERE type='ebook' AND provider='googlebooks' AND ext_id='gb2'")
    iid = have[0]["id"] if have else core.insert_item({"type": "ebook", "title": "Hunting Adeline", "author": "H. D. Carlton", "provider": "googlebooks", "ext_id": "gb2", "status": "missing"})
    core.db.execute("UPDATE item SET monitored=0 WHERE id=?", (iid,)); core.db.commit()
    lk = live.get(f"{V1}/book/lookup?term=gb:gb2&mediaType=ebook", headers=HK).json()[0]
    assert lk.get("id") == iid  # lookup tells SeerrNG it is already in Bookarr
    r = live.post(f"{V1}/book?mediaType=ebook", headers=HK, json={**lk, "monitored": True, "addOptions": {"searchForNewBook": False}})
    assert r.status_code == 200 and r.json()["id"] == iid and r.json()["monitored"] is True
    assert core.rows("SELECT monitored FROM item WHERE id=?", (iid,))[0]["monitored"] == 1


def test_ebook_and_audiobook_stay_separate(live, core):
    lk = live.get(f"{V1}/book/lookup?term=Haunting Adeline&mediaType=audiobook", headers=HK).json()[0]
    a = live.post(f"{V1}/book?mediaType=audiobook", headers=HK, json={**lk, "monitored": True, "addOptions": {"searchForNewBook": False}}).json()
    e = next(x for x in live.get(f"{V1}/book?mediaType=ebook", headers=HK).json() if x["title"] == "Haunting Adeline")
    assert a["id"] != e["id"]
    assert core.rows("SELECT type FROM item WHERE id=?", (a["id"],))[0]["type"] == "audiobook"
    assert a["id"] not in [x["id"] for x in live.get(f"{V1}/book?mediaType=ebook", headers=HK).json()]
    assert a["id"] in [x["id"] for x in live.get("/readarr-compat/audiobook/api/v1/book", headers=HK).json()]
    assert live.get(f"{V1}/book/{a['id']}?mediaType=ebook", headers=HK).status_code == 404
    assert not any(x["type"] == "comic" for x in core.rows("SELECT type FROM item WHERE id IN (?,?)", (a["id"], e["id"])))


def test_comics_are_never_listed(live, core):
    cid = core.insert_item({"type": "comic", "title": "Some Comic #1", "status": "missing"})
    for t in ("ebook", "audiobook"):
        assert cid not in [x["id"] for x in live.get(f"{V1}/book?mediaType={t}", headers=HK).json()]


def _book(live, title, t="ebook"):
    return next(x for x in live.get(f"{V1}/book?mediaType={t}", headers=HK).json() if x["title"] == title)


def test_availability_files_and_cover(live, core):
    b = _book(live, "Haunting Adeline")
    assert b["statistics"]["bookFileCount"] == 0
    p = LIB / "Ebooks" / "H. D. Carlton" / "Haunting Adeline.epub"; p.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(p, "w") as z: z.writestr("OEBPS/cover.jpg", b"\xff\xd8\xff" + b"0" * 50); z.writestr("a.xhtml", "x")
    core.db.execute("UPDATE item SET path=?, status='downloaded', cover=NULL WHERE id=?", (str(p), b["id"])); core.db.commit()
    b = live.get(f"{V1}/book/{b['id']}?mediaType=ebook", headers=HK).json()
    assert b["statistics"]["bookFileCount"] == 1 and b["statistics"]["ebookFileCount"] == 1 and b["monitored"] is True
    files = live.get(f"{V1}/bookfile?bookId={b['id']}&mediaType=ebook", headers=HK).json()
    assert files[0]["bookId"] == b["id"] and files[0]["path"] == str(p) and files[0]["size"] > 0 and files[0]["id"] > 0
    assert live.get(f"{V1}/edition?bookId={b['id']}&mediaType=ebook", headers=HK).json()[0]["foreignEditionId"]
    cov = live.get(f"/readarr-compat/MediaCover/{b['id']}/cover.jpg", headers=HK)
    assert cov.status_code == 200 and cov.content[:3] == b"\xff\xd8\xff"
    assert live.get("/readarr-compat/MediaCover/999999/cover.jpg", headers=HK).status_code == 404


def test_paged_queue_history(live, core):
    pg = live.get(f"{V1}/book/paged?offset=0&pageSize=1&includeUnmonitored=true&mediaType=ebook", headers=HK).json()
    assert pg["offset"] == 0 and pg["totalCount"] >= 2 and len(pg["records"]) == 1
    pg2 = live.get(f"{V1}/book/paged?page=2&pageSize=1&mediaType=ebook", headers=HK).json()
    assert pg2["records"][0]["id"] != pg["records"][0]["id"]
    q = live.get(f"{V1}/queue?mediaType=ebook", headers=HK).json()
    assert q["totalRecords"] == len(q["records"]) and all(r["id"] > 0 and "bookId" in r for r in q["records"])
    assert all(0 <= r["sizeleft"] <= r["size"] for r in q["records"])
    b = _book(live, "Haunting Adeline")
    h = live.get(f"{V1}/history?bookId={b['id']}&mediaType=ebook", headers=HK).json()
    assert h["records"] and h["records"][-1]["eventType"] == "grabbed" and h["records"][0]["bookId"] == b["id"]


def test_put_monitor_and_command(live, core):
    b = _book(live, "Haunting Adeline", "audiobook")
    r = live.put(f"{V1}/book/{b['id']}?mediaType=audiobook", headers=HK, json={**b, "monitored": False})
    assert r.status_code == 202 and r.json()["monitored"] is False
    live.put(f"{V1}/book/{b['id']}?mediaType=audiobook", headers=HK, json={**b, "monitored": True})
    before = len(SENT)
    c = live.post(f"{V1}/command?mediaType=audiobook", headers=HK, json={"name": "BookSearch", "bookIds": [b["id"]]}).json()
    assert c["name"] == "BookSearch" and c["status"] in ("queued", "started", "completed") and c["id"] > 0
    assert wait_command(live, c["id"], "audiobook")["status"] == "completed"
    assert len(SENT) == before + 1 and SENT[-1][1] == "audiobook"
    assert live.get(f"{V1}/command/987654", headers=HK).status_code == 404
    other = live.post(f"{V1}/command", headers=HK, json={"name": "RefreshMonitoredDownloads"}).json()
    assert wait_command(live, other["id"])["status"] in ("completed", "failed")


def test_delete_keeps_files_unless_both_allowed(live, core):
    def make(title):
        p = LIB / "Ebooks" / "Del" / f"{title}.epub"; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(b"x")
        iid = core.insert_item({"type": "ebook", "title": title, "author": "Del Author", "status": "downloaded"})
        core.db.execute("UPDATE item SET path=? WHERE id=?", (str(p), iid)); core.db.commit(); return iid, p
    i1, p1 = make("Keep One")
    live.delete(f"{V1}/book/{i1}?deleteFiles=false&mediaType=ebook", headers=HK)
    assert p1.exists() and not core.rows("SELECT 1 FROM item WHERE id=?", (i1,))
    i2, p2 = make("Keep Two")  # SeerrNG asks to delete files, but the safety switch is off
    live.delete(f"{V1}/book/{i2}?deleteFiles=true&mediaType=ebook", headers=HK)
    assert p2.exists()
    setting(core, allowDeleteFiles=1)
    i3, p3 = make("Gone Three")
    live.delete(f"{V1}/book/{i3}?deleteFiles=true&mediaType=ebook", headers=HK)
    assert not p3.exists()
    i4, p4 = make("Keep Four")  # switch on, but SeerrNG did not ask
    live.delete(f"{V1}/book/{i4}?deleteFiles=false&mediaType=ebook", headers=HK)
    assert p4.exists()
    setting(core, allowDeleteFiles=0)
    i5, p5 = make("Author Book")
    aid = next(a["id"] for a in live.get(f"{V1}/author?mediaType=ebook", headers=HK).json() if a["authorName"] == "Del Author")
    assert live.get(f"{V1}/author/{aid}?mediaType=ebook", headers=HK).json()["authorName"] == "Del Author"
    live.delete(f"{V1}/author/{aid}?deleteFiles=true&mediaType=ebook", headers=HK)
    assert p5.exists() and not core.rows("SELECT 1 FROM item WHERE id=?", (i5,))


def test_basic_login_does_not_block_seerrng(live, core):
    core.setcfg("security", {"authMode": "basic", "username": "u", "password": "p"})
    try:
        assert live.get("/").status_code == 401  # Bookarr's own page still asks for the login
        assert live.get(f"{V1}/system/status", headers=HK).status_code == 200  # SeerrNG gets in with the API key
        assert live.get(f"{V1}/system/status").status_code == 401
    finally:
        core.setcfg("security", {"authMode": "none"})


def test_turning_off_again_hides_everything(live, core):
    setting(core, readarrEnabled=0)
    assert live.get(f"{V1}/book", headers=HK).status_code == 404
    assert live.get(f"{V1}/system/status", headers=HK).status_code == 404
