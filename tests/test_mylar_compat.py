"""Tests for the Mylar3-compatible API that SeerrNG uses for comics."""
import zipfile
import pytest
from fastapi.testclient import TestClient
from conftest import KEY, LIB

API = "/mylar-compat/api"


@pytest.fixture(scope="module")
def live(core):
    with TestClient(core.app, raise_server_exceptions=False) as c: yield c


def call(live, cmd, key=KEY, **kw):
    return live.get(API, params={"apikey": key, "cmd": cmd, **kw})


def test_off_by_default(live, core):
    assert core.cfg("seerrng")["mylarEnabled"] == 0
    assert call(live, "getVersion").status_code == 404
    assert live.get(API).status_code == 404


def test_api_key_required(live, core):
    core.setcfg("seerrng", {"mylarEnabled": 1})
    r = call(live, "getVersion", key="wrong")
    assert r.status_code == 401 and r.json()["success"] is False
    assert call(live, "getVersion", key="").status_code == 401
    r = call(live, "getVersion")
    assert r.status_code == 200 and r.json()["success"] is True and r.json()["data"]["current_version"]


def test_add_comic_follows_series_and_lists_issues(live, core):
    # The existing-behaviour tests may already follow ComicVine 4050; start clean for this check.
    for s in core.rows("SELECT id FROM series WHERE provider='comicvine' AND ext_id='4050'"): core.series_del(s["id"])
    r = call(live, "addComic", id="4050")
    assert r.json()["success"] is True
    idx = call(live, "getIndex").json()["data"]
    dp = next(c for c in idx if c["id"] == "4050")
    assert dp["name"] == "Deadpool" and dp["totalIssues"] == 3 and dp["publisher"] == "Marvel"
    d = call(live, "getComic", id="4050").json()["data"]
    assert d["comic"][0]["id"] == "4050" and [i["number"] for i in d["issues"]] == ["1", "2", "3"]
    assert all(i["id"].isdigit() and i["status"] in ("Wanted", "Snatched") for i in d["issues"])
    assert call(live, "addComic", id="4050").json()["success"] is True  # adding twice is fine
    assert len(core.rows("SELECT id FROM series WHERE provider='comicvine' AND ext_id='4050'")) == 1


def test_downloaded_issue_and_download(live, core):
    it = core.rows("SELECT * FROM item WHERE type='comic' AND ext_id='9001'")[0]
    p = LIB / "Comics" / "Deadpool" / "Deadpool 001.cbz"; p.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(p, "w") as z: z.writestr("01.jpg", b"img")
    core.db.execute("UPDATE item SET path=?, status='downloaded' WHERE id=?", (str(p), it["id"])); core.db.commit()
    iss = call(live, "getComic", id="4050").json()["data"]["issues"]
    assert next(i for i in iss if i["id"] == "9001")["status"] == "Downloaded"
    r = call(live, "downloadIssue", id="9001")
    from urllib.parse import unquote
    assert r.status_code == 200 and "Deadpool 001.cbz" in unquote(r.headers["content-disposition"]) and "json" not in r.headers["content-type"]
    assert r.content == p.read_bytes()
    assert call(live, "downloadIssue", id="9002").json()["success"] is False
    assert call(live, "downloadIssue", id="abc").status_code == 400


def test_unknown_and_missing(live):
    assert call(live, "getComic", id="123").json()["success"] is False
    assert call(live, "somethingElse").json()["success"] is False
    assert call(live, "addComic", id="not-a-number").json()["success"] is False


def test_del_comic_keeps_files(live, core):
    p = LIB / "Comics" / "Deadpool" / "Deadpool 001.cbz"
    assert call(live, "delComic", id="4050").json()["success"] is True
    assert p.exists(), "removing a comic never deletes downloaded files"
    assert not any(c["id"] == "4050" for c in call(live, "getIndex").json()["data"])


def test_turning_off_hides_everything(live, core):
    core.setcfg("seerrng", {"mylarEnabled": 0})
    assert call(live, "getIndex").status_code == 404
