"""Safety net for what Bookarr already does.

These tests describe today's behaviour (before the SeerrNG connectors existed) and must
keep passing after any change. They were run against the code *before* the SeerrNG work
was added, and again after, to prove nothing that worked before was broken.
"""
import os


def test_ping_and_page(client):
    assert client.get("/ping").json() == {"ok": True}
    r = client.get("/")
    assert r.status_code == 200 and "Bookarr" in r.text


def test_api_key_required(client):
    assert client.get("/api/v1/item").status_code == 401
    assert client.get("/api/v1/item", headers={"X-Api-Key": "wrong"}).status_code == 401


def test_settings_roundtrip(client, H):
    r = client.put("/api/v1/config/quality", headers=H, json={"ebookMaxMB": 150})
    assert r.status_code == 200 and r.json()["ebookMaxMB"] == 150
    assert client.get("/api/v1/config/quality", headers=H).json()["ebookMaxMB"] == 150
    assert client.get("/api/v1/config/nope", headers=H).status_code == 404


def test_lookup_ebook_and_audio(client, H):
    r = client.get("/api/v1/lookup?term=Haunting Adeline&types=ebook,audiobook", headers=H).json()
    kinds = {(x["type"], x["provider"]) for x in r}
    assert ("ebook", "googlebooks") in kinds and ("audiobook", "audible") in kinds


def test_add_search_flow_creates_item_and_grab(client, H, core):
    r = client.post("/api/v1/add", headers=H, json={"type": "ebook", "title": "Mistborn", "author": "Brandon Sanderson", "provider": "googlebooks", "ext_id": "gb3", "mode": "search"}).json()
    assert r["id"] and r["release"]
    it = client.get(f"/api/v1/item/{r['id']}", headers=H).json()
    assert it["status"] == "wanted" and it["monitored"] == 1
    # adding again must not duplicate
    r2 = client.post("/api/v1/add", headers=H, json={"type": "ebook", "title": "Mistborn", "author": "Brandon Sanderson", "provider": "googlebooks", "ext_id": "gb3", "mode": "manual"}).json()
    assert r2["id"] == r["id"] and r2["existing"] is True


def test_item_list_filter_and_delete(client, H, make_file, core):
    iid = client.post("/api/v1/item", headers=H, json={"type": "ebook", "title": "Delete Me", "author": "Nobody"}).json()["id"]
    path = make_file("Ebooks/Nobody/Delete Me.epub")
    core.db.execute("UPDATE item SET path=?, status='downloaded' WHERE id=?", (path, iid)); core.db.commit()
    assert any(i["id"] == iid for i in client.get("/api/v1/item?type=ebook", headers=H).json())
    assert not any(i["id"] == iid for i in client.get("/api/v1/item?type=audiobook", headers=H).json())
    client.delete(f"/api/v1/item/{iid}?deleteFile=0", headers=H)
    assert os.path.exists(path), "removing without deleteFile must keep the file"
    iid2 = client.post("/api/v1/item", headers=H, json={"type": "ebook", "title": "Delete Me Too", "author": "Nobody"}).json()["id"]
    core.db.execute("UPDATE item SET path=?, status='downloaded' WHERE id=?", (path, iid2)); core.db.commit()
    client.delete(f"/api/v1/item/{iid2}?deleteFile=1", headers=H)
    assert not os.path.exists(path)


def test_bulk_monitor(client, H):
    ids = [client.post("/api/v1/item", headers=H, json={"type": "audiobook", "title": f"Bulk {n}"}).json()["id"] for n in range(2)]
    assert client.post("/api/v1/item/bulk", headers=H, json={"ids": ids, "action": "unmonitor"}).json()["updated"] == 2
    assert all(client.get(f"/api/v1/item/{i}", headers=H).json()["monitored"] == 0 for i in ids)


def test_history_and_clear(client, H):
    hs = client.get("/api/v1/history", headers=H).json()
    assert isinstance(hs, list)
    assert client.delete("/api/v1/history?scope=finished", headers=H).status_code == 200


def test_follow_comic_series(client, H):
    r = client.post("/api/v1/series", headers=H, json={"type": "comic", "title": "Deadpool", "provider": "comicvine", "ext_id": "4050"})
    assert r.status_code == 200 and r.json()["added"] == 3
    s = client.get("/api/v1/series?type=comic", headers=H).json()
    assert any(x["ext_id"] == "4050" and x["total"] == 3 for x in s)


def test_follow_author(client, H):
    r = client.post("/api/v1/series", headers=H, json={"type": "ebook", "title": "H. D. Carlton", "author": "H. D. Carlton", "provider": "author", "ext_id": "H. D. Carlton"})
    assert r.status_code == 200
    assert client.get("/api/v1/series?type=ebook&kind=author", headers=H).json()


def test_opds_and_kindle_pages(client):
    assert client.get("/opds").status_code == 200
    assert client.get("/kindle").status_code == 200


def test_system_pages(client, H):
    assert "disks" in client.get("/api/v1/system/info", headers=H).json()
    assert isinstance(client.get("/api/v1/health", headers=H).json(), list)
