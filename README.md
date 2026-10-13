# Bookarr

Comics, ebooks and audiobooks in one arr-style app: search, monitor authors and series, download via Prowlarr + SABnzbd/qBittorrent, import, tag, convert, send to Kindle and serve over OPDS.

## Install on Unraid (private repo)
1. Create a GitHub repo named `bookarr` and push this folder to `main`. The Actions workflow builds `ghcr.io/<you>/bookarr:latest`.
2. Private image: on the Unraid terminal run `docker login ghcr.io -u <you>` with a personal access token that has `read:packages`. (Or make the package public in GitHub > Packages.)
3. Edit `bookarr.xml` and replace `YOUR_GITHUB_USER`. Copy it to `/boot/config/plugins/dockerMan/templates-user/bookarr.xml`.
4. Docker tab > Add Container > pick the `bookarr` template > Apply. Open port 8787.
5. Update later: push to GitHub, wait for the build, then "Force update" the container.

## First-run checklist (Settings)
- Indexers: Prowlarr URL and API key.
- Download clients: SABnzbd and/or qBittorrent. Use the local address with the port (`http://192.168.0.185:8080`), not a Cloudflare address. Then tap **Set up folders** on each client.
- Metadata: ComicVine API key (free) for comic series monitoring.
- Converter (optional): your Transmute server and an API key from Transmute's Settings > API keys. Needed to convert formats and to make Kindle copies of EPUB-only books.
- Kindle: your `@kindle.com` address and an SMTP account for Send to Kindle.
- System page shows anything still misconfigured.

## Download folders
With Settings > Media > "Download subfolder for books" = `Books` (the default), **Set up folders** makes each client save into:

```
downloads/
  Books/
    Comics/
    Ebooks/
    Audiobooks/
```

- SABnzbd: categories `comics`, `ebooks`, `audiobooks` with folders `Books/Comics` etc. These sit inside SABnzbd's "Completed Download Folder".
- qBittorrent: the same categories with save paths `<default save path>/Books/Comics` etc. Bookarr adds torrents with Automatic Torrent Management, so the category folder is used.

When a download finishes, Bookarr moves (usenet) or links/copies (torrents, so they keep seeding) it into your Books share: `Books/Comics/<Series>`, `Books/Ebooks/<Author>`, `Books/Audiobooks/<Author>/<Title>`.

## Adding things
- **Search releases**: adds the title, keeps it monitored, downloads the best match now (or keeps looking).
- **Search + monitor author**: the same, and follows the author. Their other books are listed under Ebooks > Authors (or Audiobooks > Authors & series). By default only new releases download; switch an author to "Every book" to get the back catalogue, or tick individual books.
- **Monitor series** (comics, with ComicVine): every issue of the run, plus new issues.
- **Pick a release myself**: adds and monitors it, then shows the release list.

## Kindle
- **Send to Kindle** (email): EPUB or PDF is sent; MOBI/AZW3-only books are converted to EPUB first; comics become PDF. 50 MB limit (Amazon's).
- **Kindle web browser**: Settings > Reader devices > create a key, then open `http://<server>:8787/kindle?apikey=<key>` on the Kindle. Tap a title to download an AZW3 (or MOBI) copy.
- **Download for Kindle** on any book's page gives you the Kindle file for USB.

## Readers
OPDS catalog: `http://<server>:8787/opds`. Create one key per app in Settings > Reader devices. Comics support page streaming (OPDS-PSE).

## Backup / restore
Backups are zipped weekly into `/config/backups`. To restore, stop the container and replace `/config/bookarr.db` with the file from the zip.
