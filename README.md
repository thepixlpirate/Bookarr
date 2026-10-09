# Bookarr

Comics, ebooks and audiobooks in one arr-style app: search, monitor series, download via Prowlarr + SABnzbd/qBittorrent, import, tag, and serve over OPDS.

## Install on Unraid (private repo)
1. Create a GitHub repo named `bookarr` and push this folder to `main`. The Actions workflow builds `ghcr.io/<you>/bookarr:latest`.
2. Private image: on the Unraid terminal run `docker login ghcr.io -u <you>` with a personal access token that has `read:packages`. (Or make the package public in GitHub > Packages.)
3. Edit `bookarr.xml` and replace `YOUR_GITHUB_USER`. Copy it to `/boot/config/plugins/dockerMan/templates-user/bookarr.xml`.
4. Docker tab > Add Container > pick the `bookarr` template > Apply. Open port 8787.
5. Update later: push to GitHub, wait for the build, then "Force update" the container.

## First-run checklist (Settings)
- Indexers: Prowlarr URL and API key. Download clients: SABnzbd and/or qBittorrent.
- Metadata: ComicVine API key (free) for comic series monitoring.
- Media: if SABnzbd sees `/data/...` but Bookarr sees `/downloads/...`, fill in the two path mapping fields.
- System page shows anything still misconfigured.

## Readers
OPDS catalog: `http://<server>:8787/opds`. Create one key per app in Settings > Reader devices. Comics support page streaming (OPDS-PSE).

## Backup / restore
Backups are zipped weekly into `/config/backups`. To restore, stop the container and replace `/config/bookarr.db` with the file from the zip.
