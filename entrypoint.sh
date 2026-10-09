#!/bin/sh
# Run as PUID:PGID (Unraid default 99:100) so imported files are owned by "nobody:users", not root
umask "${UMASK:-002}"
mkdir -p /config && chown -R "${PUID:-99}:${PGID:-100}" /config 2>/dev/null
exec setpriv --reuid="${PUID:-99}" --regid="${PGID:-100}" --clear-groups uvicorn main:app --host 0.0.0.0 --port 8787
