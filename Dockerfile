FROM python:3.12-slim
ENV DATA_DIR=/config PUID=99 PGID=100 UMASK=002 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py entrypoint.sh ./
RUN mkdir static
COPY index.html static/index.html
VOLUME /config
RUN chmod +x entrypoint.sh
EXPOSE 8787
HEALTHCHECK --interval=60s --timeout=5s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8787/ping')"
ENTRYPOINT ["./entrypoint.sh"]
