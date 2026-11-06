# marketagg application image (scheduler, worker, API, UI, migrate share one image).
# Base pinned by digest; digest looked up from Docker Hub on 2026-10-25. Re-pin on upgrade.
FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    MARKETAGG_PG_BIN=/usr/lib/postgresql/18/bin

# PostgreSQL 18 client tools for pg_dump/pg_restore in backup jobs.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
      https://www.postgresql.org/media/keys/ACCC4CF8.asc \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update && apt-get install -y --no-install-recommends postgresql-client-18 \
 && apt-get purge -y gnupg && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv==0.13.4

RUN useradd --system --uid 10001 --home /app --shell /usr/sbin/nologin marketagg
# Mount points owned by the app user: fresh named volumes copy this ownership.
RUN mkdir -p /data /backups && chown 10001 /data /backups
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
COPY config ./config
COPY ui ./ui
COPY scripts ./scripts
RUN uv sync --frozen --no-dev --extra yahoo \
 && chown -R root:root /app && chmod -R a-w /app

USER 10001
ENV PATH=/app/.venv/bin:$PATH
ENTRYPOINT ["marketagg"]
CMD ["--help"]
