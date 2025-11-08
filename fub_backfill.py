# === BEGIN: robust env + DB connection (keep this as-is) ===
import os
import logging
import psycopg2
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

FUB_API_KEY = os.getenv("FUB_API_KEY", "").strip()

def _pick_db_url():
    # Prefer the one you've been using.
    order = ("DATABASE_URL", "DB_CONN", "POSTGRES_URL", "POSTGRES_CONNECTION_STRING")
    for key in order:
        val = os.getenv(key)
        if val and val.strip():
            return key, val.strip()
    return None, None

def _ensure_ssl_require(dsn: str) -> str:
    try:
        u = urlparse(dsn)
        qs = dict(parse_qsl(u.query, keep_blank_values=True))
        if "sslmode" not in qs:
            qs["sslmode"] = "require"
        return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(qs), u.fragment))
    except Exception:
        return dsn

def _redact_url(dsn: str) -> str:
    try:
        u = urlparse(dsn)
        # redact password if present
        if u.username or u.password:
            netloc = u.hostname or ""
            if u.port:
                netloc = f"{netloc}:{u.port}"
            u = u._replace(netloc=netloc)
        # show scheme://host:port/dbname
        return urlunparse((u.scheme, u.netloc, u.path, "", "", ""))
    except Exception:
        return "<unparseable>"

def get_db_connection():
    key, db_url = _pick_db_url()
    if not db_url:
        logging.critical(
            "No database URL found. Set DATABASE_URL to your Postgres connection string "
            "(or DB_CONN / POSTGRES_URL / POSTGRES_CONNECTION_STRING)."
        )
        raise SystemExit(1)

    if not (db_url.startswith("postgres://") or db_url.startswith("postgresql://")):
        logging.critical(
            f"{key} does not look like a Postgres URL. Value starts with: {db_url[:20]}..."
        )
        raise SystemExit(1)

    db_url = _ensure_ssl_require(db_url)
    logging.info(f"Using database URL from {key}: {_redact_url(db_url)}")

    return psycopg2.connect(db_url)
# === END: robust env + DB connection ===
