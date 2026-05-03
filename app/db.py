import os
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor

from db.connection import init_connection
from db.migrations import run_migrations
from app.services.db_init import init_db


def is_postgres():
    return "DATABASE_URL" in os.environ


def get_db_connection():
    if is_postgres():
        return get_postgres_connection()
    else:
        return get_sqlite_connection()


# --- SQLITE (lokaal) ---
def get_sqlite_connection() -> sqlite3.Connection:
    conn = init_connection()
    conn.row_factory = sqlite3.Row

    init_db(conn)
    run_migrations(conn)

    return conn


# --- POSTGRES (Render) ---
def get_postgres_connection():
    db_url = os.environ.get("DATABASE_URL")

    conn = psycopg2.connect(
        db_url,
        cursor_factory=RealDictCursor
    )

    # ⚠️ init/migrations voorlopig overslaan voor Postgres
    # (die fixen we in volgende stap)

    return conn