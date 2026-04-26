import sqlite3

from db.connection import init_connection
from db.migrations import run_migrations
from app.services.db_init import init_db


def get_db_connection() -> sqlite3.Connection:
    conn = init_connection()
    conn.row_factory = sqlite3.Row

    # Zelfde setup als Streamlit
    init_db(conn)
    run_migrations(conn)

    return conn