import os
import sqlite3

def init_connection():
    db_path = os.environ.get("DB_PATH", "kitchenmotor.db")
    print(f"[DB] Using SQLite file: {db_path}")

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn