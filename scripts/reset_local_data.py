import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from app.db import get_db_connection


TABLES_TO_CLEAR = [
    "workfloor_task_status",
    "planning_overrides",
    "planning_starturen",
    "planning_cache",
    "planning_saved",
    "planning_runs",
    "productieplanning",
    "menu_recept_selectie",
    "menu",
    "stappen",
    "handelingen",
    "recepten",
]


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name=?
        """,
        (table_name,),
    ).fetchone()

    return row is not None


def count_rows(conn, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"] or 0)


def main():
    conn = get_db_connection()

    try:
        print("Voor reset:")
        for table_name in TABLES_TO_CLEAR:
            if table_exists(conn, table_name):
                print(f"{table_name}: {count_rows(conn, table_name)}")

        print("\nData wissen...")

        conn.execute("PRAGMA foreign_keys = OFF")

        for table_name in TABLES_TO_CLEAR:
            if not table_exists(conn, table_name):
                print(f"SKIP {table_name}: tabel bestaat niet")
                continue

            conn.execute(f"DELETE FROM {table_name}")
            print(f"Leeggemaakt: {table_name}")

        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()

        print("\nNa reset:")
        for table_name in TABLES_TO_CLEAR:
            if table_exists(conn, table_name):
                print(f"{table_name}: {count_rows(conn, table_name)}")

        print("\nBehouden posten:")
        rows = conn.execute("""
            SELECT id, naam, planning_fase, capaciteit_minuten
            FROM posten
            ORDER BY naam
        """).fetchall()

        for row in rows:
            print(dict(row))

        print("\nReset klaar.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()