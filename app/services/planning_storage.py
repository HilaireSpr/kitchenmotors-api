import pandas as pd
from datetime import datetime


def init_planning_storage(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS planning_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT NOT NULL,
            beschrijving TEXT DEFAULT '',
            aangemaakt_op TEXT DEFAULT CURRENT_TIMESTAMP,
            laatst_gebruikt_op TEXT DEFAULT CURRENT_TIMESTAMP,
            actief INTEGER DEFAULT 0
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS planning_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planning_run_id INTEGER NOT NULL,
            data TEXT,
            last_updated TEXT,
            FOREIGN KEY (planning_run_id) REFERENCES planning_runs(id)
        )
        """
    )

    existing_cols = conn.execute("PRAGMA table_info(planning_cache)").fetchall()
    col_names = {row["name"] for row in existing_cols}

    if "planning_run_id" not in col_names:
        conn.execute(
            "ALTER TABLE planning_cache ADD COLUMN planning_run_id INTEGER DEFAULT 1"
        )

    conn.commit()


def _ensure_default_planning_run(conn):
    init_planning_storage(conn)

    row = conn.execute(
        """
        SELECT id
        FROM planning_runs
        WHERE actief = 1
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()

    if row:
        return int(row["id"])

    conn.execute("UPDATE planning_runs SET actief = 0")

    cur = conn.execute(
        """
        INSERT INTO planning_runs (naam, beschrijving, actief, aangemaakt_op, laatst_gebruikt_op)
        VALUES (?, ?, 1, ?, ?)
        """,
        (
            "Standaard planning",
            "",
            datetime.now().isoformat(),
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def create_planning_run(conn, naam: str, beschrijving: str = "") -> int:
    init_planning_storage(conn)

    conn.execute("UPDATE planning_runs SET actief = 0")

    cur = conn.execute(
        """
        INSERT INTO planning_runs (naam, beschrijving, actief, aangemaakt_op, laatst_gebruikt_op)
        VALUES (?, ?, 1, ?, ?)
        """,
        (
            str(naam).strip(),
            str(beschrijving).strip(),
            datetime.now().isoformat(),
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_planning_runs(conn):
    init_planning_storage(conn)
    _ensure_default_planning_run(conn)

    return conn.execute(
        """
        SELECT id, naam, beschrijving, aangemaakt_op, laatst_gebruikt_op, actief
        FROM planning_runs
        ORDER BY actief DESC, naam ASC, id DESC
        """
    ).fetchall()


def get_active_planning_run(conn):
    init_planning_storage(conn)
    active_id = _ensure_default_planning_run(conn)

    row = conn.execute(
        """
        SELECT id, naam, beschrijving, aangemaakt_op, laatst_gebruikt_op, actief
        FROM planning_runs
        WHERE id = ?
        """,
        (active_id,),
    ).fetchone()

    return row


def set_active_planning_run(conn, planning_run_id: int):
    init_planning_storage(conn)

    conn.execute("UPDATE planning_runs SET actief = 0")
    conn.execute(
        """
        UPDATE planning_runs
        SET actief = 1,
            laatst_gebruikt_op = ?
        WHERE id = ?
        """,
        (datetime.now().isoformat(), int(planning_run_id)),
    )
    conn.commit()


def delete_planning_run(conn, planning_run_id: int):
    init_planning_storage(conn)

    active = get_active_planning_run(conn)
    was_active = active and int(active["id"]) == int(planning_run_id)

    conn.execute(
        "DELETE FROM planning_cache WHERE planning_run_id = ?",
        (int(planning_run_id),),
    )
    conn.execute(
        "DELETE FROM planning_runs WHERE id = ?",
        (int(planning_run_id),),
    )
    conn.commit()

    rows = conn.execute("SELECT id FROM planning_runs ORDER BY id ASC").fetchall()

    if not rows:
        _ensure_default_planning_run(conn)
    elif was_active:
        set_active_planning_run(conn, int(rows[0]["id"]))


def duplicate_planning_run(conn, source_run_id: int, nieuwe_naam: str) -> int:
    init_planning_storage(conn)

    source_run = conn.execute(
        """
        SELECT beschrijving
        FROM planning_runs
        WHERE id = ?
        """,
        (int(source_run_id),),
    ).fetchone()

    new_run_id = create_planning_run(
        conn,
        naam=nieuwe_naam,
        beschrijving=(source_run["beschrijving"] if source_run else ""),
    )

    source_cache = conn.execute(
        """
        SELECT data, last_updated
        FROM planning_cache
        WHERE planning_run_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(source_run_id),),
    ).fetchone()

    if source_cache and source_cache["data"]:
        conn.execute(
            """
            INSERT INTO planning_cache (planning_run_id, data, last_updated)
            VALUES (?, ?, ?)
            """,
            (
                int(new_run_id),
                source_cache["data"],
                datetime.now().isoformat(),
            ),
        )
        conn.commit()

    return int(new_run_id)


def save_planning_df(conn, df: pd.DataFrame, planning_run_id: int | None = None):
    init_planning_storage(conn)

    if planning_run_id is None:
        planning_run_id = _ensure_default_planning_run(conn)

    if df is None or df.empty:
        conn.execute(
            "DELETE FROM planning_cache WHERE planning_run_id = ?",
            (int(planning_run_id),),
        )
        conn.commit()
        return

    df_copy = df.copy()

    if "Start" in df_copy.columns:
        df_copy["Start"] = pd.to_datetime(df_copy["Start"], errors="coerce").astype(str)
    if "Einde" in df_copy.columns:
        df_copy["Einde"] = pd.to_datetime(df_copy["Einde"], errors="coerce").astype(str)

    json_data = df_copy.to_json(orient="records")

    conn.execute(
        "DELETE FROM planning_cache WHERE planning_run_id = ?",
        (int(planning_run_id),),
    )

    conn.execute(
        """
        INSERT INTO planning_cache (planning_run_id, data, last_updated)
        VALUES (?, ?, ?)
        """,
        (int(planning_run_id), json_data, datetime.now().isoformat()),
    )

    conn.commit()


def load_planning_df(conn, planning_run_id: int | None = None) -> pd.DataFrame:
    init_planning_storage(conn)

    if planning_run_id is None:
        planning_run_id = _ensure_default_planning_run(conn)

    row = conn.execute(
        """
        SELECT data
        FROM planning_cache
        WHERE planning_run_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(planning_run_id),),
    ).fetchone()

    if not row or not row["data"]:
        return pd.DataFrame()

    df = pd.read_json(row["data"])

    if "Start" in df.columns:
        df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    if "Einde" in df.columns:
        df["Einde"] = pd.to_datetime(df["Einde"], errors="coerce")

    return df


def clear_planning_df(conn, planning_run_id: int | None = None):
    init_planning_storage(conn)

    if planning_run_id is None:
        planning_run_id = _ensure_default_planning_run(conn)

    conn.execute(
        "DELETE FROM planning_cache WHERE planning_run_id = ?",
        (int(planning_run_id),),
    )
    conn.commit()


def get_planning_last_updated(conn, planning_run_id: int | None = None):
    init_planning_storage(conn)

    if planning_run_id is None:
        planning_run_id = _ensure_default_planning_run(conn)

    row = conn.execute(
        """
        SELECT last_updated
        FROM planning_cache
        WHERE planning_run_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(planning_run_id),),
    ).fetchone()

    if not row:
        return None

    return row["last_updated"]