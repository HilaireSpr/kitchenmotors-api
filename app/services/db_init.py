from __future__ import annotations

import sqlite3
from typing import Iterable


# =========================================================
# LOW-LEVEL HELPERS
# =========================================================
def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None



def _get_column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    names: set[str] = set()

    for row in rows:
        if isinstance(row, sqlite3.Row):
            names.add(row["name"])
        else:
            names.add(row[1])

    return names



def ensure_table(conn: sqlite3.Connection, create_sql: str) -> None:
    conn.execute(create_sql)



def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_def: str,
) -> None:
    col_names = _get_column_names(conn, table_name)

    # extra veiligheid: case-insensitive check
    if column_name.lower() in {c.lower() for c in col_names}:
        return

    try:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
    except sqlite3.OperationalError:
        # fallback: kolom bestond al maar werd niet gedetecteerd
        pass


def ensure_index(conn: sqlite3.Connection, index_name: str, create_sql: str) -> None:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'index' AND name = ?
        """,
        (index_name,),
    ).fetchone()
    if row is None:
        conn.execute(create_sql)



def _safe_execute(conn: sqlite3.Connection, sql: str, params: Iterable | tuple = ()) -> None:
    try:
        conn.execute(sql, tuple(params))
    except sqlite3.OperationalError:
        pass


# =========================================================
# TABLE CREATION
# =========================================================
def create_core_tables(conn: sqlite3.Connection) -> None:
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS recepten (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT,
            categorie TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS menu_periodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT NOT NULL,
            menu_groep TEXT NOT NULL,
            startdatum TEXT NOT NULL,
            einddatum TEXT NOT NULL,
            rotatielengte_weken INTEGER NOT NULL DEFAULT 1,
            startweek_in_cyclus INTEGER NOT NULL DEFAULT 1,
            default_prognose_aantal REAL,
            actief INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS menu (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recept_id INTEGER,
            serveerdag TEXT,
            aantal REAL,
            naam TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS handelingen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recept_id INTEGER,
            taak TEXT,
            post TEXT,
            toestel TEXT,
            stap_volgorde INTEGER,
            dag_offset INTEGER DEFAULT 0,
            min_offset_dagen INTEGER,
            max_offset_dagen INTEGER,
            actieve_tijd INTEGER DEFAULT 0,
            passieve_tijd INTEGER DEFAULT 0,
            totale_duur INTEGER DEFAULT 0,
            aantal REAL,
            opmerkingen TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS posten (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT,
            kleur TEXT,
            capaciteit_minuten INTEGER DEFAULT 450,
            startuur TEXT,
            einduur TEXT,
            actief INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS planning_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT NOT NULL,
            actief INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS planning_saved (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planning_run_id INTEGER,
            planning_id TEXT,
            werkdag_iso TEXT,
            recept TEXT,
            taak TEXT,
            post TEXT,
            toestel TEXT,
            start TEXT,
            einde TEXT,
            actieve_tijd INTEGER DEFAULT 0,
            passieve_tijd INTEGER DEFAULT 0,
            totale_duur INTEGER DEFAULT 0,
            locked INTEGER DEFAULT 0,
            manueel_aangepast INTEGER DEFAULT 0,
            start_offset_minuten INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS planning_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planning_id TEXT NOT NULL,
            planning_run_id INTEGER,
            start_offset_minutes INTEGER DEFAULT 0,
            post_override TEXT,
            toestel_override TEXT,
            locked INTEGER DEFAULT 0,
            move_before_planning_id TEXT,
            move_after_planning_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS planning_starturen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            werkdag TEXT NOT NULL,
            post TEXT NOT NULL,
            starttijd TEXT NOT NULL DEFAULT '08:00',
            UNIQUE (werkdag, post)
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS toestellen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT NOT NULL UNIQUE
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS posten (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            naam TEXT NOT NULL UNIQUE,
            kleur TEXT DEFAULT '',
            capaciteit_minuten INTEGER DEFAULT 480
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS recepten (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT DEFAULT '',
            naam TEXT NOT NULL,
            categorie TEXT DEFAULT '',
            menu_groep TEXT DEFAULT ''
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS handelingen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recept_id INTEGER NOT NULL,
            code TEXT DEFAULT '',
            naam TEXT NOT NULL,
            dag_offset INTEGER DEFAULT 0,
            min_offset_dagen INTEGER,
            max_offset_dagen INTEGER,
            sort_order INTEGER DEFAULT 0,
            post TEXT DEFAULT '-',
            toestel TEXT DEFAULT 'Geen',
            passieve_tijd INTEGER DEFAULT 0,
            heeft_vast_startuur INTEGER DEFAULT 0,
            vast_startuur TEXT DEFAULT '',
            is_vaste_taak INTEGER DEFAULT 0,
            planning_type TEXT DEFAULT 'floating',
            actief_vanaf TEXT,
            actief_tot TEXT,
            FOREIGN KEY (recept_id) REFERENCES recepten(id)
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS stappen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            handeling_id INTEGER NOT NULL,
            naam TEXT NOT NULL,
            tijd INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            FOREIGN KEY (handeling_id) REFERENCES handelingen(id)
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS menu (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recept_id INTEGER NOT NULL,
            cyclus_week INTEGER,
            cyclus_dag INTEGER,
            serveerdag TEXT NOT NULL,
            FOREIGN KEY (recept_id) REFERENCES recepten(id)
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS planning_starturen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            werkdag TEXT NOT NULL,
            post TEXT NOT NULL,
            starttijd TEXT NOT NULL DEFAULT '08:00',
            UNIQUE (werkdag, post)
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS planning_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recept_id INTEGER NOT NULL,
            week INTEGER NOT NULL,
            dag INTEGER NOT NULL,
            FOREIGN KEY (recept_id) REFERENCES recepten(id)
        )
        """,
    )

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS menu_recept_selectie (
            recept_id INTEGER PRIMARY KEY,
            actief INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (recept_id) REFERENCES recepten(id)
        )
        """,
    )

# =========================================================
# COLUMN MIGRATIONS
# NOTE: bij ALTER TABLE gebruiken we GEEN CURRENT_TIMESTAMP defaults,
# want SQLite laat geen non-constant default toe bij ADD COLUMN.
# =========================================================
def migrate_recepten_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "recepten"):
        return

    ensure_column(conn, "recepten", "naam", "TEXT")
    ensure_column(conn, "recepten", "categorie", "TEXT")
    ensure_column(conn, "recepten", "created_at", "TEXT")

def migrate_menu_periodes_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "menu_periodes"):
        return

    ensure_column(conn, "menu_periodes", "naam", "TEXT")
    ensure_column(conn, "menu_periodes", "menu_groep", "TEXT")
    ensure_column(conn, "menu_periodes", "startdatum", "TEXT")
    ensure_column(conn, "menu_periodes", "einddatum", "TEXT")
    ensure_column(conn, "menu_periodes", "rotatielengte_weken", "INTEGER DEFAULT 1")
    ensure_column(conn, "menu_periodes", "startweek_in_cyclus", "INTEGER DEFAULT 1")
    ensure_column(conn, "menu_periodes", "default_prognose_aantal", "REAL")
    ensure_column(conn, "menu_periodes", "actief", "INTEGER DEFAULT 1")
    ensure_column(conn, "menu_periodes", "created_at", "TEXT")
    ensure_column(conn, "menu_periodes", "updated_at", "TEXT")

def migrate_menu_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "menu"):
        return

    ensure_column(conn, "menu", "recept_id", "INTEGER")
    ensure_column(conn, "menu", "serveerdag", "TEXT")
    ensure_column(conn, "menu", "aantal", "REAL")
    ensure_column(conn, "menu", "naam", "TEXT")
    ensure_column(conn, "menu", "created_at", "TEXT")

def migrate_menu_schedule_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "menu"):
        return

    ensure_column(conn, "menu", "menu_groep", "TEXT")
    ensure_column(conn, "menu", "ritme_type", "TEXT")
    ensure_column(conn, "menu", "ritme_interval_weken", "INTEGER")
    ensure_column(conn, "menu", "bron", "TEXT")

def migrate_menu_generation_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "menu"):
        return

    ensure_column(conn, "menu", "prognose_aantal", "REAL")
    ensure_column(conn, "menu", "periode_naam", "TEXT")
    ensure_column(conn, "menu", "is_exception", "INTEGER DEFAULT 0")
    ensure_column(conn, "menu", "opmerking", "TEXT")

def migrate_menu_override_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "menu"):
        return

    ensure_column(conn, "menu", "status", "TEXT DEFAULT 'active'")
    ensure_column(conn, "menu", "replaced_at", "TEXT")
    ensure_column(conn, "menu", "replaced_by_menu_id", "INTEGER")
    ensure_column(conn, "menu", "override_reason", "TEXT")

def migrate_handelingen_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "handelingen"):
        return

    ensure_column(conn, "handelingen", "recept_id", "INTEGER")
    ensure_column(conn, "handelingen", "taak", "TEXT")
    ensure_column(conn, "handelingen", "post", "TEXT")
    ensure_column(conn, "handelingen", "toestel", "TEXT")
    ensure_column(conn, "handelingen", "stap_volgorde", "INTEGER")
    ensure_column(conn, "handelingen", "dag_offset", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "min_offset_dagen", "INTEGER")
    ensure_column(conn, "handelingen", "max_offset_dagen", "INTEGER")
    ensure_column(conn, "handelingen", "actieve_tijd", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "passieve_tijd", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "totale_duur", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "aantal", "REAL")
    ensure_column(conn, "handelingen", "opmerkingen", "TEXT")
    ensure_column(conn, "handelingen", "created_at", "TEXT")
    ensure_column(conn, "handelingen", "is_vaste_taak", "INTEGER DEFAULT 0")



def migrate_posten_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "posten"):
        return

    ensure_column(conn, "posten", "naam", "TEXT")
    ensure_column(conn, "posten", "kleur", "TEXT")
    ensure_column(conn, "posten", "capaciteit_minuten", "INTEGER DEFAULT 450")
    ensure_column(conn, "posten", "startuur", "TEXT")
    ensure_column(conn, "posten", "einduur", "TEXT")
    ensure_column(conn, "posten", "actief", "INTEGER DEFAULT 1")
    ensure_column(conn, "posten", "created_at", "TEXT")

def migrate_posten_startuur_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "posten"):
        return

    ensure_column(conn, "posten", "startuur", "TEXT DEFAULT '08:00'")

def migrate_planning_runs_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "planning_runs"):
        return

    ensure_column(conn, "planning_runs", "naam", "TEXT")
    ensure_column(conn, "planning_runs", "actief", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_runs", "created_at", "TEXT")
    ensure_column(conn, "planning_runs", "updated_at", "TEXT")



def migrate_planning_saved_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "planning_saved"):
        return

    ensure_column(conn, "planning_saved", "planning_run_id", "INTEGER")
    ensure_column(conn, "planning_saved", "planning_id", "TEXT")
    ensure_column(conn, "planning_saved", "werkdag_iso", "TEXT")
    ensure_column(conn, "planning_saved", "recept", "TEXT")
    ensure_column(conn, "planning_saved", "taak", "TEXT")
    ensure_column(conn, "planning_saved", "post", "TEXT")
    ensure_column(conn, "planning_saved", "toestel", "TEXT")
    ensure_column(conn, "planning_saved", "start", "TEXT")
    ensure_column(conn, "planning_saved", "einde", "TEXT")
    ensure_column(conn, "planning_saved", "actieve_tijd", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_saved", "passieve_tijd", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_saved", "totale_duur", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_saved", "locked", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_saved", "manueel_aangepast", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_saved", "start_offset_minuten", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_saved", "created_at", "TEXT")
    ensure_column(conn, "planning_saved", "updated_at", "TEXT")



def migrate_planning_overrides_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "planning_overrides"):
        return

    ensure_column(conn, "planning_overrides", "planning_id", "TEXT")
    ensure_column(conn, "planning_overrides", "planning_run_id", "INTEGER")
    ensure_column(conn, "planning_overrides", "werkdag_override", "TEXT")
    ensure_column(conn, "planning_overrides", "start_offset_minutes", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_overrides", "post_override", "TEXT")
    ensure_column(conn, "planning_overrides", "toestel_override", "TEXT")
    ensure_column(conn, "planning_overrides", "locked", "INTEGER DEFAULT 0")
    ensure_column(conn, "planning_overrides", "move_before_planning_id", "TEXT")
    ensure_column(conn, "planning_overrides", "move_after_planning_id", "TEXT")
    ensure_column(conn, "planning_overrides", "created_at", "TEXT")
    ensure_column(conn, "planning_overrides", "updated_at", "TEXT")


# =========================================================
# DATA BACKFILLS / NORMALIZATION
# =========================================================
def backfill_handelingen_offsets(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "handelingen"):
        return

    _safe_execute(
        conn,
        """
        UPDATE handelingen
        SET dag_offset = 0
        WHERE dag_offset IS NULL
        """,
    )

    _safe_execute(
        conn,
        """
        UPDATE handelingen
        SET min_offset_dagen = dag_offset
        WHERE min_offset_dagen IS NULL
        """,
    )

    _safe_execute(
        conn,
        """
        UPDATE handelingen
        SET max_offset_dagen = dag_offset
        WHERE max_offset_dagen IS NULL
        """,
    )

def backfill_handelingen_planning_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "handelingen"):
        return

    _safe_execute(
        conn,
        """
        UPDATE handelingen
        SET planning_type = 'floating'
        WHERE planning_type IS NULL OR TRIM(planning_type) = ''
        """,
    )

def backfill_handelingen_duration_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "handelingen"):
        return

    _safe_execute(
        conn,
        "UPDATE handelingen SET actieve_tijd = 0 WHERE actieve_tijd IS NULL",
    )
    _safe_execute(
        conn,
        "UPDATE handelingen SET passieve_tijd = 0 WHERE passieve_tijd IS NULL",
    )
    _safe_execute(
        conn,
        """
        UPDATE handelingen
        SET totale_duur = COALESCE(actieve_tijd, 0) + COALESCE(passieve_tijd, 0)
        WHERE totale_duur IS NULL OR totale_duur = 0
        """,
    )

def backfill_posten_startuur(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "posten"):
        return

    _safe_execute(
        conn,
        """
        UPDATE posten
        SET startuur = '08:00'
        WHERE startuur IS NULL OR TRIM(startuur) = ''
        """,
    )

def backfill_planning_runs_active_flag(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "planning_runs"):
        return

    _safe_execute(
        conn,
        "UPDATE planning_runs SET actief = 0 WHERE actief IS NULL",
    )

    row = conn.execute(
        """
        SELECT id
        FROM planning_runs
        WHERE actief = 1
        ORDER BY id
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        first_row = conn.execute(
            """
            SELECT id
            FROM planning_runs
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()
        if first_row is not None:
            row_id = first_row["id"] if isinstance(first_row, sqlite3.Row) else first_row[0]
            conn.execute("UPDATE planning_runs SET actief = 1 WHERE id = ?", (row_id,))



def backfill_planning_saved_flags(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "planning_saved"):
        return

    for col in [
        "locked",
        "manueel_aangepast",
        "start_offset_minuten",
        "actieve_tijd",
        "passieve_tijd",
        "totale_duur",
    ]:
        _safe_execute(conn, f"UPDATE planning_saved SET {col} = 0 WHERE {col} IS NULL")



def backfill_planning_overrides_defaults(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "planning_overrides"):
        return

    _safe_execute(
        conn,
        "UPDATE planning_overrides SET start_offset_minutes = 0 WHERE start_offset_minutes IS NULL",
    )
    _safe_execute(
        conn,
        "UPDATE planning_overrides SET locked = 0 WHERE locked IS NULL",
    )



def backfill_timestamps(conn: sqlite3.Connection, table_name: str) -> None:
    if not _table_exists(conn, table_name):
        return

    cols = _get_column_names(conn, table_name)

    if "created_at" in cols:
        _safe_execute(
            conn,
            f"UPDATE {table_name} SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL",
        )

    if "updated_at" in cols:
        _safe_execute(
            conn,
            f"UPDATE {table_name} SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL",
        )


# =========================================================
# INDEXES
# =========================================================
def create_indexes(conn: sqlite3.Connection) -> None:
    ensure_index(conn, "idx_menu_recept_id", "CREATE INDEX idx_menu_recept_id ON menu(recept_id)")
    ensure_index(conn, "idx_menu_serveerdag", "CREATE INDEX idx_menu_serveerdag ON menu(serveerdag)")
    ensure_index(conn, "idx_handelingen_recept_id", "CREATE INDEX idx_handelingen_recept_id ON handelingen(recept_id)")
    ensure_index(conn, "idx_handelingen_post", "CREATE INDEX idx_handelingen_post ON handelingen(post)")
    ensure_index(conn, "idx_planning_runs_actief", "CREATE INDEX idx_planning_runs_actief ON planning_runs(actief)")
    ensure_index(conn, "idx_planning_saved_run", "CREATE INDEX idx_planning_saved_run ON planning_saved(planning_run_id)")
    ensure_index(conn, "idx_planning_saved_pid", "CREATE INDEX idx_planning_saved_pid ON planning_saved(planning_id)")
    ensure_index(conn, "idx_planning_saved_werkdag", "CREATE INDEX idx_planning_saved_werkdag ON planning_saved(werkdag_iso)")
    ensure_index(conn, "idx_planning_overrides_run", "CREATE INDEX idx_planning_overrides_run ON planning_overrides(planning_run_id)")
    ensure_index(conn, "idx_planning_overrides_pid", "CREATE INDEX idx_planning_overrides_pid ON planning_overrides(planning_id)")


# =========================================================
# PUBLIC ENTRYPOINT
# =========================================================
def init_db(conn: sqlite3.Connection) -> None:
    create_core_tables(conn)

    migrate_recepten_table(conn)
    migrate_menu_table(conn)
    migrate_menu_schedule_fields(conn)
    migrate_menu_generation_fields(conn)
    migrate_menu_override_fields(conn)
    migrate_menu_periodes_table(conn)
    migrate_handelingen_table(conn)
    migrate_posten_table(conn)
    migrate_posten_startuur_fields(conn)
    migrate_planning_runs_table(conn)
    migrate_planning_saved_table(conn)
    migrate_planning_overrides_table(conn)

    backfill_handelingen_offsets(conn)
    backfill_handelingen_duration_fields(conn)
    backfill_posten_startuur(conn)
    backfill_handelingen_planning_fields(conn)
    backfill_planning_runs_active_flag(conn)
    backfill_planning_saved_flags(conn)
    backfill_planning_overrides_defaults(conn)

    for table_name in [
        "recepten",
        "menu",
        "menu_periodes",
        "handelingen",
        "posten",
        "planning_runs",
        "planning_saved",
        "planning_overrides",
    ]:
        backfill_timestamps(conn, table_name)

    create_indexes(conn)
    conn.commit()

def migrate_handelingen_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "handelingen"):
        return

    ensure_column(conn, "handelingen", "recept_id", "INTEGER")
    ensure_column(conn, "handelingen", "taak", "TEXT")
    ensure_column(conn, "handelingen", "post", "TEXT")
    ensure_column(conn, "handelingen", "toestel", "TEXT")
    ensure_column(conn, "handelingen", "stap_volgorde", "INTEGER")
    ensure_column(conn, "handelingen", "dag_offset", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "min_offset_dagen", "INTEGER")
    ensure_column(conn, "handelingen", "max_offset_dagen", "INTEGER")
    ensure_column(conn, "handelingen", "actieve_tijd", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "passieve_tijd", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "totale_duur", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "aantal", "REAL")
    ensure_column(conn, "handelingen", "opmerkingen", "TEXT")
    ensure_column(conn, "handelingen", "created_at", "TEXT")
    ensure_column(conn, "handelingen", "is_vaste_taak", "INTEGER DEFAULT 0")

    # bestaand nieuwer model
    ensure_column(conn, "handelingen", "code", "TEXT DEFAULT ''")
    ensure_column(conn, "handelingen", "naam", "TEXT")
    ensure_column(conn, "handelingen", "sort_order", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "heeft_vast_startuur", "INTEGER DEFAULT 0")
    ensure_column(conn, "handelingen", "vast_startuur", "TEXT DEFAULT ''")

    # nieuw voor slimere vaste taken
    ensure_column(conn, "handelingen", "planning_type", "TEXT DEFAULT 'floating'")
    ensure_column(conn, "handelingen", "actief_vanaf", "TEXT")
    ensure_column(conn, "handelingen", "actief_tot", "TEXT")