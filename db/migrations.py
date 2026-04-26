def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_column_names(conn, table_name: str) -> list[str]:
    if not table_exists(conn, table_name):
        return []

    info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in info]


def add_column_if_missing(
    conn,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    if not table_exists(conn, table_name):
        return

    bestaande_kolommen = get_column_names(conn, table_name)

    if column_name not in bestaande_kolommen:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def create_tables_if_missing(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS productieplanning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            werkdag_iso TEXT,
            post TEXT,
            recept TEXT,
            taak TEXT,
            toestel TEXT,
            start TEXT,
            einde TEXT,
            totale_duur INTEGER,
            batch_key TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS menu_recept_selectie (
            recept_id INTEGER PRIMARY KEY,
            actief INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (recept_id) REFERENCES recepten(id)
        )
        """
    )


def seed_menu_recept_selectie(conn) -> None:
    conn.execute(
        """
        INSERT INTO menu_recept_selectie (recept_id, actief)
        SELECT r.id, 1
        FROM recepten r
        WHERE NOT EXISTS (
            SELECT 1
            FROM menu_recept_selectie mrs
            WHERE mrs.recept_id = r.id
        )
        """
    )


def run_migrations(conn) -> None:
    create_tables_if_missing(conn)

    # ===============================
    # posten
    # ===============================
    add_column_if_missing(
        conn,
        "posten",
        "kleur",
        "kleur TEXT DEFAULT ''",
    )
    add_column_if_missing(
        conn,
        "posten",
        "capaciteit_minuten",
        "capaciteit_minuten INTEGER DEFAULT 480",
    )

    # ===============================
    # categorieen
    # ===============================
    add_column_if_missing(
        conn,
        "categorieen",
        "sort_order",
        "sort_order INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "categorieen",
        "actief",
        "actief INTEGER DEFAULT 1",
    )

    # ===============================
    # recepten
    # ===============================
    add_column_if_missing(
        conn,
        "recepten",
        "categorie",
        "categorie TEXT DEFAULT ''",
    )
    add_column_if_missing(
        conn,
        "recepten",
        "menu_groep",
        "menu_groep TEXT DEFAULT ''",
    )

    # ===============================
    # handelingen
    # ===============================
    add_column_if_missing(
        conn,
        "handelingen",
        "code",
        "code TEXT DEFAULT ''",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "dag_offset",
        "dag_offset INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "min_offset_dagen",
        "min_offset_dagen INTEGER",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "max_offset_dagen",
        "max_offset_dagen INTEGER",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "sort_order",
        "sort_order INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "post",
        "post TEXT DEFAULT '-'",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "toestel",
        "toestel TEXT DEFAULT 'Geen'",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "passieve_tijd",
        "passieve_tijd INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "heeft_vast_startuur",
        "heeft_vast_startuur INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "handelingen",
        "vast_startuur",
        "vast_startuur TEXT DEFAULT ''",
    )

    # ===============================
    # productieplanning
    # ===============================
    add_column_if_missing(
        conn,
        "productieplanning",
        "actieve_tijd",
        "actieve_tijd INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "productieplanning",
        "passieve_tijd",
        "passieve_tijd INTEGER DEFAULT 0",
    )
    add_column_if_missing(
        conn,
        "productieplanning",
        "onderdeel",
        "onderdeel TEXT DEFAULT ''",
    )

    # ===============================
    # stappen
    # ===============================
    add_column_if_missing(
        conn,
        "stappen",
        "sort_order",
        "sort_order INTEGER DEFAULT 0",
    )

    # ===============================
    # seed data
    # ===============================
    seed_menu_recept_selectie(conn)

    # ===============================
    # recepten
    # ===============================
    add_column_if_missing(
        conn,
        "recepten",
        "code",
        "code TEXT DEFAULT ''",
    )
    add_column_if_missing(
        conn,
        "recepten",
        "categorie",
        "categorie TEXT DEFAULT ''",
    )
    add_column_if_missing(
        conn,
        "recepten",
        "menu_groep",
        "menu_groep TEXT DEFAULT ''",
    )

    # ===============================
    # menu
    # ===============================
    add_column_if_missing(
        conn,
        "menu",
        "cyclus_week",
        "cyclus_week INTEGER DEFAULT 1",
    )
    add_column_if_missing(
        conn,
        "menu",
        "cyclus_dag",
        "cyclus_dag INTEGER DEFAULT 1",
    )

    conn.commit()