def init_db(conn):
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS categorieen (
        id INTEGER PRIMARY KEY,
        naam TEXT UNIQUE,
        sort_order INTEGER DEFAULT 0,
        actief INTEGER DEFAULT 1
    )
    """)

    c.execute("""
CREATE TABLE IF NOT EXISTS posten (
    id INTEGER PRIMARY KEY,
    naam TEXT UNIQUE,
    kleur TEXT DEFAULT '',
    capaciteit_minuten INTEGER DEFAULT 480
)
""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS toestellen (
        id INTEGER PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS recepten (
        id INTEGER PRIMARY KEY,
        naam TEXT,
        code TEXT UNIQUE,
        categorie TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS handelingen (
        id INTEGER PRIMARY KEY,
        recept_id INTEGER,
        naam TEXT,
        code TEXT,
        dag_offset INTEGER,
        sort_order INTEGER,
        post TEXT,
        toestel TEXT,
        passieve_tijd INTEGER,
        heeft_vast_startuur INTEGER DEFAULT 0,
        vast_startuur TEXT DEFAULT ''
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS stappen (
        id INTEGER PRIMARY KEY,
        handeling_id INTEGER,
        naam TEXT,
        tijd INTEGER,
        sort_order INTEGER
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS planning_templates (
        id INTEGER PRIMARY KEY,
        recept_id INTEGER,
        week INTEGER,
        dag INTEGER
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS menu (
        id INTEGER PRIMARY KEY,
        recept_id INTEGER,
        cyclus_week INTEGER,
        cyclus_dag INTEGER,
        serveerdag TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS planning_starturen (
        id INTEGER PRIMARY KEY,
        werkdag TEXT,
        post TEXT,
        starttijd TEXT
    )
    """)
    conn.execute(
    """
    CREATE TABLE IF NOT EXISTS menu_recept_selectie (
        recept_id INTEGER PRIMARY KEY,
        actief INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY (recept_id) REFERENCES recepten(id)
    )
    """
)

    conn.commit()