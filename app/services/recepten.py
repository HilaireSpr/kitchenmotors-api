GEEN_POST = "-"
GEEN_TOESTEL = "Geen"


def clean_text(value, default=""):
    if value is None:
        return default
    return str(value).strip() or default

def bulk_update_menu_groep_for_recept_ids(conn, recept_ids: list[int], menu_groep: str) -> int:
    menu_groep = clean_text(menu_groep)
    recept_ids = [int(rid) for rid in recept_ids if rid is not None]

    if not menu_groep or not recept_ids:
        return 0

    placeholders = ",".join(["?"] * len(recept_ids))

    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE recepten
        SET menu_groep = ?
        WHERE id IN ({placeholders})
        """,
        [menu_groep, *recept_ids],
    )
    conn.commit()
    return cur.rowcount

def get_categorieen(conn, active_only=False):
    query = "SELECT naam FROM categorieen"
    if active_only:
        query += " WHERE actief=1"
    query += " ORDER BY sort_order, naam"
    return [r["naam"] for r in conn.execute(query).fetchall()]


def get_recepten(conn):
    return conn.execute(
        """
        SELECT *
        FROM recepten
        ORDER BY menu_groep, categorie, code, naam
        """
    ).fetchall()


def get_posten(conn):
    rows = conn.execute("SELECT naam FROM posten ORDER BY naam").fetchall()
    return [r["naam"] for r in rows]


def get_toestellen(conn):
    rows = conn.execute("SELECT naam FROM toestellen ORDER BY naam").fetchall()
    waarden = [r["naam"] for r in rows]
    return [GEEN_TOESTEL] + [v for v in waarden if v != GEEN_TOESTEL]


def create_recept(conn, code, naam, categorie, menu_groep=""):
    conn.execute(
        """
        INSERT INTO recepten (code, naam, categorie, menu_groep)
        VALUES (?, ?, ?, ?)
        """,
        (
            clean_text(code),
            clean_text(naam),
            clean_text(categorie),
            clean_text(menu_groep),
        ),
    )
    conn.commit()


def update_recept(conn, recept_id, code, naam, categorie, menu_groep=""):
    conn.execute(
        """
        UPDATE recepten
        SET code=?, naam=?, categorie=?, menu_groep=?
        WHERE id=?
        """,
        (
            clean_text(code),
            clean_text(naam),
            clean_text(categorie),
            clean_text(menu_groep),
            recept_id,
        ),
    )
    conn.commit()


def delete_recept(conn, recept_id):
    conn.execute("DELETE FROM recepten WHERE id=?", (recept_id,))
    conn.commit()


def get_handelingen_for_recept(conn, recept_id):
    return conn.execute(
        """
        SELECT *
        FROM handelingen
        WHERE recept_id=?
        ORDER BY dag_offset, sort_order, code, naam, id
        """,
        (recept_id,),
    ).fetchall()


def create_handeling(
    conn,
    recept_id,
    code,
    naam,
    dag_offset,
    sort_order,
    post,
    toestel,
    passieve_tijd,
    heeft_vast_startuur=0,
    vast_startuur="",
    min_offset_dagen=0,
    max_offset_dagen=0,
    is_vaste_taak=0,
):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO handelingen
        (
            recept_id,
            code,
            naam,
            dag_offset,
            sort_order,
            post,
            toestel,
            passieve_tijd,
            heeft_vast_startuur,
            vast_startuur,
            min_offset_dagen,
            max_offset_dagen,
            is_vaste_taak
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            recept_id,
            clean_text(code),
            clean_text(naam),
            int(dag_offset),
            int(sort_order),
            clean_text(post, GEEN_POST),
            clean_text(toestel, GEEN_TOESTEL),
            int(passieve_tijd),
            int(heeft_vast_startuur),
            clean_text(vast_startuur),
            int(min_offset_dagen),
            int(max_offset_dagen),
            int(is_vaste_taak),
        ),
    )
    conn.commit()
    return cur.lastrowid


def update_handeling(
    conn,
    handeling_id,
    code,
    naam,
    dag_offset,
    sort_order,
    post,
    toestel,
    passieve_tijd,
    heeft_vast_startuur=0,
    vast_startuur="",
    min_offset_dagen=0,
    max_offset_dagen=0,
    is_vaste_taak=0,
):
    conn.execute(
        """
        UPDATE handelingen
        SET code=?, naam=?, dag_offset=?, sort_order=?, post=?, toestel=?, passieve_tijd=?, heeft_vast_startuur=?, vast_startuur=?, min_offset_dagen=?, max_offset_dagen=?, is_vaste_taak=?        WHERE id=?
        """,
        (
            clean_text(code),
            clean_text(naam),
            int(dag_offset),
            int(sort_order),
            clean_text(post, GEEN_POST),
            clean_text(toestel, GEEN_TOESTEL),
            int(passieve_tijd),
            int(heeft_vast_startuur),
            clean_text(vast_startuur),
            int(min_offset_dagen),
            int(max_offset_dagen),
            int(is_vaste_taak),
            handeling_id,
        ),
    )
    conn.commit()


def delete_handeling(conn, handeling_id):
    conn.execute("DELETE FROM handelingen WHERE id=?", (handeling_id,))
    conn.commit()


def get_stappen_for_handeling(conn, handeling_id):
    return conn.execute(
        """
        SELECT *
        FROM stappen
        WHERE handeling_id=?
        ORDER BY sort_order, id
        """,
        (handeling_id,),
    ).fetchall()


def create_stap(conn, handeling_id, naam, tijd, sort_order):
    conn.execute(
        """
        INSERT INTO stappen (handeling_id, naam, tijd, sort_order)
        VALUES (?, ?, ?, ?)
        """,
        (
            handeling_id,
            clean_text(naam),
            tijd,
            sort_order,
        ),
    )
    conn.commit()


def delete_stap(conn, stap_id):
    conn.execute("DELETE FROM stappen WHERE id=?", (stap_id,))
    conn.commit()


def get_actieve_tijd(conn, handeling_id):
    row = conn.execute(
        """
        SELECT COALESCE(SUM(tijd), 0) AS totaal
        FROM stappen
        WHERE handeling_id=?
        """,
        (handeling_id,),
    ).fetchone()
    return int(row["totaal"] if row else 0)


def get_templates_for_recept(conn, recept_id):
    return conn.execute(
        """
        SELECT *
        FROM planning_templates
        WHERE recept_id=?
        ORDER BY week, dag, id
        """,
        (recept_id,),
    ).fetchall()


def save_recept_templates(conn, recept_id, weken, dagen):
    conn.execute("DELETE FROM planning_templates WHERE recept_id=?", (recept_id,))
    for week in weken:
        for dag in dagen:
            conn.execute(
                """
                INSERT INTO planning_templates (recept_id, week, dag)
                VALUES (?, ?, ?)
                """,
                (recept_id, int(week), int(dag)),
            )
    conn.commit()