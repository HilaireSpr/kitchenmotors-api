def get_recept_selectie(conn):
    rows = conn.execute(
        """
        SELECT
            r.id,
            r.naam,
            r.categorie,
            r.menu_groep,
            COALESCE(mrs.actief, 1) AS actief
        FROM recepten r
        LEFT JOIN menu_recept_selectie mrs
            ON mrs.recept_id = r.id
        ORDER BY
            COALESCE(r.menu_groep, ''),
            COALESCE(r.categorie, ''),
            r.naam
        """
    ).fetchall()

    return rows


def save_recept_selectie(conn, selectie_ids: list[int]):
    conn.execute("UPDATE menu_recept_selectie SET actief = 0")

    for rid in selectie_ids:
        conn.execute(
            """
            INSERT INTO menu_recept_selectie (recept_id, actief)
            VALUES (?, 1)
            ON CONFLICT(recept_id) DO UPDATE SET actief = 1
            """,
            (rid,),
        )

    conn.commit()


def get_actieve_recepten(conn):
    rows = conn.execute(
        """
        SELECT recept_id
        FROM menu_recept_selectie
        WHERE actief = 1
        ORDER BY recept_id
        """
    ).fetchall()

    return [r["recept_id"] for r in rows]


def get_bestaande_menu_groepen(conn):
    rows = conn.execute(
        """
        SELECT DISTINCT TRIM(COALESCE(menu_groep, '')) AS menu_groep
        FROM recepten
        WHERE TRIM(COALESCE(menu_groep, '')) <> ''
        ORDER BY menu_groep
        """
    ).fetchall()

    return [r["menu_groep"] for r in rows]