from datetime import datetime
import sqlite3

from planner.planning import generate_menu, sync_starturen


def get_recept_selectie(conn):
    rows = conn.execute(
        """
        SELECT
            r.id,
            r.code,
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

    return [dict(r) for r in rows]


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


def generate_menu_for_selection(conn, start_monday: str, start_week: int, cycles: int):
    start_monday_date = datetime.strptime(start_monday, "%Y-%m-%d").date()

    generate_menu(
        conn=conn,
        start_monday=start_monday_date,
        start_week=int(start_week),
        cycles=int(cycles),
    )

    sync_starturen(conn)


def get_menu_items(conn):
    rows = conn.execute(
        """
        SELECT
            m.id,
            m.recept_id,
            m.cyclus_week,
            m.cyclus_dag,
            m.serveerdag,
            m.menu_groep,
            m.ritme_type,
            m.ritme_interval_weken,
            m.bron,
            m.prognose_aantal,
            m.periode_naam,
            m.is_exception,
            m.opmerking,
            r.code,
            r.naam,
            r.categorie,
            r.menu_groep AS recept_menu_groep
        FROM menu m
        JOIN recepten r ON r.id = m.recept_id
        WHERE COALESCE(m.status, 'active') = 'active'
        ORDER BY m.serveerdag, COALESCE(m.menu_groep, r.menu_groep), r.code, r.naam
        """
    ).fetchall()

    return [dict(r) for r in rows]


def create_menu_item(
    conn,
    recept_id: int,
    serveerdag: str,
    cyclus_week: int | None = None,
    cyclus_dag: int | None = None,
    menu_groep: str | None = None,
    ritme_type: str | None = None,
    ritme_interval_weken: int | None = None,
    bron: str | None = "manual",
    prognose_aantal: float | None = None,
    periode_naam: str | None = None,
    is_exception: int | None = 0,
    opmerking: str | None = None,
):
    recept = conn.execute(
        """
        SELECT id, menu_groep
        FROM recepten
        WHERE id = ?
        """,
        (recept_id,),
    ).fetchone()

    if not recept:
        raise ValueError(f"Recept met id {recept_id} niet gevonden")

    effective_menu_groep = menu_groep if menu_groep is not None else recept["menu_groep"]

    cur = conn.cursor()
    cyclus_week = int(cyclus_week or 1)

    existing = cur.execute(
        """
        SELECT id
        FROM menu
        WHERE recept_id = ?
        AND serveerdag = ?
        AND COALESCE(menu_groep, '') = COALESCE(?, '')
        AND COALESCE(ritme_type, '') = COALESCE(?, '')
        AND COALESCE(cyclus_week, 1) = ?
        AND COALESCE(status, 'active') = 'active'
        LIMIT 1
        """,
        (
            recept_id,
            serveerdag,
            menu_groep,
            ritme_type,
            cyclus_week,
        ),
    ).fetchone()

    if existing:
        raise ValueError("Dit recept zit al in deze menu-groep voor deze dag/cyclus.")
    
    cur.execute(
        """
        INSERT INTO menu
        (
            recept_id,
            cyclus_week,
            cyclus_dag,
            serveerdag,
            menu_groep,
            ritme_type,
            ritme_interval_weken,
            bron,
            prognose_aantal,
            periode_naam,
            is_exception,
            opmerking
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            recept_id,
            cyclus_week,
            cyclus_dag,
            serveerdag,
            effective_menu_groep,
            ritme_type,
            ritme_interval_weken,
            bron,
            prognose_aantal,
            periode_naam,
            int(is_exception or 0),
            opmerking,
        ),
    )
    conn.commit()
    return cur.lastrowid

def replace_menu_override(
    conn: sqlite3.Connection,
    serveerdag: str,
    menu_groep: str,
    recept_id: int,
    prognose_aantal: float | None = None,
    override_reason: str | None = None,
) -> dict:
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    recept = conn.execute(
        """
        SELECT id, menu_groep
        FROM recepten
        WHERE id = ?
        """,
        (recept_id,),
    ).fetchone()

    if not recept:
        raise ValueError(f"Recept met id {recept_id} niet gevonden")

    effective_menu_groep = menu_groep if menu_groep is not None else recept["menu_groep"]

    # 1. Zoek actieve items op dezelfde dag + groep
    rows = cursor.execute(
        """
        SELECT id, bron, is_exception, cyclus_week, cyclus_dag
        FROM menu
        WHERE serveerdag = ?
          AND menu_groep = ?
          AND COALESCE(status, 'active') = 'active'
        """,
        (serveerdag, effective_menu_groep),
    ).fetchall()

    replaced_ids = []
    cyclus_week = None
    cyclus_dag = None

    # 2. Zet generated items en bestaande exception-overrides op replaced
    for row in rows:
        if isinstance(row, sqlite3.Row):
            menu_id = row["id"]
            bron = row["bron"]
            is_exception = row["is_exception"]
            row_cyclus_week = row["cyclus_week"]
            row_cyclus_dag = row["cyclus_dag"]
        else:
            menu_id, bron, is_exception, row_cyclus_week, row_cyclus_dag = row

        if cyclus_week is None and row_cyclus_week is not None:
            cyclus_week = row_cyclus_week

        if cyclus_dag is None and row_cyclus_dag is not None:
            cyclus_dag = row_cyclus_dag

        should_replace = (
            bron == "generated"
            or (bron == "manual" and int(is_exception or 0) == 1)
        )

        if should_replace:
            cursor.execute(
                """
                UPDATE menu
                SET status = 'replaced',
                    replaced_at = ?,
                    replaced_by_menu_id = NULL
                WHERE id = ?
                """,
                (now, menu_id),
            )
            replaced_ids.append(menu_id)

    if cyclus_week is None or cyclus_dag is None:
        raise ValueError(
            f"Geen bestaand actief menu-item gevonden met cyclusgegevens voor "
            f"serveerdag={serveerdag} en menu_groep={effective_menu_groep}"
        )

    # 3. Voeg nieuw override item toe
    cursor.execute(
        """
        INSERT INTO menu (
            recept_id,
            cyclus_week,
            cyclus_dag,
            serveerdag,
            menu_groep,
            bron,
            is_exception,
            prognose_aantal,
            status,
            override_reason,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            recept_id,
            cyclus_week,
            cyclus_dag,
            serveerdag,
            effective_menu_groep,
            "manual",
            1,
            prognose_aantal,
            "active",
            override_reason,
            now,
        ),
    )

    created_id = cursor.lastrowid

    # 4. Koppel oude records aan het nieuwe record
    for old_id in replaced_ids:
        cursor.execute(
            """
            UPDATE menu
            SET replaced_by_menu_id = ?
            WHERE id = ?
            """,
            (created_id, old_id),
        )

    conn.commit()

    return {
        "success": True,
        "created_id": created_id,
        "replaced_ids": replaced_ids,
    }

def create_menu_override(
    conn,
    serveerdag: str,
    recept_id: int,
    menu_groep: str | None = None,
    prognose_aantal: float | None = None,
    opmerking: str | None = None,
    cyclus_week: int | None = None,
    cyclus_dag: int | None = None,
):
    recept = conn.execute(
        """
        SELECT id, menu_groep
        FROM recepten
        WHERE id = ?
        """,
        (recept_id,),
    ).fetchone()

    if not recept:
        raise ValueError(f"Recept met id {recept_id} niet gevonden")

    effective_menu_groep = menu_groep if menu_groep is not None else recept["menu_groep"]

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO menu
        (
            recept_id,
            cyclus_week,
            cyclus_dag,
            serveerdag,
            menu_groep,
            ritme_type,
            ritme_interval_weken,
            bron,
            prognose_aantal,
            periode_naam,
            is_exception,
            opmerking
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            recept_id,
            cyclus_week,
            cyclus_dag,
            serveerdag,
            effective_menu_groep,
            "override",
            None,
            "manual",
            prognose_aantal,
            None,
            1,
            opmerking,
        ),
    )
    conn.commit()
    return cur.lastrowid

def update_menu_item(
    conn,
    menu_item_id: int,
    serveerdag: str,
    cyclus_week: int | None = None,
    cyclus_dag: int | None = None,
    menu_groep: str | None = None,
    ritme_type: str | None = None,
    ritme_interval_weken: int | None = None,
    prognose_aantal: float | None = None,
    periode_naam: str | None = None,
    is_exception: int | None = 0,
    opmerking: str | None = None,
):
    existing = conn.execute(
        """
        SELECT id
        FROM menu
        WHERE id = ?
          AND COALESCE(status, 'active') = 'active'
        """,
        (menu_item_id,),
    ).fetchone()

    if not existing:
        raise ValueError(f"Menu-item met id {menu_item_id} niet gevonden")

    conn.execute(
        """
        UPDATE menu
        SET
            serveerdag = ?,
            cyclus_week = ?,
            cyclus_dag = ?,
            menu_groep = ?,
            ritme_type = ?,
            ritme_interval_weken = ?,
            prognose_aantal = ?,
            periode_naam = ?,
            is_exception = ?,
            opmerking = ?
        WHERE id = ?
        """,
        (
            serveerdag,
            cyclus_week,
            cyclus_dag,
            menu_groep,
            ritme_type,
            ritme_interval_weken,
            prognose_aantal,
            periode_naam,
            int(is_exception or 0),
            opmerking,
            menu_item_id,
        ),
    )

    conn.commit()

    return menu_item_id

def delete_menu_item(conn, menu_item_id: int):
    conn.execute("DELETE FROM menu WHERE id = ?", (menu_item_id,))
    conn.commit()