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

def export_menu_group(conn, menu_groep: str) -> dict:
    menu_rows = conn.execute(
        """
        SELECT *
        FROM menu
        WHERE COALESCE(status, 'active') = 'active'
          AND menu_groep = ?
        ORDER BY serveerdag, cyclus_week, cyclus_dag, recept_id
        """,
        (menu_groep,),
    ).fetchall()

    recept_ids = sorted({row["recept_id"] for row in menu_rows})

    recipes = []
    for recept_id in recept_ids:
        recept = conn.execute(
            """
            SELECT *
            FROM recepten
            WHERE id = ?
            """,
            (recept_id,),
        ).fetchone()

        if not recept:
            continue

        handelingen = conn.execute(
            """
            SELECT *
            FROM handelingen
            WHERE recept_id = ?
            ORDER BY sort_order, id
            """,
            (recept_id,),
        ).fetchall()

        handeling_ids = [h["id"] for h in handelingen]
        stappen = []

        if handeling_ids:
            placeholders = ",".join(["?"] * len(handeling_ids))
            stappen = conn.execute(
                f"""
                SELECT *
                FROM stappen
                WHERE handeling_id IN ({placeholders})
                ORDER BY handeling_id, sort_order, id
                """,
                handeling_ids,
            ).fetchall()

        recipes.append(
            {
                "recept": dict(recept),
                "handelingen": [dict(h) for h in handelingen],
                "stappen": [dict(s) for s in stappen],
            }
        )

    return {
        "version": 1,
        "type": "kitchenmotors_menu_group_export",
        "menu_groep": menu_groep,
        "menu_items": [dict(row) for row in menu_rows],
        "recipes": recipes,
    }


def import_menu_group(conn, payload: dict, target_menu_groep: str | None = None) -> dict:
    source_menu_groep = payload.get("menu_groep") or "Geïmporteerde menu-groep"
    effective_menu_groep = (target_menu_groep or f"{source_menu_groep} - lokaal test").strip()

    recipe_id_map: dict[int, int] = {}
    handeling_id_map: dict[int, int] = {}

    imported_recipes = 0
    reused_recipes = 0
    imported_handelingen = 0
    imported_stappen = 0
    imported_menu_items = 0

    for recipe_bundle in payload.get("recipes", []):
        recept = recipe_bundle.get("recept", {})
        old_recept_id = recept.get("id")
        code = str(recept.get("code") or "").strip()

        if not code:
            continue

        existing = conn.execute(
            """
            SELECT id
            FROM recepten
            WHERE code = ?
            """,
            (code,),
        ).fetchone()

        if existing:
            new_recept_id = int(existing["id"])
            reused_recipes += 1
        else:
            cursor = conn.execute(
                """
                INSERT INTO recepten (
                    code,
                    naam,
                    categorie,
                    menu_groep
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    code,
                    recept.get("naam"),
                    recept.get("categorie"),
                    effective_menu_groep,
                ),
            )
            new_recept_id = int(cursor.lastrowid)
            imported_recipes += 1

        if old_recept_id:
            recipe_id_map[int(old_recept_id)] = new_recept_id

        for handeling in recipe_bundle.get("handelingen", []):
            old_handeling_id = handeling.get("id")

            cursor = conn.execute(
                """
                INSERT INTO handelingen (
                    recept_id,
                    code,
                    naam,
                    sort_order,
                    post,
                    toestel,
                    post_policy,
                    alternatieve_posten,
                    dag_offset,
                    min_offset_dagen,
                    max_offset_dagen,
                    passieve_tijd,
                    is_vaste_taak,
                    heeft_vast_startuur,
                    vast_startuur,
                    planning_type,
                    actief_vanaf,
                    actief_tot
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_recept_id,
                    handeling.get("code"),
                    handeling.get("naam"),
                    handeling.get("sort_order"),
                    handeling.get("post"),
                    handeling.get("toestel"),
                    handeling.get("post_policy") or "fixed",
                    handeling.get("alternatieve_posten"),
                    handeling.get("dag_offset") or 0,
                    handeling.get("min_offset_dagen") or 0,
                    handeling.get("max_offset_dagen") or 0,
                    handeling.get("passieve_tijd") or 0,
                    handeling.get("is_vaste_taak") or 0,
                    handeling.get("heeft_vast_startuur") or 0,
                    handeling.get("vast_startuur"),
                    handeling.get("planning_type") or "floating",
                    handeling.get("actief_vanaf"),
                    handeling.get("actief_tot"),
                ),
            )

            new_handeling_id = int(cursor.lastrowid)
            imported_handelingen += 1

            if old_handeling_id:
                handeling_id_map[int(old_handeling_id)] = new_handeling_id

        for stap in recipe_bundle.get("stappen", []):
            old_handeling_id = stap.get("handeling_id")
            if not old_handeling_id:
                continue

            new_handeling_id = handeling_id_map.get(int(old_handeling_id))
            if not new_handeling_id:
                continue

            conn.execute(
                """
                INSERT INTO stappen (
                    handeling_id,
                    naam,
                    tijd,
                    sort_order
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    new_handeling_id,
                    stap.get("naam"),
                    stap.get("tijd") or 0,
                    stap.get("sort_order"),
                ),
            )
            imported_stappen += 1

    for item in payload.get("menu_items", []):
        old_recept_id = item.get("recept_id")
        if not old_recept_id:
            continue

        new_recept_id = recipe_id_map.get(int(old_recept_id))
        if not new_recept_id:
            continue

        conn.execute(
            """
            INSERT INTO menu (
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
                opmerking,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_recept_id,
                item.get("cyclus_week"),
                item.get("cyclus_dag"),
                item.get("serveerdag"),
                effective_menu_groep,
                item.get("ritme_type"),
                item.get("ritme_interval_weken"),
                "import",
                item.get("prognose_aantal"),
                item.get("periode_naam"),
                item.get("is_exception") or 0,
                item.get("opmerking"),
                "active",
            ),
        )
        imported_menu_items += 1

    conn.commit()

    return {
        "success": True,
        "menu_groep": effective_menu_groep,
        "imported_recipes": imported_recipes,
        "reused_recipes": reused_recipes,
        "imported_handelingen": imported_handelingen,
        "imported_stappen": imported_stappen,
        "imported_menu_items": imported_menu_items,
    }