from __future__ import annotations


def _parse_subgroep_from_handeling_code(handeling_code: str | None) -> str | None:
    """
    Probeert uit bv. PAZO_1_ZE_2 de subgroep ZE te halen.
    """
    if not handeling_code:
        return None

    parts = handeling_code.split("_")
    if len(parts) < 4:
        return None

    return parts[-2]


def _normalize_planning_type(value: str | None) -> str:
    normalized = str(value or "").strip().lower()

    if normalized in {"hard", "soft", "floating"}:
        return normalized

    return "floating"


def _normalize_optional_text(value) -> str | None:
    if value is None:
        return None

    value = str(value).strip()
    return value or None


def _to_int(value, fallback=0) -> int:
    if value is None or value == "":
        return fallback

    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def get_recipes(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            id,
            code,
            naam,
            categorie
        FROM recepten
        ORDER BY code
        """
    ).fetchall()

    return [
        {
            "recept_id": row["id"],
            "recept_code": row["code"],
            "recept_naam": row["naam"],
            "categorie": row["categorie"],
        }
        for row in rows
    ]


def get_recipe_detail(conn, recept_code: str) -> dict | None:
    recipe = conn.execute(
        """
        SELECT
            id,
            code,
            naam,
            categorie
        FROM recepten
        WHERE code = ?
        """,
        (recept_code,),
    ).fetchone()

    if not recipe:
        return None

    recept_id = recipe["id"]

    handelingen_rows = conn.execute(
        """
        SELECT
            id,
            code,
            naam,
            dag_offset,
            min_offset_dagen,
            max_offset_dagen,
            sort_order,
            post,
            toestel,
            post_policy,
            alternatieve_posten,
            passieve_tijd,
            is_vaste_taak,
            heeft_vast_startuur,
            vast_startuur,
            deadline_time,
            planning_type,
            actief_vanaf,
            actief_tot
        FROM handelingen
        WHERE recept_id = ?
        ORDER BY sort_order, code
        """,
        (recept_id,),
    ).fetchall()

    stappen_rows = conn.execute(
        """
        SELECT
            id,
            handeling_id,
            naam,
            tijd,
            sort_order
        FROM stappen
        WHERE handeling_id IN (
            SELECT id
            FROM handelingen
            WHERE recept_id = ?
        )
        ORDER BY handeling_id, sort_order, id
        """,
        (recept_id,),
    ).fetchall()

    stappen_per_handeling: dict[int, list[dict]] = {}

    for row in stappen_rows:
        handeling_id = row["handeling_id"]

        stappen_per_handeling.setdefault(handeling_id, []).append(
            {
                "stap_id": row["id"],
                "stap_volgorde": row["sort_order"],
                "stap_naam": row["naam"],
                "stap_tijd": row["tijd"],
            }
        )

    handelingen: list[dict] = []

    for row in handelingen_rows:
        handeling_id = row["id"]
        stappen = stappen_per_handeling.get(handeling_id, [])

        actieve_tijd = sum(int(step["stap_tijd"] or 0) for step in stappen)
        passieve_tijd = int(row["passieve_tijd"] or 0)

        dag_offset = _to_int(row["dag_offset"], 0)
        dag_offset_min = _to_int(row["min_offset_dagen"], dag_offset)
        dag_offset_max = _to_int(row["max_offset_dagen"], dag_offset)

        handelingen.append(
            {
                "handeling_id": handeling_id,
                "handeling_code": row["code"],
                "handeling_naam": row["naam"],
                "subgroep_code": _parse_subgroep_from_handeling_code(row["code"]),
                "volgorde_handeling": row["sort_order"],
                "post": row["post"],
                "toestel": row["toestel"],
                "post_policy": row["post_policy"] or "flexible",
                "alternatieve_posten": row["alternatieve_posten"] or "",
                "dag_offset": dag_offset,
                "dag_offset_min": dag_offset_min,
                "dag_offset_max": dag_offset_max,
                "passieve_tijd": passieve_tijd,
                "actieve_tijd": actieve_tijd,
                "totale_duur": actieve_tijd + passieve_tijd,
                "heeft_vast_startuur": bool(row["heeft_vast_startuur"]),
                "vast_startuur": row["vast_startuur"] or "",
                "deadline_time": row["deadline_time"] or "",
                "planning_type": _normalize_planning_type(row["planning_type"]),
                "actief_vanaf": row["actief_vanaf"],
                "actief_tot": row["actief_tot"],
                "stappen": stappen,
            }
        )

    return {
        "recept_id": recept_id,
        "recept_code": recipe["code"],
        "recept_naam": recipe["naam"],
        "categorie": recipe["categorie"],
        "handelingen": handelingen,
    }

def create_recipe(
    conn,
    code: str,
    naam: str,
    categorie=None,
    menu_groep=None,
):
    conn.execute(
        """
        INSERT INTO recepten (
            code,
            naam,
            categorie
        )
        VALUES (?, ?, ?)
        """,
        (
            code.strip(),
            naam.strip(),
            _normalize_optional_text(categorie),
        ),
    )

    conn.commit()

    recipe = conn.execute(
        """
        SELECT
            id,
            code,
            naam,
            categorie
        FROM recepten
        WHERE code = ?
        """,
        (code.strip(),),
    ).fetchone()

    return {
        "recept_id": recipe["id"],
        "recept_code": recipe["code"],
        "recept_naam": recipe["naam"],
        "categorie": recipe["categorie"],
    }

def create_handeling(
    conn,
    recept_id: int,
    code: str,
    naam: str,
    post=None,
    toestel=None,    
    dag_offset=0,
    dag_offset_min=0,
    dag_offset_max=0,
    passieve_tijd=0,
    is_vaste_taak=False,
    heeft_vast_startuur=False,
    vast_startuur=None,
    deadline_time=None,
    planning_type=None,
    actief_vanaf=None,
    actief_tot=None,
    post_policy="flexible",
    alternatieve_posten=None,
):
    recipe = conn.execute(
        """
        SELECT id
        FROM recepten
        WHERE id = ?
        """,
        (recept_id,),
    ).fetchone()

    if not recipe:
        return None

    next_sort_order = conn.execute(
        """
        SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_order
        FROM handelingen
        WHERE recept_id = ?
        """,
        (recept_id,),
    ).fetchone()["next_order"]

    conn.execute(
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
            deadline_time,
            planning_type,
            actief_vanaf,
            actief_tot
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            recept_id,
            code.strip(),
            naam.strip(),
            next_sort_order,
            _normalize_optional_text(post),
            _normalize_optional_text(toestel),
            _normalize_optional_text(post_policy) or "flexible",
            _normalize_optional_text(alternatieve_posten),
            _to_int(dag_offset, 0),
            _to_int(dag_offset_min, 0),
            _to_int(dag_offset_max, 0),
            _to_int(passieve_tijd, 0),
            1 if is_vaste_taak else 0,
            1 if heeft_vast_startuur else 0,
            _normalize_optional_text(vast_startuur),
            _normalize_optional_text(deadline_time),
            _normalize_planning_type(planning_type),
            _normalize_optional_text(actief_vanaf),
            _normalize_optional_text(actief_tot),
        ),
    )

    conn.commit()

    handeling_id = conn.execute(
        "SELECT last_insert_rowid()"
    ).fetchone()[0]

    return {
        "handeling_id": handeling_id,
        "handeling_code": code,
        "handeling_naam": naam,
    }

def create_stap(
    conn,
    handeling_id: int,
    naam: str,
    tijd: int,
):
    handeling = conn.execute(
        """
        SELECT id
        FROM handelingen
        WHERE id = ?
        """,
        (handeling_id,),
    ).fetchone()

    if not handeling:
        return None

    next_sort_order = conn.execute(
        """
        SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_order
        FROM stappen
        WHERE handeling_id = ?
        """,
        (handeling_id,),
    ).fetchone()["next_order"]

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
            handeling_id,
            naam.strip(),
            _to_int(tijd, 0),
            next_sort_order,
        ),
    )

    conn.commit()

    stap_id = conn.execute(
        "SELECT last_insert_rowid()"
    ).fetchone()[0]

    return {
        "stap_id": stap_id,
        "stap_naam": naam,
        "stap_tijd": tijd,
        "stap_volgorde": next_sort_order,
    }

def update_handeling(
    conn,
    handeling_id: int,
    naam: str,
    post,
    toestel,
    dag_offset,
    dag_offset_min,
    dag_offset_max,
    passieve_tijd,
    is_vaste_taak,
    heeft_vast_startuur=False,
    vast_startuur=None,
    deadline_time=None,
    planning_type=None,
    actief_vanaf=None,
    actief_tot=None,
    post_policy="flexible",
    alternatieve_posten=None,
) -> dict | None:
    existing = conn.execute(
        """
        SELECT
            id
        FROM handelingen
        WHERE id = ?
        """,
        (handeling_id,),
    ).fetchone()

    if not existing:
        return None

    normalized_post = _normalize_optional_text(post)
    normalized_toestel = _normalize_optional_text(toestel)
    normalized_post_policy = _normalize_optional_text(post_policy) or "flexible"
    normalized_alternatieve_posten = _normalize_optional_text(alternatieve_posten)
    normalized_planning_type = _normalize_planning_type(planning_type)

    deadline_time_value = _normalize_optional_text(deadline_time)

    normalized_actief_vanaf = (
        actief_vanaf.isoformat()
        if hasattr(actief_vanaf, "isoformat")
        else _normalize_optional_text(actief_vanaf)
    )
    normalized_actief_tot = (
        actief_tot.isoformat()
        if hasattr(actief_tot, "isoformat")
        else _normalize_optional_text(actief_tot)
    )

    dag_offset_value = _to_int(dag_offset, 0)
    dag_offset_min_value = _to_int(dag_offset_min, dag_offset_value)
    dag_offset_max_value = _to_int(dag_offset_max, dag_offset_value)
    heeft_vast_startuur_value = 1 if heeft_vast_startuur else 0
    vast_startuur_value = (
        _normalize_optional_text(vast_startuur)
        if heeft_vast_startuur_value
        else None
    )

    conn.execute(
        """
        UPDATE handelingen
        SET
            naam = ?,
            post = ?,
            toestel = ?,
            post_policy = ?,
            alternatieve_posten = ?,
            dag_offset = ?,
            min_offset_dagen = ?,
            max_offset_dagen = ?,
            passieve_tijd = ?,
            is_vaste_taak = ?,
            heeft_vast_startuur = ?,
            vast_startuur = ?,
            deadline_time = ?,
            planning_type = ?,
            actief_vanaf = ?,
            actief_tot = ?
        WHERE id = ?
        """,
        (
            naam.strip(),
            normalized_post,
            normalized_toestel,
            normalized_post_policy,
            normalized_alternatieve_posten,
            dag_offset_value,
            dag_offset_min_value,
            dag_offset_max_value,
            _to_int(passieve_tijd, 0),
            1 if is_vaste_taak else 0,
            heeft_vast_startuur_value,
            vast_startuur_value,
            deadline_time_value,
            normalized_planning_type,
            normalized_actief_vanaf,
            normalized_actief_tot,
            handeling_id,
        ),
    )
    conn.commit()

    updated = conn.execute(
        """
        SELECT
            id,
            code,
            naam,
            dag_offset,
            min_offset_dagen,
            max_offset_dagen,
            sort_order,
            post,
            toestel,
            post_policy,
            alternatieve_posten,
            passieve_tijd,
            is_vaste_taak,
            heeft_vast_startuur,
            vast_startuur,
            deadline_time,
            planning_type,
            actief_vanaf,
            actief_tot
        FROM handelingen
        WHERE id = ?
        """,
        (handeling_id,),
    ).fetchone()

    dag_offset_value = _to_int(updated["dag_offset"], 0)
    dag_offset_min_value = _to_int(updated["min_offset_dagen"], dag_offset_value)
    dag_offset_max_value = _to_int(updated["max_offset_dagen"], dag_offset_value)

    return {
        "handeling_id": updated["id"],
        "handeling_code": updated["code"],
        "handeling_naam": updated["naam"],
        "subgroep_code": _parse_subgroep_from_handeling_code(updated["code"]),
        "volgorde_handeling": updated["sort_order"],
        "post": updated["post"],
        "toestel": updated["toestel"],
        "post_policy": updated["post_policy"] or "flexible",
        "alternatieve_posten": updated["alternatieve_posten"] or "",
        "dag_offset": dag_offset_value,
        "dag_offset_min": dag_offset_min_value,
        "dag_offset_max": dag_offset_max_value,
        "passieve_tijd": updated["passieve_tijd"],
        "is_vaste_taak": bool(updated["is_vaste_taak"]),
        "heeft_vast_startuur": bool(updated["heeft_vast_startuur"]),
        "vast_startuur": updated["vast_startuur"],
        "deadline_time": updated["deadline_time"],
        "planning_type": _normalize_planning_type(updated["planning_type"]),
        "actief_vanaf": updated["actief_vanaf"],
        "actief_tot": updated["actief_tot"],
    }


def update_stap(
    conn,
    stap_id: int,
    naam: str,
    tijd: int,
):
    existing = conn.execute(
        "SELECT id FROM stappen WHERE id = ?",
        (stap_id,),
    ).fetchone()

    if not existing:
        return None

    conn.execute(
        """
        UPDATE stappen
        SET naam = ?, tijd = ?
        WHERE id = ?
        """,
        (naam.strip(), int(tijd), stap_id),
    )
    conn.commit()

    updated = conn.execute(
        """
        SELECT id, naam, tijd, sort_order
        FROM stappen
        WHERE id = ?
        """,
        (stap_id,),
    ).fetchone()

    return {
        "stap_id": updated["id"],
        "stap_naam": updated["naam"],
        "stap_tijd": updated["tijd"],
        "stap_volgorde": updated["sort_order"],
    }