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
            passieve_tijd,
            is_vaste_taak,
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
                "dag_offset": dag_offset,
                "dag_offset_min": dag_offset_min,
                "dag_offset_max": dag_offset_max,
                "passieve_tijd": passieve_tijd,
                "actieve_tijd": actieve_tijd,
                "totale_duur": actieve_tijd + passieve_tijd,
                "is_vaste_taak": bool(row["is_vaste_taak"]),
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
    planning_type=None,
    actief_vanaf=None,
    actief_tot=None,
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
    normalized_planning_type = _normalize_planning_type(planning_type)

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

    conn.execute(
        """
        UPDATE handelingen
        SET
            naam = ?,
            post = ?,
            toestel = ?,
            dag_offset = ?,
            min_offset_dagen = ?,
            max_offset_dagen = ?,
            passieve_tijd = ?,
            is_vaste_taak = ?,
            planning_type = ?,
            actief_vanaf = ?,
            actief_tot = ?
        WHERE id = ?
        """,
        (
            naam.strip(),
            normalized_post,
            normalized_toestel,
            dag_offset_value,
            dag_offset_min_value,
            dag_offset_max_value,
            _to_int(passieve_tijd, 0),
            1 if is_vaste_taak else 0,
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
            passieve_tijd,
            is_vaste_taak,
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
        "dag_offset": dag_offset_value,
        "dag_offset_min": dag_offset_min_value,
        "dag_offset_max": dag_offset_max_value,
        "passieve_tijd": updated["passieve_tijd"],
        "is_vaste_taak": bool(updated["is_vaste_taak"]),
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