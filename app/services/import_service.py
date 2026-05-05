from io import BytesIO

import pandas as pd


REQUIRED_COLUMNS = [
    "recept_code",
    "recept_naam",
    "handeling_code",
    "handeling_naam",
    "stap_naam",
]

OPTIONAL_DEFAULTS = {
    "categorie": "",
    "subgroep_code": "",
    "dag_offset": None,
    "dag_offset_min": None,
    "dag_offset_max": None,
    "volgorde_handeling": None,
    "post": "-",
    "toestel": "Geen",
    "passieve_tijd": 0,
    "stap_volgorde": None,
    "stap_tijd": 0,
    "is_vaste_taak": 0,
}


def clean_text(value, default=""):
    if value is None:
        return default

    try:
        if pd.isna(value):
            return default
    except Exception:
        pass

    value = str(value).strip()
    return value if value else default


def safe_int(value, default=0):
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def normalize_post(value):
    value = clean_text(value, "-")
    return value if value else "-"


def normalize_toestel(value):
    value = clean_text(value, "Geen")

    if value.lower() in {"", "geen", "none", "-", "nan"}:
        return "Geen"

    return value


def normalize_bool(value):
    value_text = clean_text(value).lower()

    if value_text in {"ja", "yes", "true", "1", "x"}:
        return 1

    if value_text in {"nee", "no", "false", "0", ""}:
        return 0

    return safe_int(value, 0)


def get_parent_handeling_code(recept_code: str, subgroep_code: str) -> str:
    recept_code = clean_text(recept_code)
    subgroep_code = clean_text(subgroep_code)

    if not recept_code or not subgroep_code:
        return ""

    return f"{recept_code}_{subgroep_code}"


def is_parent_handeling_code(recept_code: str, subgroep_code: str, handeling_code: str) -> bool:
    parent_code = get_parent_handeling_code(recept_code, subgroep_code)
    return bool(parent_code and clean_text(handeling_code) == parent_code)


def ensure_columns(df: pd.DataFrame):
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]

    if missing_required:
        raise ValueError(
            f"Ontbrekende verplichte kolommen: {', '.join(missing_required)}"
        )

    for column, default_value in OPTIONAL_DEFAULTS.items():
        if column not in df.columns:
            df[column] = default_value

    return df


def get_auto_handeling_order(handeling_sort_orders, recept_code, handeling_code):
    key = (recept_code, handeling_code)

    if key not in handeling_sort_orders:
        existing_for_recept = [
            existing_key
            for existing_key in handeling_sort_orders
            if existing_key[0] == recept_code
        ]

        handeling_sort_orders[key] = len(existing_for_recept) + 1

    return handeling_sort_orders[key]


def get_auto_stap_order(stap_sort_orders, recept_code, handeling_code):
    key = (recept_code, handeling_code)

    if key not in stap_sort_orders:
        stap_sort_orders[key] = 0

    stap_sort_orders[key] += 1
    return stap_sort_orders[key]


def normalize_offset_values(row):
    dag_offset_min = safe_int(row.get("dag_offset_min"), 0)
    dag_offset_max = safe_int(row.get("dag_offset_max"), dag_offset_min)

    dag_offset = safe_int(row.get("dag_offset"), dag_offset_max)

    return dag_offset, dag_offset_min, dag_offset_max


def cleanup_parent_handeling(conn, recept_id: int, parent_code: str):
    parent_code = clean_text(parent_code)

    if not parent_code:
        return

    conn.execute(
        """
        DELETE FROM handelingen
        WHERE recept_id = ?
          AND code = ?
        """,
        (recept_id, parent_code),
    )


def build_incoming_recipe_maps(df: pd.DataFrame):
    handeling_sort_orders = {}
    stap_sort_orders = {}

    incoming_by_recept = {}

    for _, row in df.iterrows():
        recept_code = clean_text(row["recept_code"])
        recept_naam = clean_text(row["recept_naam"])
        categorie = clean_text(row["categorie"])
        subgroep_code = clean_text(row["subgroep_code"])

        handeling_code = clean_text(row["handeling_code"])
        handeling_naam = clean_text(row["handeling_naam"])
        stap_naam = clean_text(row["stap_naam"])

        if not recept_code or not recept_naam or not handeling_code or not handeling_naam:
            continue

        if is_parent_handeling_code(recept_code, subgroep_code, handeling_code):
            continue

        dag_offset, dag_offset_min, dag_offset_max = normalize_offset_values(row)

        auto_handeling_order = get_auto_handeling_order(
            handeling_sort_orders,
            recept_code,
            handeling_code,
        )

        volgorde_handeling = safe_int(
            row["volgorde_handeling"],
            auto_handeling_order,
        )

        post = normalize_post(row["post"])
        toestel = normalize_toestel(row["toestel"])
        passieve_tijd = safe_int(row["passieve_tijd"], 0)
        is_vaste_taak = normalize_bool(row["is_vaste_taak"])

        auto_stap_order = get_auto_stap_order(
            stap_sort_orders,
            recept_code,
            handeling_code,
        )

        stap_volgorde = safe_int(
            row["stap_volgorde"],
            auto_stap_order,
        )

        stap_tijd = safe_int(row["stap_tijd"], 0)

        if recept_code not in incoming_by_recept:
            incoming_by_recept[recept_code] = {
                "recept_naam": recept_naam,
                "categorie": categorie,
                "handelingen": {},
            }

        if handeling_code not in incoming_by_recept[recept_code]["handelingen"]:
            incoming_by_recept[recept_code]["handelingen"][handeling_code] = {
                "handeling_naam": handeling_naam,
                "dag_offset": dag_offset,
                "dag_offset_min": dag_offset_min,
                "dag_offset_max": dag_offset_max,
                "volgorde_handeling": volgorde_handeling,
                "post": post,
                "toestel": toestel,
                "passieve_tijd": passieve_tijd,
                "is_vaste_taak": is_vaste_taak,
                "stappen": [],
            }

        if stap_naam:
            incoming_by_recept[recept_code]["handelingen"][handeling_code]["stappen"].append(
                {
                    "stap_naam": stap_naam,
                    "stap_tijd": stap_tijd,
                    "stap_volgorde": stap_volgorde,
                }
            )

    return incoming_by_recept


def recipe_has_changes(conn, recept_code: str, incoming_recipe: dict) -> bool:
    recipe = conn.execute(
        """
        SELECT id, naam, categorie
        FROM recepten
        WHERE code = ?
        """,
        (recept_code,),
    ).fetchone()

    if not recipe:
        return False

    if clean_text(recipe["naam"]) != clean_text(incoming_recipe["recept_naam"]):
        return True

    if clean_text(recipe["categorie"]) != clean_text(incoming_recipe["categorie"]):
        return True

    recept_id = recipe["id"]

    existing_handelingen = conn.execute(
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
            is_vaste_taak
        FROM handelingen
        WHERE recept_id = ?
        ORDER BY code
        """,
        (recept_id,),
    ).fetchall()

    existing_map = {}

    for handeling in existing_handelingen:
        stappen = conn.execute(
            """
            SELECT naam, tijd, sort_order
            FROM stappen
            WHERE handeling_id = ?
            ORDER BY sort_order, id
            """,
            (handeling["id"],),
        ).fetchall()

        existing_map[handeling["code"]] = {
            "handeling_naam": clean_text(handeling["naam"]),
            "dag_offset": safe_int(handeling["dag_offset"], 0),
            "dag_offset_min": safe_int(
                handeling["min_offset_dagen"],
                safe_int(handeling["dag_offset"], 0),
            ),
            "dag_offset_max": safe_int(
                handeling["max_offset_dagen"],
                safe_int(handeling["dag_offset"], 0),
            ),
            "volgorde_handeling": safe_int(handeling["sort_order"], 0),
            "post": clean_text(handeling["post"]),
            "toestel": clean_text(handeling["toestel"]),
            "passieve_tijd": safe_int(handeling["passieve_tijd"], 0),
            "is_vaste_taak": safe_int(handeling["is_vaste_taak"], 0),
            "stappen": [
                {
                    "stap_naam": clean_text(stap["naam"]),
                    "stap_tijd": safe_int(stap["tijd"], 0),
                    "stap_volgorde": safe_int(stap["sort_order"], 0),
                }
                for stap in stappen
            ],
        }

    return existing_map != incoming_recipe["handelingen"]


def delete_recipe_contents(conn, recept_id: int):
    conn.execute(
        """
        DELETE FROM stappen
        WHERE handeling_id IN (
            SELECT id FROM handelingen WHERE recept_id = ?
        )
        """,
        (recept_id,),
    )

    conn.execute(
        "DELETE FROM handelingen WHERE recept_id = ?",
        (recept_id,),
    )

    conn.execute(
        "DELETE FROM menu WHERE recept_id = ?",
        (recept_id,),
    )

    conn.execute(
        "DELETE FROM menu_recept_selectie WHERE recept_id = ?",
        (recept_id,),
    )

    conn.execute(
        "DELETE FROM planning_templates WHERE recept_id = ?",
        (recept_id,),
    )


def import_excel_to_database(
    conn,
    file_bytes: bytes,
    overwrite_existing: bool = False,
):
    df = pd.read_excel(BytesIO(file_bytes))
    df.columns = [str(c).strip() for c in df.columns]
    df = ensure_columns(df)

    incoming_by_recept = build_incoming_recipe_maps(df)

    imported_recepten = 0
    imported_handelingen = 0
    imported_stappen = 0
    skipped_rows = 0
    skipped_existing_recepten = 0
    overwritten_recepten = 0
    effectief_gewijzigde_recepten = 0

    recepten_to_skip = set()
    recepten_overwritten = set()
    gewijzigde_recepten = set()

    seen_recepten = set()
    seen_handelingen = set()

    handeling_sort_orders = {}
    stap_sort_orders = {}

    for _, row in df.iterrows():
        recept_code = clean_text(row["recept_code"])
        recept_naam = clean_text(row["recept_naam"])
        categorie = clean_text(row["categorie"])
        subgroep_code = clean_text(row["subgroep_code"])

        handeling_code = clean_text(row["handeling_code"])
        handeling_naam = clean_text(row["handeling_naam"])
        stap_naam = clean_text(row["stap_naam"])

        if not recept_code or not recept_naam or not handeling_code or not handeling_naam:
            skipped_rows += 1
            continue

        parent_code = get_parent_handeling_code(recept_code, subgroep_code)

        if is_parent_handeling_code(recept_code, subgroep_code, handeling_code):
            skipped_rows += 1
            continue

        dag_offset, dag_offset_min, dag_offset_max = normalize_offset_values(row)

        auto_handeling_order = get_auto_handeling_order(
            handeling_sort_orders,
            recept_code,
            handeling_code,
        )

        volgorde_handeling = safe_int(
            row["volgorde_handeling"],
            auto_handeling_order,
        )

        post = normalize_post(row["post"])
        toestel = normalize_toestel(row["toestel"])
        passieve_tijd = safe_int(row["passieve_tijd"], 0)
        is_vaste_taak = normalize_bool(row["is_vaste_taak"])

        auto_stap_order = get_auto_stap_order(
            stap_sort_orders,
            recept_code,
            handeling_code,
        )

        stap_volgorde = safe_int(
            row["stap_volgorde"],
            auto_stap_order,
        )

        stap_tijd = safe_int(row["stap_tijd"], 0)

        row_db = conn.execute(
            "SELECT id FROM recepten WHERE code = ?",
            (recept_code,),
        ).fetchone()

        if row_db:
            if not overwrite_existing:
                if recept_code not in recepten_to_skip:
                    skipped_existing_recepten += 1
                    recepten_to_skip.add(recept_code)

                continue

            recept_id = row_db["id"]

            if recept_code not in recepten_overwritten:
                incoming_recipe = incoming_by_recept.get(recept_code)

                if incoming_recipe and recipe_has_changes(conn, recept_code, incoming_recipe):
                    gewijzigde_recepten.add(recept_code)

                delete_recipe_contents(conn, recept_id)

                conn.execute(
                    """
                    UPDATE recepten
                    SET naam = ?,
                        categorie = ?
                    WHERE id = ?
                    """,
                    (recept_naam, categorie, recept_id),
                )

                conn.commit()

                overwritten_recepten += 1
                recepten_overwritten.add(recept_code)

        else:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO recepten (code, naam, categorie)
                VALUES (?, ?, ?)
                """,
                (recept_code, recept_naam, categorie),
            )
            conn.commit()
            recept_id = cur.lastrowid

        cleanup_parent_handeling(conn, recept_id, parent_code)

        if recept_code not in seen_recepten:
            imported_recepten += 1
            seen_recepten.add(recept_code)

        row_db = conn.execute(
            """
            SELECT id
            FROM handelingen
            WHERE recept_id = ?
              AND code = ?
            """,
            (recept_id, handeling_code),
        ).fetchone()

        if row_db:
            handeling_id = row_db["id"]

            conn.execute(
                """
                UPDATE handelingen
                SET naam = ?,
                    dag_offset = ?,
                    min_offset_dagen = ?,
                    max_offset_dagen = ?,
                    sort_order = ?,
                    post = ?,
                    toestel = ?,
                    passieve_tijd = ?,
                    is_vaste_taak = ?
                WHERE id = ?
                """,
                (
                    handeling_naam,
                    dag_offset,
                    dag_offset_min,
                    dag_offset_max,
                    volgorde_handeling,
                    post,
                    toestel,
                    passieve_tijd,
                    is_vaste_taak,
                    handeling_id,
                ),
            )
            conn.commit()

        else:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO handelingen (
                    recept_id,
                    code,
                    naam,
                    dag_offset,
                    min_offset_dagen,
                    max_offset_dagen,
                    sort_order,
                    post,
                    toestel,
                    passieve_tijd,
                    is_vaste_taak
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recept_id,
                    handeling_code,
                    handeling_naam,
                    dag_offset,
                    dag_offset_min,
                    dag_offset_max,
                    volgorde_handeling,
                    post,
                    toestel,
                    passieve_tijd,
                    is_vaste_taak,
                ),
            )
            conn.commit()
            handeling_id = cur.lastrowid

        handeling_key = (recept_id, handeling_code)

        if handeling_key not in seen_handelingen:
            imported_handelingen += 1
            seen_handelingen.add(handeling_key)

        if stap_naam:
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
                    stap_naam,
                    stap_tijd,
                    stap_volgorde,
                ),
            )
            conn.commit()
            imported_stappen += 1

    effectief_gewijzigde_recepten = len(gewijzigde_recepten)

    return {
        "recepten": imported_recepten,
        "handelingen": imported_handelingen,
        "stappen": imported_stappen,
        "overgeschreven_recepten": overwritten_recepten,
        "effectief_gewijzigde_recepten": effectief_gewijzigde_recepten,
        "overgeslagen_bestaande_recepten": skipped_existing_recepten,
        "overgeslagen_rijen": skipped_rows,
    }