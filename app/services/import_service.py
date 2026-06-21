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


def build_incoming_recipe_maps(df: pd.DataFrame):
    handeling_sort_orders = {}
    stap_sort_orders = {}
    incoming_by_recept = {}
    skipped_rows = 0

    for _, row in df.iterrows():
        recept_code = clean_text(row["recept_code"])
        recept_naam = clean_text(row["recept_naam"])
        categorie = clean_text(row["categorie"])

        handeling_code = clean_text(row["handeling_code"])
        handeling_naam = clean_text(row["handeling_naam"])
        stap_naam = clean_text(row["stap_naam"])

        if not recept_code or not recept_naam or not handeling_code or not handeling_naam:
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

        auto_stap_order = get_auto_stap_order(
            stap_sort_orders,
            recept_code,
            handeling_code,
        )

        stap_volgorde = safe_int(
            row["stap_volgorde"],
            auto_stap_order,
        )

        if recept_code not in incoming_by_recept:
            incoming_by_recept[recept_code] = {
                "recept_naam": recept_naam,
                "categorie": categorie,
                "handelingen": {},
            }

        if handeling_code not in incoming_by_recept[recept_code]["handelingen"]:
            incoming_by_recept[recept_code]["handelingen"][handeling_code] = {
                "handeling_code": handeling_code,
                "handeling_naam": handeling_naam,
                "dag_offset": dag_offset,
                "dag_offset_min": dag_offset_min,
                "dag_offset_max": dag_offset_max,
                "volgorde_handeling": volgorde_handeling,
                "post": normalize_post(row["post"]),
                "toestel": normalize_toestel(row["toestel"]),
                "passieve_tijd": safe_int(row["passieve_tijd"], 0),
                "is_vaste_taak": normalize_bool(row["is_vaste_taak"]),
                "stappen": [],
            }

        if stap_naam:
            incoming_by_recept[recept_code]["handelingen"][handeling_code]["stappen"].append(
                {
                    "stap_naam": stap_naam,
                    "stap_tijd": safe_int(row["stap_tijd"], 0),
                    "stap_volgorde": stap_volgorde,
                }
            )

    return incoming_by_recept, skipped_rows


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

    return True


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


def upsert_recipe(conn, recept_code: str, incoming_recipe: dict, overwrite_existing: bool):
    existing = conn.execute(
        """
        SELECT id
        FROM recepten
        WHERE code = ?
        """,
        (recept_code,),
    ).fetchone()

    if existing:
        recept_id = existing["id"]

        if not overwrite_existing:
            return recept_id, False, True

        delete_recipe_contents(conn, recept_id)

        conn.execute(
            """
            UPDATE recepten
            SET naam = ?,
                categorie = ?
            WHERE id = ?
            """,
            (
                incoming_recipe["recept_naam"],
                incoming_recipe["categorie"],
                recept_id,
            ),
        )

        return recept_id, True, False

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO recepten (
            code,
            naam,
            categorie
        )
        VALUES (?, ?, ?)
        """,
        (
            recept_code,
            incoming_recipe["recept_naam"],
            incoming_recipe["categorie"],
        ),
    )

    return cur.lastrowid, False, False


def insert_handeling(conn, recept_id: int, handeling: dict):
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
            handeling["handeling_code"],
            handeling["handeling_naam"],
            handeling["dag_offset"],
            handeling["dag_offset_min"],
            handeling["dag_offset_max"],
            handeling["volgorde_handeling"],
            handeling["post"],
            handeling["toestel"],
            handeling["passieve_tijd"],
            handeling["is_vaste_taak"],
        ),
    )

    return cur.lastrowid


def insert_stap(conn, handeling_id: int, stap: dict):
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
            stap["stap_naam"],
            stap["stap_tijd"],
            stap["stap_volgorde"],
        ),
    )


def import_excel_to_database(
    conn,
    file_bytes: bytes,
    overwrite_existing: bool = False,
):
    df = pd.read_excel(BytesIO(file_bytes))
    df.columns = [str(c).strip() for c in df.columns]
    df = ensure_columns(df)

    incoming_by_recept, skipped_rows = build_incoming_recipe_maps(df)

    imported_recepten = 0
    imported_handelingen = 0
    imported_stappen = 0
    skipped_existing_recepten = 0
    overwritten_recepten = 0
    effectief_gewijzigde_recepten = 0

    try:
        for recept_code, incoming_recipe in incoming_by_recept.items():
            changed = recipe_has_changes(conn, recept_code, incoming_recipe)

            recept_id, overwritten, skipped_existing = upsert_recipe(
                conn=conn,
                recept_code=recept_code,
                incoming_recipe=incoming_recipe,
                overwrite_existing=overwrite_existing,
            )

            if skipped_existing:
                skipped_existing_recepten += 1
                continue

            imported_recepten += 1

            if overwritten:
                overwritten_recepten += 1

            if changed:
                effectief_gewijzigde_recepten += 1

            handelingen = sorted(
                incoming_recipe["handelingen"].values(),
                key=lambda h: (h["volgorde_handeling"], h["handeling_code"]),
            )

            for handeling in handelingen:
                handeling_id = insert_handeling(conn, recept_id, handeling)
                imported_handelingen += 1

                stappen = sorted(
                    handeling["stappen"],
                    key=lambda s: (s["stap_volgorde"], s["stap_naam"]),
                )

                for stap in stappen:
                    insert_stap(conn, handeling_id, stap)
                    imported_stappen += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    return {
        "recepten": imported_recepten,
        "handelingen": imported_handelingen,
        "stappen": imported_stappen,
        "overgeschreven_recepten": overwritten_recepten,
        "effectief_gewijzigde_recepten": effectief_gewijzigde_recepten,
        "overgeslagen_bestaande_recepten": skipped_existing_recepten,
        "overgeslagen_rijen": skipped_rows,
    }