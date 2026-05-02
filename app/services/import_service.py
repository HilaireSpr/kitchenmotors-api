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
    "dag_offset": 0,
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
        if pd.isna(value):
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


def get_parent_handeling_code(recept_code: str, subgroep_code: str) -> str:
    """
    Parent-code die soms foutief als handeling ontstond.

    Voorbeeld:
    recept_code = PAZO_1
    subgroep_code = ZE
    parent = PAZO_1_ZE

    Die parent-code mag NIET als aparte handeling geïmporteerd worden.
    Echte handeling-codes zoals PAZO_1_ZE_1, PAZO_3_ZE_A1, GG10_1, SR15_10,
    FB... blijven wél geldig.
    """
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


def cleanup_parent_handeling(conn, recept_id: int, parent_code: str):
    """
    Verwijdert enkel de foutieve parent-code voor dit recept.
    Verwijdert dus NIET langer codes die niet op _nummer eindigen, want codes zoals
    PAZO_3_ZE_A1, GG10_1, SR15_10 en FB... zijn geldig.
    """
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
    conn.commit()


def import_excel_to_database(conn, file_bytes: bytes):
    df = pd.read_excel(BytesIO(file_bytes))
    df.columns = [str(c).strip() for c in df.columns]
    df = ensure_columns(df)

    imported_recepten = 0
    imported_handelingen = 0
    imported_stappen = 0
    skipped_rows = 0

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

        dag_offset = safe_int(row["dag_offset"], 0)

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
        is_vaste_taak = safe_int(row["is_vaste_taak"], 0)

        row_db = conn.execute(
            "SELECT id FROM recepten WHERE code=?",
            (recept_code,),
        ).fetchone()

        if row_db:
            recept_id = row_db["id"]
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
            WHERE recept_id=? AND code=?
            """,
            (recept_id, handeling_code),
        ).fetchone()

        if row_db:
            handeling_id = row_db["id"]

            conn.execute(
                """
                UPDATE handelingen
                SET naam=?,
                    dag_offset=?,
                    sort_order=?,
                    post=?,
                    toestel=?,
                    passieve_tijd=?,
                    is_vaste_taak=?
                WHERE id=?
                """,
                (
                    handeling_naam,
                    dag_offset,
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
                    sort_order,
                    post,
                    toestel,
                    passieve_tijd,
                    is_vaste_taak
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recept_id,
                    handeling_code,
                    handeling_naam,
                    dag_offset,
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

    return {
        "recepten": imported_recepten,
        "handelingen": imported_handelingen,
        "stappen": imported_stappen,
        "overgeslagen_rijen": skipped_rows,
    }
