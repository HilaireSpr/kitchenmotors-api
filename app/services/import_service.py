from io import BytesIO
import pandas as pd


IMPORT_COLUMNS = [
    "recept_code",
    "recept_naam",
    "categorie",
    "handeling_code",
    "handeling_naam",
    "dag_offset",
    "volgorde_handeling",
    "post",
    "toestel",
    "passieve_tijd",
    "stap_volgorde",
    "stap_naam",
    "stap_tijd",
]


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
    if value.lower() in {"", "geen", "none", "-"}:
        return "Geen"
    return value


def import_excel_to_database(conn, file_bytes: bytes):
    df = pd.read_excel(BytesIO(file_bytes))
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in IMPORT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Ontbrekende kolommen: {', '.join(missing)}")

    imported_recepten = 0
    imported_handelingen = 0
    imported_stappen = 0

    seen_recepten = set()
    seen_handelingen = set()

    for idx, row in df.iterrows():
        recept_code = clean_text(row["recept_code"])
        recept_naam = clean_text(row["recept_naam"])
        categorie = clean_text(row["categorie"])
        handeling_code = clean_text(row["handeling_code"])
        handeling_naam = clean_text(row["handeling_naam"])
        dag_offset = safe_int(row["dag_offset"], 0)
        volgorde_handeling = safe_int(row["volgorde_handeling"], 0)
        post = normalize_post(row["post"])
        toestel = normalize_toestel(row["toestel"])
        passieve_tijd = safe_int(row["passieve_tijd"], 0)
        stap_volgorde = safe_int(row["stap_volgorde"], 0)
        stap_naam = clean_text(row["stap_naam"])
        stap_tijd = safe_int(row["stap_tijd"], 0)
        is_vaste_taak = safe_int(row["is_vaste_taak"], 0) if "is_vaste_taak" in df.columns else 0

        if not recept_code or not recept_naam or not handeling_naam:
            continue

        # --- recept ---
        row_db = conn.execute(
            "SELECT id FROM recepten WHERE code=?",
            (recept_code,),
        ).fetchone()

        if row_db:
            recept_id = row_db["id"]
        else:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO recepten (code, naam, categorie) VALUES (?, ?, ?)",
                (recept_code, recept_naam, categorie),
            )
            conn.commit()
            recept_id = cur.lastrowid

        if recept_code not in seen_recepten:
            imported_recepten += 1
            seen_recepten.add(recept_code)

        # --- handeling ---
        row_db = conn.execute(
            "SELECT id FROM handelingen WHERE recept_id=? AND code=?",
            (recept_id, handeling_code),
        ).fetchone()

        if row_db:
            handeling_id = row_db["id"]
            conn.execute(
                """
                UPDATE handelingen
                SET naam=?, dag_offset=?, sort_order=?, post=?, toestel=?, passieve_tijd=?, is_vaste_taak=?
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
                INSERT INTO handelingen
                (recept_id, code, naam, dag_offset, sort_order, post, toestel, passieve_tijd, is_vaste_taak)
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
            
        key = (recept_id, handeling_code)
        if key not in seen_handelingen:
            imported_handelingen += 1
            seen_handelingen.add(key)

        # --- stap ---
        if stap_naam:
            conn.execute(
                """
                INSERT INTO stappen (handeling_id, naam, tijd, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (handeling_id, stap_naam, stap_tijd, stap_volgorde),
            )
            conn.commit()
            imported_stappen += 1

    return {
        "recepten": imported_recepten,
        "handelingen": imported_handelingen,
        "stappen": imported_stappen,
    }