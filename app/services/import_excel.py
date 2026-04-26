import pandas as pd
from utils.helpers import clean_text, safe_int

def import_excel(conn, file):
    df = pd.read_excel(file)

    c = conn.cursor()

    for _, row in df.iterrows():
        code = clean_text(row["recept_code"])
        naam = clean_text(row["recept_naam"])
        categorie = clean_text(row["categorie"])

        c.execute("""
        INSERT OR IGNORE INTO recepten (code, naam, categorie)
        VALUES (?, ?, ?)
        """, (code, naam, categorie))

    conn.commit()