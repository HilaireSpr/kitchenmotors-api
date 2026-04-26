import pandas as pd

print("OVERVIEW_V2_LOADED")


WEEKDAY_MAP = {
    1: "Maandag",
    2: "Dinsdag",
    3: "Woensdag",
    4: "Donderdag",
    5: "Vrijdag",
    6: "Zaterdag",
    7: "Zondag",
}


def ensure_overview_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    temp = df.copy()

    fallback_columns = {
        "Werkdag_iso": None,
        "Onderdeel": "Onbekend",
        "Post": "Onbekend",
        "Recept": "",
        "Taak": "",
        "Actieve tijd": 0,
        "Passieve tijd": 0,
        "Totale duur": 0,
    }

    for col, default_value in fallback_columns.items():
        if col not in temp.columns:
            temp[col] = default_value

    temp["Onderdeel"] = temp["Onderdeel"].fillna("Onbekend").astype(str)
    temp["Post"] = temp["Post"].fillna("Onbekend").astype(str)
    temp["Recept"] = temp["Recept"].fillna("").astype(str)
    temp["Taak"] = temp["Taak"].fillna("").astype(str)

    temp["Actieve tijd"] = pd.to_numeric(
        temp["Actieve tijd"], errors="coerce"
    ).fillna(0)
    temp["Passieve tijd"] = pd.to_numeric(
        temp["Passieve tijd"], errors="coerce"
    ).fillna(0)
    temp["Totale duur"] = pd.to_numeric(
        temp["Totale duur"], errors="coerce"
    ).fillna(0)

    return temp


def build_overview_from_planning(df: pd.DataFrame) -> pd.DataFrame:
    empty_result = pd.DataFrame(
        columns=[
            "Werkdag",
            "Werkdag_label",
            "Weekdag",
            "Onderdelen",
            "Posten",
            "Aantal recepten",
            "Aantal handelingen",
            "Actieve minuten",
            "Passieve minuten",
            "Totale minuten",
            "Recepten",
        ]
    )

    if df is None or df.empty:
        return empty_result

    temp = ensure_overview_columns(df)

    temp["Werkdag_dt"] = pd.to_datetime(
        temp["Werkdag_iso"], errors="coerce"
    ).dt.normalize()

    temp = temp[temp["Werkdag_dt"].notna()].copy()

    if temp.empty:
        return empty_result

    temp["Weekdag_num"] = temp["Werkdag_dt"].dt.weekday + 1
    temp["Weekdag"] = temp["Weekdag_num"].map(WEEKDAY_MAP)

    grouped = (
        temp.groupby(["Werkdag_dt", "Weekdag"])
        .apply(
            lambda g: pd.Series(
                {
                    "Onderdelen": ", ".join(
                        sorted(set(v for v in g["Onderdeel"] if str(v).strip()))
                    ),
                    "Posten": ", ".join(
                        sorted(set(v for v in g["Post"] if str(v).strip()))
                    ),
                    "Recepten": " | ".join(
                        sorted(set(v for v in g["Recept"] if str(v).strip()))
                    ),
                    "Aantal handelingen": int(g["Taak"].count()),
                    "Actieve minuten": float(g["Actieve tijd"].sum()),
                    "Passieve minuten": float(g["Passieve tijd"].sum()),
                    "Totale minuten": float(g["Totale duur"].sum()),
                }
            )
        )
        .reset_index()
    )

    recepten_count = (
        temp.groupby("Werkdag_dt")["Recept"]
        .nunique()
        .reset_index()
        .rename(columns={"Werkdag_dt": "Werkdag", "Recept": "Aantal recepten"})
    )

    grouped = grouped.rename(columns={"Werkdag_dt": "Werkdag"})
    grouped = grouped.merge(recepten_count, on="Werkdag", how="left")
    grouped = grouped.sort_values("Werkdag")
    grouped["Werkdag_label"] = grouped["Werkdag"].dt.strftime("%d/%m/%Y")

    return grouped[
        [
            "Werkdag",
            "Werkdag_label",
            "Weekdag",
            "Onderdelen",
            "Posten",
            "Aantal recepten",
            "Aantal handelingen",
            "Actieve minuten",
            "Passieve minuten",
            "Totale minuten",
            "Recepten",
        ]
    ]


def build_onderdeel_summary(df: pd.DataFrame) -> pd.DataFrame:
    empty_result = pd.DataFrame(
        columns=[
            "Onderdeel",
            "Aantal taken",
            "Actieve minuten",
            "Passieve minuten",
            "Totale minuten",
        ]
    )

    if df is None or df.empty:
        return empty_result

    temp = ensure_overview_columns(df)

    out = (
        temp.groupby("Onderdeel")
        .apply(
            lambda g: pd.Series(
                {
                    "Aantal taken": int(g["Taak"].count()),
                    "Actieve minuten": float(g["Actieve tijd"].sum()),
                    "Passieve minuten": float(g["Passieve tijd"].sum()),
                    "Totale minuten": float(g["Totale duur"].sum()),
                }
            )
        )
        .reset_index()
    )

    return out.sort_values("Totale minuten", ascending=False)