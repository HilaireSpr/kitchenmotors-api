from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class ToestelConflict:
    toestel: str
    start_a: pd.Timestamp
    einde_a: pd.Timestamp
    taak_a: str
    post_a: str
    start_b: pd.Timestamp
    einde_b: pd.Timestamp
    taak_b: str
    post_b: str


def _is_valid_toestel(value: Any) -> bool:
    if value is None:
        return False

    text = str(value).strip()
    if text == "":
        return False

    if text.lower() in {"geen", "-", "n.v.t.", "nvt"}:
        return False

    return True


def _overlap(
    start_a: pd.Timestamp,
    einde_a: pd.Timestamp,
    start_b: pd.Timestamp,
    einde_b: pd.Timestamp,
) -> bool:
    if pd.isna(start_a) or pd.isna(einde_a) or pd.isna(start_b) or pd.isna(einde_b):
        return False

    return start_a < einde_b and start_b < einde_a


def detect_toestel_conflicten(planning_df: pd.DataFrame) -> tuple[pd.DataFrame, list[ToestelConflict]]:
    if planning_df is None or planning_df.empty:
        result = pd.DataFrame() if planning_df is None else planning_df.copy()
        if not result.empty:
            result["Toestel conflict"] = False
            result["Conflict details"] = ""
        return result, []

    df = planning_df.copy()

    if "Toestel" not in df.columns:
        df["Toestel"] = ""
    if "Taak" not in df.columns:
        df["Taak"] = ""
    if "Post" not in df.columns:
        df["Post"] = ""
    if "Werkdag_iso" not in df.columns:
        df["Werkdag_iso"] = ""
    if "Start" not in df.columns:
        df["Start"] = pd.NaT
    if "Einde" not in df.columns:
        df["Einde"] = pd.NaT

    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["Einde"] = pd.to_datetime(df["Einde"], errors="coerce")
    df["Toestel conflict"] = False
    df["Conflict details"] = ""

    conflicts: list[ToestelConflict] = []

    valid_df = df[df["Toestel"].apply(_is_valid_toestel)].copy()
    valid_df = valid_df.sort_values(["Werkdag_iso", "Toestel", "Start", "Einde"]).reset_index()

    for (werkdag, toestel), groep in valid_df.groupby(["Werkdag_iso", "Toestel"], dropna=False):
        rows = groep.to_dict("records")

        for i in range(len(rows)):
            row_a = rows[i]
            for j in range(i + 1, len(rows)):
                row_b = rows[j]

                if _overlap(row_a["Start"], row_a["Einde"], row_b["Start"], row_b["Einde"]):
                    conflict = ToestelConflict(
                        toestel=str(toestel),
                        start_a=row_a["Start"],
                        einde_a=row_a["Einde"],
                        taak_a=str(row_a.get("Taak", "")),
                        post_a=str(row_a.get("Post", "")),
                        start_b=row_b["Start"],
                        einde_b=row_b["Einde"],
                        taak_b=str(row_b.get("Taak", "")),
                        post_b=str(row_b.get("Post", "")),
                    )
                    conflicts.append(conflict)

                    idx_a = row_a["index"]
                    idx_b = row_b["index"]

                    df.at[idx_a, "Toestel conflict"] = True
                    df.at[idx_b, "Toestel conflict"] = True

                    detail_a = (
                        f"{toestel}: overlap met '{row_b.get('Taak', '')}' "
                        f"({row_b['Start'].strftime('%H:%M')}–{row_b['Einde'].strftime('%H:%M')})"
                    )
                    detail_b = (
                        f"{toestel}: overlap met '{row_a.get('Taak', '')}' "
                        f"({row_a['Start'].strftime('%H:%M')}–{row_a['Einde'].strftime('%H:%M')})"
                    )

                    existing_a = str(df.at[idx_a, "Conflict details"]).strip()
                    existing_b = str(df.at[idx_b, "Conflict details"]).strip()

                    df.at[idx_a, "Conflict details"] = (
                        f"{existing_a} | {detail_a}" if existing_a else detail_a
                    )
                    df.at[idx_b, "Conflict details"] = (
                        f"{existing_b} | {detail_b}" if existing_b else detail_b
                    )

    return df, conflicts