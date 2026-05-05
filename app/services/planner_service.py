from datetime import datetime

import pandas as pd

from app.db import get_db_connection
from app.services.planning import (
    GEEN_TOESTEL,
    build_planning_df,
    get_capacity_status,
    get_post_capaciteiten,
    sync_starturen,
)
from app.services.planning_overrides import set_task_move_after
from app.services.planning_overrides import apply_planning_overrides
from app.services.planning_storage import create_planning_run, save_planning_df

# =========================================================
# SAMENVATTINGEN / ANALYSE
# =========================================================
def build_capacity_summary(conn, planning_df: pd.DataFrame) -> list[dict]:
    if planning_df.empty:
        return []

    post_capaciteiten = get_post_capaciteiten(conn)
    task_df = planning_df.copy()

    if "Taak" in task_df.columns:
        task_df = task_df[task_df["Taak"] != "🕒 Pauze"]

    if task_df.empty:
        return []

    grouped = (
        task_df.groupby(["Werkdag_iso", "Werkdag", "Post"], dropna=False)["Totale duur"]
        .sum()
        .reset_index()
    )

    rows: list[dict] = []

    for _, row in grouped.iterrows():
        post = row["Post"]
        totale_minuten = int(row["Totale duur"] or 0)
        capaciteit_minuten = int(post_capaciteiten.get(post, 0) or 0)

        belasting_pct = None
        if capaciteit_minuten > 0:
            belasting_pct = round((totale_minuten / capaciteit_minuten) * 100, 1)

        rows.append(
            {
                "Werkdag_iso": row["Werkdag_iso"],
                "Werkdag": row["Werkdag"],
                "Post": post,
                "Geplande minuten": totale_minuten,
                "Capaciteit minuten": capaciteit_minuten,
                "Belasting pct": belasting_pct,
                "Status": get_capacity_status(totale_minuten, capaciteit_minuten),
            }
        )

    rows.sort(key=lambda r: (str(r["Werkdag_iso"]), str(r["Post"])))
    return rows


def detect_toestel_conflicten(planning_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    if planning_df.empty:
        planning_df = planning_df.copy()
        planning_df["Toestel conflict"] = False
        planning_df["Conflict details"] = ""
        return planning_df, []

    df = planning_df.copy()
    df["Toestel conflict"] = False
    df["Conflict details"] = ""

    required_cols = {"Werkdag_iso", "Toestel", "Start", "Einde", "Planning ID", "Taak", "Post"}
    if not required_cols.issubset(df.columns):
        return df, []

    work_df = df[
        df["Toestel"].notna()
        & (df["Toestel"] != "")
        & (df["Toestel"] != GEEN_TOESTEL)
        & (df["Taak"] != "🕒 Pauze")
    ].copy()

    if work_df.empty:
        return df, []

    work_df["Start_dt"] = pd.to_datetime(work_df["Start"], errors="coerce")
    work_df["Einde_dt"] = pd.to_datetime(work_df["Einde"], errors="coerce")
    work_df = work_df.dropna(subset=["Start_dt", "Einde_dt"])

    conflict_details_map: dict[str, list[str]] = {}
    conflict_summary: list[dict] = []

    grouped = work_df.groupby(["Werkdag_iso", "Toestel"], dropna=False)

    for (werkdag_iso, toestel), group in grouped:
        group = group.sort_values(["Start_dt", "Einde_dt"]).reset_index()

        for i in range(len(group)):
            a = group.iloc[i]

            for j in range(i + 1, len(group)):
                b = group.iloc[j]

                if b["Start_dt"] >= a["Einde_dt"]:
                    break

                overlap = a["Start_dt"] < b["Einde_dt"] and b["Start_dt"] < a["Einde_dt"]
                if not overlap:
                    continue

                a_id = str(a["Planning ID"])
                b_id = str(b["Planning ID"])

                a_detail = (
                    f"Overlap met {b['Taak']} ({b['Post']}) "
                    f"{b['Start_dt'].strftime('%H:%M')}–{b['Einde_dt'].strftime('%H:%M')}"
                )
                b_detail = (
                    f"Overlap met {a['Taak']} ({a['Post']}) "
                    f"{a['Start_dt'].strftime('%H:%M')}–{a['Einde_dt'].strftime('%H:%M')}"
                )

                conflict_details_map.setdefault(a_id, []).append(a_detail)
                conflict_details_map.setdefault(b_id, []).append(b_detail)

                conflict_summary.append(
                    {
                        "Werkdag_iso": werkdag_iso,
                        "Toestel": toestel,
                        "Planning ID A": a_id,
                        "Taak A": a["Taak"],
                        "Post A": a["Post"],
                        "Start A": a["Start_dt"].strftime("%Y-%m-%d %H:%M"),
                        "Einde A": a["Einde_dt"].strftime("%Y-%m-%d %H:%M"),
                        "Planning ID B": b_id,
                        "Taak B": b["Taak"],
                        "Post B": b["Post"],
                        "Start B": b["Start_dt"].strftime("%Y-%m-%d %H:%M"),
                        "Einde B": b["Einde_dt"].strftime("%Y-%m-%d %H:%M"),
                    }
                )

    if conflict_details_map:
        for idx, row in df.iterrows():
            planning_id = str(row.get("Planning ID"))
            details = conflict_details_map.get(planning_id)
            if details:
                df.at[idx, "Toestel conflict"] = True
                df.at[idx, "Conflict details"] = " | ".join(details)

    return df, conflict_summary


# =========================================================
# OVERRIDES
# =========================================================
def apply_overrides(planning_df: pd.DataFrame, overrides) -> pd.DataFrame:
    if planning_df.empty or not overrides:
        return planning_df

    df = planning_df.copy()

    active_overrides = [o for o in overrides if getattr(o, "locked", False)]
    if not active_overrides:
        return df

    for idx, row in df.iterrows():
        row_recept_id = row.get("Recept ID")
        row_handeling_id = row.get("Handeling ID")
        row_werkdag_iso = row.get("Werkdag_iso")
        row_planning_id = str(row.get("Planning ID"))

        matched_override = None

        for override in active_overrides:
            override_recept_id = getattr(override, "receptId", None)
            override_handeling_id = getattr(override, "handelingId", None)
            override_werkdag_iso = getattr(override, "werkdagIso", None)
            override_planning_id = str(getattr(override, "planningId", ""))

            stable_match = (
                override_recept_id == row_recept_id
                and override_handeling_id == row_handeling_id
                and override_werkdag_iso == row_werkdag_iso
            )

            fallback_match = override_planning_id == row_planning_id

            if stable_match or fallback_match:
                matched_override = override
                break

        if not matched_override:
            continue

        if matched_override.post is not None:
            df.at[idx, "Post"] = matched_override.post

        if matched_override.toestel is not None:
            df.at[idx, "Toestel"] = matched_override.toestel

        if matched_override.start is not None:
            df.at[idx, "Start"] = matched_override.start

        if matched_override.end is not None:
            df.at[idx, "Einde"] = matched_override.end

    return df

def reorder_planning_task(conn, planning_id: str, move_after_planning_id: str):
    set_task_move_after(
        conn=conn,
        planning_id=planning_id,
        target_planning_id=move_after_planning_id,
    )

    return {"success": True}

def move_planning_task(conn, planning_id: str, werkdag_override: str):
    now = datetime.utcnow().isoformat()

    conn.execute(
        """
        INSERT INTO planning_overrides (
            planning_id,
            werkdag_override,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(planning_id) DO UPDATE SET
            werkdag_override = excluded.werkdag_override,
            updated_at = excluded.updated_at
        """,
        (planning_id, werkdag_override, now, now),
    )

    conn.commit()
    return {"success": True}


def override_planning_post(conn, planning_id: str, post_override: str):
    now = datetime.utcnow().isoformat()

    conn.execute(
        """
        INSERT INTO planning_overrides (
            planning_id,
            post_override,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(planning_id) DO UPDATE SET
            post_override = excluded.post_override,
            updated_at = excluded.updated_at
        """,
        (planning_id, post_override, now, now),
    )

    conn.commit()
    return {"success": True}


def lock_planning_task(conn, planning_id: str, locked: bool):
    now = datetime.utcnow().isoformat()

    conn.execute(
        """
        INSERT INTO planning_overrides (
            planning_id,
            locked,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(planning_id) DO UPDATE SET
            locked = excluded.locked,
            updated_at = excluded.updated_at
        """,
        (planning_id, int(locked), now, now),
    )

    conn.commit()
    return {"success": True}


def reset_planning_override(conn, planning_id: str):
    conn.execute(
        """
        DELETE FROM planning_overrides
        WHERE planning_id = ?
        """,
        (planning_id,),
    )

    conn.commit()
    return {"success": True}


# =========================================================
# PLANNER RUN
# =========================================================
def run_planner(payload) -> dict:
    overrides = payload.overrides if hasattr(payload, "overrides") else []
    menu_rotation = payload.menu_rotation if hasattr(payload, "menu_rotation") else None
    menu_groep = payload.menu_groep if hasattr(payload, "menu_groep") else None

    conn = get_db_connection()

    try:
        before_counts = {
            "recepten": conn.execute("SELECT COUNT(*) AS cnt FROM recepten").fetchone()["cnt"],
            "handelingen": conn.execute("SELECT COUNT(*) AS cnt FROM handelingen").fetchone()["cnt"],
            "stappen": conn.execute("SELECT COUNT(*) AS cnt FROM stappen").fetchone()["cnt"],
            "planning_templates": conn.execute("SELECT COUNT(*) AS cnt FROM planning_templates").fetchone()["cnt"],
            "menu_recept_selectie_actief": conn.execute(
                "SELECT COUNT(*) AS cnt FROM menu_recept_selectie WHERE actief = 1"
            ).fetchone()["cnt"],
            "menu_before": conn.execute("SELECT COUNT(*) AS cnt FROM menu").fetchone()["cnt"],
        }

        menu_after_generate = conn.execute(
            "SELECT COUNT(*) AS cnt FROM menu"
        ).fetchone()["cnt"]

        sync_starturen(
            conn=conn,
            start_monday=payload.start_monday,
            start_week=payload.start_week,
            cycles=payload.cycles,
            menu_groep=menu_groep,
        )

        starturen_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM planning_starturen"
        ).fetchone()["cnt"]

        planning_df = build_planning_df(
            conn=conn,
            start_monday=payload.start_monday,
            start_week=payload.start_week,
            cycles=payload.cycles,
            menu_groep=menu_groep,
        )

        planning_df = apply_planning_overrides(conn, planning_df)
        planning_df = apply_overrides(planning_df, overrides)
        planning_df, conflict_summary = detect_toestel_conflicten(planning_df)
        capacity_summary = build_capacity_summary(conn, planning_df)
        conflict_count = len(conflict_summary)

        if "Start" in planning_df.columns:
            planning_df["Start"] = planning_df["Start"].astype(str)

        if "Einde" in planning_df.columns:
            planning_df["Einde"] = planning_df["Einde"].astype(str)

        planning_df = planning_df.astype(object)
        planning_df = planning_df.where(pd.notna(planning_df), None)

        rows = planning_df.to_dict(orient="records")
        
        planning_naam = getattr(payload, "planning_naam", None)

        if not planning_naam:
            planning_naam = f"Planning {payload.start_monday} - {menu_groep or 'alle menu-groepen'}"

        planning_run_id = create_planning_run(
            conn,
            naam=planning_naam,
            beschrijving=f"Menu-groep: {menu_groep or 'alle'} | Start: {payload.start_monday} | Cycli: {payload.cycles}",
        )

        save_planning_df(
            conn,
            planning_df,
            planning_run_id=planning_run_id,
        )

        debug_menu_rotation = None
        if menu_rotation:
            debug_menu_rotation = {
                "menu_type": menu_rotation.menu_type,
                "rotation_length": menu_rotation.rotation_length,
                "week_in_cycle": menu_rotation.week_in_cycle,
            }

        return {
            "planning_run_id": planning_run_id,
            "planning_naam": planning_naam,
            "rows": rows,
            "row_count": len(rows),
            "capacity_summary": capacity_summary,
            "conflict_summary": conflict_summary,
            "conflict_count": conflict_count,
            "debug_overrides": [
                o.model_dump() if hasattr(o, "model_dump") else o.dict()
                for o in overrides
            ],
            "debug_menu_rotation": debug_menu_rotation,
            "debug_counts": {
                **before_counts,
                "menu_after_generate": menu_after_generate,
                "planning_starturen": starturen_count,
            },
        }

    finally:
        conn.close()