from datetime import date

from fastapi import APIRouter, HTTPException, Query

from app.db import get_db_connection
from app.services.planning_storage import load_planning_df
from app.services.workfloor_service import complete_task, get_completed_task_ids


router = APIRouter()


def _find_column(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _get_task_id(row, index):
    for key in ["Planning ID", "planning_id", "id", "ID"]:
        if key in row and row[key] is not None:
            return str(row[key])
    return str(index)

def _get_handeling_from_task(task_title: str):
    if not task_title:
        return ""

    code = task_title.split(" - ")[0].strip()

    parts = code.split("_")

    if len(parts) >= 3:
        return "_".join(parts[:3])

    return code

def _clean_value(value):
    if value is None:
        return None

    text = str(value)

    if text.lower() in ["nan", "nat", "none"]:
        return None

    return value

@router.get("/my-tasks/today")
def get_my_tasks_today(
    user_id: str = Query(...),
    work_date: str | None = Query(default=None),
):
    conn = get_db_connection()

    try:
        df = load_planning_df(conn)

        if df.empty:
            return {"success": True, "tasks": []}

        columns = list(df.columns)

        medewerker_col = _find_column(
            columns,
            [
                "assigned_user_id",
                "medewerker_id",
                "Medewerker ID",
                "Medewerker",
                "Personeelslid",
                "personeelslid",
            ],
        )

        # Tijdelijk: als er nog geen medewerker-kolom is,
        # gebruiken we Post als user_id.
        if not medewerker_col:
            medewerker_col = "Post"

        date_col = _find_column(
            columns,
            [
                "Werkdag_iso",
                "werkdag_iso",
                "Werkdag",
                "werkdag",
                "Datum",
                "datum",
                "Start",
            ],
        )

        if not date_col:
            raise HTTPException(
                status_code=400,
                detail=f"Geen datum-kolom gevonden. Beschikbare kolommen: {columns}",
            )

        selected_date = work_date or date.today().isoformat()
        completed_ids = get_completed_task_ids(conn, user_id)

        df_filtered = df.copy()
        df_filtered[date_col] = df_filtered[date_col].astype(str).str[:10]

        df_filtered = df_filtered[
            (df_filtered[medewerker_col].astype(str) == str(user_id))
            & (df_filtered[date_col] == selected_date)
        ]

        tasks = []

        for index, row in df_filtered.iterrows():
            row_dict = row.to_dict()
            task_id = _get_task_id(row_dict, index)
            task_title = row_dict.get("Taak") or row_dict.get("taak") or ""

            if "pauze" in str(task_title).lower():
                continue

            if task_id in completed_ids:
                continue

            tasks.append(
                {
                    "id": task_id,
                    "title": row_dict.get("Taak")
                    or row_dict.get("taak")
                    or row_dict.get("Recept")
                    or row_dict.get("recept")
                    or row_dict.get("Product")
                    or row_dict.get("product")
                    or "Taak",
                    "post": _clean_value(row_dict.get("Post") or row_dict.get("post")),
                    "recept": _clean_value(row_dict.get("Recept") or row_dict.get("recept")),
                    "handeling_id": _clean_value(row_dict.get("Handeling ID")),
                    "handeling": _get_handeling_from_task(
                        row_dict.get("Taak") or row_dict.get("taak") or ""
                    ),
                    "toestel": _clean_value(row_dict.get("Toestel") or row_dict.get("toestel")),
                    "stappen": _clean_value(row_dict.get("Stappen") or row_dict.get("stappen")) or "",
                    "start": str(row_dict.get("Start", "")),
                    "end": str(row_dict.get("Einde", "")),
                    "status": "open",
                    "completed_at": None,
                }
            )

        return {
            "success": True,
            "tasks": tasks,
        }

    finally:
        conn.close()


@router.patch("/tasks/{task_id}/complete")
def complete_task_endpoint(task_id: str, payload: dict):
    conn = get_db_connection()

    try:
        complete_task(
            conn=conn,
            planning_id=task_id,
            user_id=payload["user_id"],
        )

        return {
            "success": True,
            "task_id": task_id,
            "status": "done",
        }

    finally:
        conn.close()