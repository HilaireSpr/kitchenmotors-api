import json

from fastapi import APIRouter, HTTPException

from fastapi import Query

from fastapi.responses import StreamingResponse
from app.services.planning_export import (
    export_planning_run_to_excel,
    export_visible_day_rows_to_excel,
)

from app.db import get_db_connection
from app.schemas.planning import (
    PlanningLockRequest,
    PlanningMoveOverrideRequest,
    PlanningPostOverrideRequest,
    PlanningRequest,
    PlanningResetRequest,
    PlanningReorderRequest,
    PlanningToestelOverrideRequest,
)
from app.services.planner_service import (
    lock_planning_task,
    move_planning_task,
    override_planning_post,
    reorder_planning_task,
    reset_planning_override,
    run_planner,
    override_planning_toestel,
)
from app.services.planning import build_planning_df
from app.services.planning_v3 import build_planning_v3_df
from app.services.planning_v2 import build_planning_v2_df
from app.services.planning_overrides import (
    apply_planning_overrides,
    clear_task_override,
    set_task_lock,
)
from app.services.planning_storage import (
    delete_planning_run,
    get_planning_runs,
    load_planning_df,
    set_active_planning_run,
)
from app.services.planning_dependencies import apply_dependency_warnings

router = APIRouter()


def dataframe_to_rows(df):
    if "Start" in df.columns:
        df["Start"] = df["Start"].astype(str)

    if "Einde" in df.columns:
        df["Einde"] = df["Einde"].astype(str)

    json_data = df.to_json(orient="records", date_format="iso")
    return json.loads(json_data)

def get_row_dependency_status(conn, planning_run_id: int, planning_id: str):
    df = load_planning_df(conn, planning_run_id)

    if df is None or df.empty:
        return None

    df = apply_planning_overrides(
        conn=conn,
        planning_df=df,
        planning_run_id=planning_run_id,
    )

    rows = dataframe_to_rows(df)
    rows = apply_dependency_warnings(rows)

    for row in rows:
        if row.get("Planning ID") == planning_id:
            return {
                "status": row.get("Dependency status"),
                "warning": row.get("Dependency warning"),
            }

    return None


def assert_task_not_dependency_blocked(conn, planning_run_id: int | None, planning_id: str):
    if planning_run_id is None:
        return

    dependency = get_row_dependency_status(
        conn=conn,
        planning_run_id=planning_run_id,
        planning_id=planning_id,
    )

    if not dependency:
        return

    if dependency.get("status") == "blocked":
        raise HTTPException(
            status_code=400,
            detail=dependency.get("warning") or "Deze verplaatsing breekt de taakvolgorde.",
        )

def _normalize_planning_df_for_compare(df):
    if df is None or df.empty:
        return []

    work_df = df.copy()

    for col in ["Start", "Einde"]:
        if col in work_df.columns:
            work_df[col] = work_df[col].astype(str)

    work_df = work_df.astype(object)
    work_df = work_df.where(work_df.notna(), None)

    return work_df.to_dict(orient="records")


def _rows_by_planning_id(rows: list[dict]) -> dict[str, dict]:
    result = {}

    for row in rows:
        planning_id = row.get("Planning ID")

        if planning_id is None:
            continue

        result[str(planning_id)] = row

    return result


def _compare_planning_rows(v1_rows: list[dict], v2_rows: list[dict]) -> dict:
    v1_by_id = _rows_by_planning_id(v1_rows)
    v2_by_id = _rows_by_planning_id(v2_rows)

    v1_ids = set(v1_by_id.keys())
    v2_ids = set(v2_by_id.keys())

    common_ids = sorted(v1_ids & v2_ids)

    only_in_v1 = [v1_by_id[planning_id] for planning_id in sorted(v1_ids - v2_ids)]
    only_in_v2 = [v2_by_id[planning_id] for planning_id in sorted(v2_ids - v1_ids)]

    different_post = []
    different_workday = []
    different_start = []
    different_toestel = []

    for planning_id in common_ids:
        v1 = v1_by_id[planning_id]
        v2 = v2_by_id[planning_id]

        base = {
            "Planning ID": planning_id,
            "Recept": v2.get("Recept") or v1.get("Recept"),
            "Taak": v2.get("Taak") or v1.get("Taak"),
        }

        if v1.get("Post") != v2.get("Post"):
            different_post.append({
                **base,
                "v1_Post": v1.get("Post"),
                "v2_Post": v2.get("Post"),
                "v1_Planner reden": v1.get("Planner reden"),
                "v2_Planner reden": v2.get("Planner reden"),
            })

        if v1.get("Werkdag_iso") != v2.get("Werkdag_iso"):
            different_workday.append({
                **base,
                "v1_Werkdag_iso": v1.get("Werkdag_iso"),
                "v2_Werkdag_iso": v2.get("Werkdag_iso"),
                "v1_Planner reden": v1.get("Planner reden"),
                "v2_Planner reden": v2.get("Planner reden"),
            })

        if str(v1.get("Start")) != str(v2.get("Start")):
            different_start.append({
                **base,
                "v1_Start": str(v1.get("Start")),
                "v2_Start": str(v2.get("Start")),
                "v1_Post": v1.get("Post"),
                "v2_Post": v2.get("Post"),
            })

        if v1.get("Toestel") != v2.get("Toestel"):
            different_toestel.append({
                **base,
                "v1_Toestel": v1.get("Toestel"),
                "v2_Toestel": v2.get("Toestel"),
            })

    return {
        "summary": {
            "v1_row_count": len(v1_rows),
            "v2_row_count": len(v2_rows),
            "common_count": len(common_ids),
            "only_in_v1_count": len(only_in_v1),
            "only_in_v2_count": len(only_in_v2),
            "different_post_count": len(different_post),
            "different_workday_count": len(different_workday),
            "different_start_count": len(different_start),
            "different_toestel_count": len(different_toestel),
        },
        "different_post": different_post[:100],
        "different_workday": different_workday[:100],
        "different_start": different_start[:100],
        "different_toestel": different_toestel[:100],
        "only_in_v1": only_in_v1[:50],
        "only_in_v2": only_in_v2[:50],
    }

@router.get("/runs")
def get_planning_runs_endpoint():
    conn = get_db_connection()
    try:
        rows = get_planning_runs(conn)

        return {
            "success": True,
            "result": [
                {
                    "id": row["id"],
                    "naam": row["naam"],
                    "beschrijving": row["beschrijving"],
                    "aangemaakt_op": row["aangemaakt_op"],
                    "laatst_gebruikt_op": row["laatst_gebruikt_op"],
                    "actief": bool(row["actief"]),
                }
                for row in rows
            ],
        }
    finally:
        conn.close()


@router.get("/runs/{planning_run_id}")
def get_planning_run_rows_endpoint(planning_run_id: int):
    conn = get_db_connection()
    try:
        set_active_planning_run(conn, planning_run_id)

        df = load_planning_df(conn, planning_run_id)

        if df is None or df.empty:
            return {
                "success": True,
                "result": {"rows": []},
            }

        df = apply_planning_overrides(
            conn=conn,
            planning_df=df,
            planning_run_id=planning_run_id,
        )

        rows = dataframe_to_rows(df)
        rows = apply_dependency_warnings(rows)

        return {
            "success": True,
            "result": {"rows": rows},
        }
    finally:
        conn.close()


@router.delete("/runs/{planning_run_id}")
def delete_planning_run_endpoint(planning_run_id: int):
    conn = get_db_connection()
    try:
        delete_planning_run(conn, planning_run_id)
        return {"success": True}
    finally:
        conn.close()


@router.post("/run")
def run_planning_endpoint(payload: PlanningRequest):
    result = run_planner(payload)

    return {
        "success": True,
        "result": result,
    }


@router.post("/override/move")
def move_planning_task_endpoint(payload: PlanningMoveOverrideRequest):
    conn = get_db_connection()
    try:
        result = move_planning_task(
            conn=conn,
            planning_id=payload.planning_id,
            werkdag_override=payload.werkdag_override,
            planning_run_id=payload.planning_run_id,
        )
        return {"success": True, "result": result}
    finally:
        conn.close()


@router.post("/override/post")
def override_planning_post_endpoint(payload: PlanningPostOverrideRequest):
    conn = get_db_connection()
    try:
        result = override_planning_post(
            conn=conn,
            planning_id=payload.planning_id,
            post_override=payload.post_override,
            planning_run_id=payload.planning_run_id,
        )
        return {"success": True, "result": result}
    finally:
        conn.close()

@router.post("/override/toestel")
def override_planning_toestel_endpoint(payload: PlanningToestelOverrideRequest):
    conn = get_db_connection()
    try:
        result = override_planning_toestel(
            conn=conn,
            planning_id=payload.planning_id,
            toestel_override=payload.toestel_override,
            planning_run_id=payload.planning_run_id,
        )
        return {"success": True, "result": result}
    finally:
        conn.close()

@router.post("/override/reorder")
def reorder_planning_task_endpoint(payload: PlanningReorderRequest):
    conn = get_db_connection()
    try:
        result = reorder_planning_task(
            conn=conn,
            planning_id=payload.planning_id,
            move_after_planning_id=payload.move_after_planning_id,
            planning_run_id=payload.planning_run_id,
        )

        try:
            assert_task_not_dependency_blocked(
                conn=conn,
                planning_run_id=payload.planning_run_id,
                planning_id=payload.planning_id,
            )
        except HTTPException:
            reset_planning_override(
                conn=conn,
                planning_id=payload.planning_id,
                planning_run_id=payload.planning_run_id,
            )
            raise

        return {"success": True, "result": result}
    finally:
        conn.close()


@router.post("/lock")
def lock_planning_task_endpoint(payload: PlanningLockRequest):
    conn = get_db_connection()
    try:
        result = lock_planning_task(
            conn=conn,
            planning_id=payload.planning_id,
            locked=payload.locked,
            planning_run_id=payload.planning_run_id,
        )
        return {"success": True, "result": result}
    finally:
        conn.close()


@router.post("/reset")
def reset_planning_override_endpoint(payload: PlanningResetRequest):
    conn = get_db_connection()
    try:
        result = reset_planning_override(
            conn=conn,
            planning_id=payload.planning_id,
            planning_run_id=payload.planning_run_id,
        )
        return {"success": True, "result": result}
    finally:
        conn.close()


@router.post("/set-lock")
def set_lock(payload: dict):
    conn = get_db_connection()
    try:
        set_task_lock(
            conn=conn,
            planning_id=payload["planning_id"],
            locked=payload["locked"],
            planning_run_id=payload.get("planning_run_id"),
        )
        return {"success": True}
    finally:
        conn.close()


@router.post("/clear-override")
def clear_override(payload: dict):
    conn = get_db_connection()
    try:
        clear_task_override(
            conn=conn,
            planning_id=payload["planning_id"],
            planning_run_id=payload.get("planning_run_id"),
        )
        return {"success": True}
    finally:
        conn.close()


@router.post("/admin/cleanup-handelingen")
def cleanup_handelingen():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            DELETE FROM handelingen
            WHERE code NOT LIKE '%_[0-9]'
            """
        )
        conn.commit()

        return {"success": True, "message": "Cleanup uitgevoerd"}
    finally:
        conn.close()


@router.post("/admin/reset-recipes")
def reset_recipes():
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM stappen")
        conn.execute("DELETE FROM handelingen")
        conn.execute("DELETE FROM recepten")
        conn.commit()

        return {
            "success": True,
            "message": "Alle recepten, handelingen en stappen zijn verwijderd",
        }
    finally:
        conn.close()

@router.post("/test-v2")
def test_planner_v2(payload: PlanningRequest):
    conn = get_db_connection()

    try:
        planning_df = build_planning_v2_df(
            conn=conn,
            start_monday=payload.start_monday,
            start_week=payload.start_week,
            cycles=payload.cycles,
            menu_groep=getattr(payload, "menu_groep", None),
        )

        if "Start" in planning_df.columns:
            planning_df["Start"] = planning_df["Start"].astype(str)

        if "Einde" in planning_df.columns:
            planning_df["Einde"] = planning_df["Einde"].astype(str)

        planning_df = planning_df.astype(object)
        planning_df = planning_df.where(planning_df.notna(), None)

        return {
            "planner": "v2-test",
            "row_count": len(planning_df),
            "rows": planning_df.to_dict(orient="records"),
        }

    finally:
        conn.close()

@router.post("/compare-v1-v2")
def compare_planner_v1_v2(payload: PlanningRequest):
    conn = get_db_connection()

    try:
        v1_df = build_planning_df(
            conn=conn,
            start_monday=payload.start_monday,
            start_week=payload.start_week,
            cycles=payload.cycles,
            menu_groep=getattr(payload, "menu_groep", None),
        )

        v2_df = build_planning_v2_df(
            conn=conn,
            start_monday=payload.start_monday,
            start_week=payload.start_week,
            cycles=payload.cycles,
            menu_groep=getattr(payload, "menu_groep", None),
        )

        v1_rows = _normalize_planning_df_for_compare(v1_df)
        v2_rows = _normalize_planning_df_for_compare(v2_df)

        comparison = _compare_planning_rows(v1_rows, v2_rows)

        return {
            "planner": "compare-v1-v2",
            **comparison,
        }

    finally:
        conn.close()

@router.get("/export/{planning_run_id}")
def export_planning_run(
    planning_run_id: int,
    werkdag: str = Query(...),
):
    conn = get_db_connection()

    try:
        excel_file = export_planning_run_to_excel(
            conn=conn,
            planning_run_id=planning_run_id,
            werkdag=werkdag,
        )

        filename = f"planning_{werkdag}.xlsx"

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    finally:
        conn.close()

@router.post("/export-day")
def export_visible_day(payload: dict):
    werkdag = payload.get("werkdag")
    rows = payload.get("rows") or []

    if not werkdag:
        raise HTTPException(status_code=400, detail="werkdag ontbreekt")

    excel_file = export_visible_day_rows_to_excel(
        rows=rows,
        werkdag=werkdag,
    )

    filename = f"planning_{werkdag}.xlsx"

    return StreamingResponse(
        excel_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )

@router.post("/run-v3")
def run_planning_v3(request: PlanningRequest):
    """
    Planner V3

    Deze endpoint is volledig onafhankelijk van Planner V1.

    V3 bevindt zich momenteel nog in ontwikkeling en gebruikt
    een nieuwe architectuur gebaseerd op:

    - Operating Model
    - Production Planning
    - Work Packages
    - Planner Rule Book

    V1 blijft voorlopig de productieplanner.
    """

    df = build_planning_v3_df(
        start_monday=request.start_monday,
        start_week=request.start_week,
        cycles=request.cycles,
        menu_rotation=getattr(request, "menu_rotation", None),
        explain=getattr(request, "explain", True),
        overrides=getattr(request, "overrides", []),
    )

    return {
        "planner_version": "V3",
        "rows": len(df),
        "debug": df.attrs.get("debug", {}),
        "planning": df.to_dict(orient="records"),
    }