import json

from fastapi import APIRouter

from app.db import get_db_connection
from app.schemas.planning import (
    PlanningLockRequest,
    PlanningMoveOverrideRequest,
    PlanningPostOverrideRequest,
    PlanningRequest,
    PlanningResetRequest,
    PlanningReorderRequest,
)
from app.services.planner_service import (
    lock_planning_task,
    move_planning_task,
    override_planning_post,
    run_planner,
    reset_planning_override,
    reorder_planning_task,
)
from app.services.planning_overrides import (
    clear_task_override,
    set_task_lock,
)
from app.services.planning_storage import (
    delete_planning_run,
    get_planning_runs,
    load_planning_df,
    set_active_planning_run,
)

router = APIRouter()


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

        json_data = df.to_json(orient="records", date_format="iso")

        return {
            "success": True,
            "result": {"rows": json.loads(json_data)},
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
        )
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
        )
        return {"success": True, "result": result}
    finally:
        conn.close()


@router.post("/set-lock")
def set_lock(payload: dict):
    conn = get_db_connection()
    try:
        set_task_lock(
            conn,
            planning_id=payload["planning_id"],
            locked=payload["locked"],
        )
        return {"success": True}
    finally:
        conn.close()


@router.post("/clear-override")
def clear_override(payload: dict):
    conn = get_db_connection()
    try:
        clear_task_override(
            conn,
            planning_id=payload["planning_id"],
        )
        return {"success": True}
    finally:
        conn.close()

@router.post("/admin/cleanup-handelingen")
def cleanup_handelingen():
    conn = get_db_connection()
    try:
        conn.execute("""
        DELETE FROM handelingen
        WHERE code NOT LIKE '%_[0-9]'
        """)
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