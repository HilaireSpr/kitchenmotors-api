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

router = APIRouter()


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
def reorder_planning(request: PlanningReorderRequest):
    try:
        from app.services.planning_overrides import set_reorder_override

        set_reorder_override(
            planning_id=request.planning_id,
            move_after_planning_id=request.move_after_planning_id,
        )

        return {"success": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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