from fastapi import APIRouter, HTTPException

from app.db import get_db_connection
from app.schemas.recipes import HandelingUpdateRequest
from app.services.recipes_service import (
    get_recipes,
    get_recipe_detail,
    update_handeling,
)
from app.schemas.recipes import StapUpdateRequest
from app.services.recipes_service import update_stap

router = APIRouter()


@router.get("")
def list_recipes():
    conn = get_db_connection()
    try:
        rows = get_recipes(conn)
        return {"success": True, "result": rows}
    finally:
        conn.close()


@router.get("/{recept_code}")
def get_recipe_detail_endpoint(recept_code: str):
    conn = get_db_connection()
    try:
        result = get_recipe_detail(conn, recept_code)

        if not result:
            raise HTTPException(status_code=404, detail="Recept niet gevonden")

        return {"success": True, "result": result}
    finally:
        conn.close()

@router.put("/handelingen/{handeling_id}")
def update_handeling_endpoint(handeling_id: int, payload: HandelingUpdateRequest):
    conn = get_db_connection()
    try:
        result = update_handeling(
            conn=conn,
            handeling_id=handeling_id,
            naam=payload.naam,
            post=payload.post,
            toestel=payload.toestel,
            dag_offset=payload.dag_offset,
            dag_offset_min=payload.min_offset_dagen,
            dag_offset_max=payload.max_offset_dagen,
            passieve_tijd=payload.passieve_tijd,
            is_vaste_taak=payload.is_vaste_taak,
        )

        if not result:
            raise HTTPException(status_code=404, detail="Handeling niet gevonden")

        return {"success": True, "result": result}
    finally:
        conn.close()

@router.put("/stappen/{stap_id}")
def update_stap_endpoint(stap_id: int, payload: StapUpdateRequest):
    conn = get_db_connection()
    try:
        result = update_stap(
            conn=conn,
            stap_id=stap_id,
            naam=payload.naam,
            tijd=payload.tijd,
        )

        if not result:
            raise HTTPException(status_code=404, detail="Stap niet gevonden")

        return {"success": True, "result": result}
    finally:
        conn.close()

@router.delete("/{recept_id}")
def delete_recipe_endpoint(recept_id: int):
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT id, naam FROM recepten WHERE id = ?",
            (recept_id,),
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Recept niet gevonden")

        conn.execute(
            """
            DELETE FROM stappen
            WHERE handeling_id IN (
                SELECT id FROM handelingen WHERE recept_id = ?
            )
            """,
            (recept_id,),
        )

        conn.execute(
            "DELETE FROM handelingen WHERE recept_id = ?",
            (recept_id,),
        )

        conn.execute(
            "DELETE FROM menu WHERE recept_id = ?",
            (recept_id,),
        )

        conn.execute(
            "DELETE FROM menu_recept_selectie WHERE recept_id = ?",
            (recept_id,),
        )

        conn.execute(
            "DELETE FROM planning_templates WHERE recept_id = ?",
            (recept_id,),
        )

        conn.execute(
            "DELETE FROM recepten WHERE id = ?",
            (recept_id,),
        )

        conn.commit()

        return {
            "success": True,
            "message": "Recept verwijderd",
        }
    finally:
        conn.close()

@router.post("/admin/reset-recipes")
def reset_recipes():
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM stappen")
        conn.execute("DELETE FROM handelingen")
        conn.execute("DELETE FROM menu")
        conn.execute("DELETE FROM menu_recept_selectie")
        conn.execute("DELETE FROM planning_templates")
        conn.execute("DELETE FROM recepten")
        conn.commit()
        return {"success": True}
    finally:
        conn.close()