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