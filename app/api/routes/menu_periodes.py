from fastapi import APIRouter, HTTPException, Query

from app.db import get_db_connection
from app.schemas.menu_periodes import (
    MenuPeriodeCreateRequest,
    MenuPeriodeDeleteRequest,
    MenuPeriodeGenerateToMenuRequest,
    MenuPeriodeUpdateRequest,
)
from app.services.menu_periodes_service import (
    create_menu_periode,
    delete_menu_periode,
    generate_menu_from_periode,
    get_menu_periodes,
    update_menu_periode,
)

router = APIRouter()


@router.get("")
def get_menu_periodes_endpoint():
    conn = get_db_connection()
    try:
        rows = get_menu_periodes(conn)
        return {"success": True, "result": rows}
    finally:
        conn.close()


@router.post("")
def create_menu_periode_endpoint(payload: MenuPeriodeCreateRequest):
    conn = get_db_connection()
    try:
        periode_id = create_menu_periode(
            conn=conn,
            naam=payload.naam,
            menu_groep=payload.menu_groep,
            startdatum=payload.startdatum,
            einddatum=payload.einddatum,
            rotatielengte_weken=payload.rotatielengte_weken,
            startweek_in_cyclus=payload.startweek_in_cyclus,
            default_prognose_aantal=payload.default_prognose_aantal,
            actief=payload.actief,
        )
        return {"success": True, "result": {"id": periode_id}}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@router.put("")
def update_menu_periode_endpoint(
    payload: MenuPeriodeUpdateRequest,
    periode_id: int = Query(...),
):
    conn = get_db_connection()
    try:
        update_menu_periode(
            conn=conn,
            periode_id=periode_id,
            naam=payload.naam,
            menu_groep=payload.menu_groep,
            startdatum=payload.startdatum,
            einddatum=payload.einddatum,
            rotatielengte_weken=payload.rotatielengte_weken,
            startweek_in_cyclus=payload.startweek_in_cyclus,
            default_prognose_aantal=payload.default_prognose_aantal,
            actief=payload.actief,
        )
        return {"success": True}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@router.delete("")
def delete_menu_periode_endpoint(payload: MenuPeriodeDeleteRequest):
    conn = get_db_connection()
    try:
        delete_menu_periode(conn, payload.periode_id)
        return {"success": True}
    finally:
        conn.close()

@router.post("/generate-menu")
def generate_menu_from_periode_endpoint(payload: MenuPeriodeGenerateToMenuRequest):
    conn = get_db_connection()
    try:
        result = generate_menu_from_periode(
            conn=conn,
            periode_id=payload.periode_id,
            clear_existing_generated=payload.clear_existing_generated,
        )
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()