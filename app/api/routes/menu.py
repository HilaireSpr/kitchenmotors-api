from fastapi import APIRouter, HTTPException, Body

from app.db import get_db_connection
from app.schemas.menu import (
    MenuGenerateRequest,
    MenuItemCreateRequest,
    MenuItemDeleteRequest,
    MenuItemUpdateRequest,
    MenuOverrideRequest,
    MenuReplaceOverrideRequest,
    MenuSelectionRequest,
)
from app.services.menu_service import (
    create_menu_item,
    create_menu_override,
    delete_menu_item,
    export_menu_group,
    import_menu_group,
    generate_menu_for_selection,
    get_menu_items,
    get_recept_selectie,
    replace_menu_override,
    save_recept_selectie,
    update_menu_item,
)

router = APIRouter()


@router.get("/recipes")
def get_menu_recipes():
    conn = get_db_connection()
    try:
        rows = get_recept_selectie(conn)
        return {"success": True, "result": rows}
    finally:
        conn.close()


@router.post("/selection")
def save_menu_selection(payload: MenuSelectionRequest):
    conn = get_db_connection()
    try:
        save_recept_selectie(conn, payload.selectie_ids)
        return {"success": True}
    finally:
        conn.close()


@router.post("/generate")
def generate_menu_endpoint(payload: MenuGenerateRequest):
    conn = get_db_connection()
    try:
        generate_menu_for_selection(
            conn=conn,
            start_monday=payload.start_monday,
            start_week=payload.start_week,
            cycles=payload.cycles,
        )
        return {"success": True}
    finally:
        conn.close()


@router.get("/items")
def get_menu_items_endpoint():
    conn = get_db_connection()
    try:
        rows = get_menu_items(conn)
        return {"success": True, "result": rows}
    finally:
        conn.close()

@router.get("/export/{menu_groep}")
def export_menu_group_endpoint(menu_groep: str):
    conn = get_db_connection()
    try:
        result = export_menu_group(conn, menu_groep)

        if not result.get("menu_items"):
            raise HTTPException(status_code=404, detail="Menu-groep niet gevonden of leeg")

        return {"success": True, "result": result}
    finally:
        conn.close()


@router.post("/import")
def import_menu_group_endpoint(payload: dict = Body(...)):
    conn = get_db_connection()
    try:
        result = import_menu_group(
            conn=conn,
            payload=payload.get("export") or payload,
            target_menu_groep=payload.get("target_menu_groep"),
        )

        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

@router.post("/items")
def create_menu_item_endpoint(payload: MenuItemCreateRequest):
    conn = get_db_connection()
    try:
        menu_item_id = create_menu_item(
            conn=conn,
            recept_id=payload.recept_id,
            serveerdag=payload.serveerdag,
            cyclus_week=payload.cyclus_week,
            cyclus_dag=payload.cyclus_dag,
            menu_groep=payload.menu_groep,
            ritme_type=payload.ritme_type,
            ritme_interval_weken=payload.ritme_interval_weken,
            bron=payload.bron,
            prognose_aantal=payload.prognose_aantal,
            periode_naam=payload.periode_naam,
            is_exception=payload.is_exception,
            opmerking=payload.opmerking,
        )
        return {"success": True, "result": {"id": menu_item_id}}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@router.post("/override")
def create_menu_override_endpoint(payload: MenuOverrideRequest):
    conn = get_db_connection()
    try:
        menu_item_id = create_menu_override(
            conn=conn,
            serveerdag=payload.serveerdag,
            recept_id=payload.recept_id,
            menu_groep=payload.menu_groep,
            prognose_aantal=payload.prognose_aantal,
            opmerking=payload.opmerking,
            cyclus_week=payload.cyclus_week,
            cyclus_dag=payload.cyclus_dag,
        )
        return {"success": True, "result": {"id": menu_item_id}}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

@router.post("/override/replace")
def replace_menu_override_endpoint(payload: MenuReplaceOverrideRequest):
    conn = get_db_connection()
    try:
        result = replace_menu_override(
            conn=conn,
            serveerdag=payload.serveerdag,
            recept_id=payload.recept_id,
            menu_groep=payload.menu_groep,
            prognose_aantal=payload.prognose_aantal,
            override_reason=payload.override_reason,
        )
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

@router.put("/items/{menu_item_id}")
def update_menu_item_endpoint(menu_item_id: int, payload: MenuItemUpdateRequest):
    conn = get_db_connection()
    try:
        update_menu_item(
            conn=conn,
            menu_item_id=menu_item_id,
            serveerdag=payload.serveerdag,
            cyclus_week=payload.cyclus_week,
            cyclus_dag=payload.cyclus_dag,
            menu_groep=payload.menu_groep,
            ritme_type=payload.ritme_type,
            ritme_interval_weken=payload.ritme_interval_weken,
            prognose_aantal=payload.prognose_aantal,
            periode_naam=payload.periode_naam,
            is_exception=payload.is_exception,
            opmerking=payload.opmerking,
        )
        return {"success": True, "result": {"id": menu_item_id}}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

@router.delete("/items")
def delete_menu_item_endpoint(payload: MenuItemDeleteRequest):
    conn = get_db_connection()
    try:
        delete_menu_item(conn, payload.menu_item_id)
        return {"success": True}
    finally:
        conn.close()