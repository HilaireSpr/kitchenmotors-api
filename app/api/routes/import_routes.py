from fastapi import APIRouter, UploadFile, File, HTTPException

from app.db import get_db_connection
from app.services.import_service import import_excel_to_database

router = APIRouter()


@router.post("/recipes-excel")
async def import_recipes_excel(
    file: UploadFile = File(...),
    overwrite_existing: bool = False,
):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Alleen .xlsx bestanden toegestaan")

    content = await file.read()

    conn = get_db_connection()

    try:
        result = import_excel_to_database(
            conn,
            content,
            overwrite_existing=overwrite_existing,
        )
        return {"success": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()