from pydantic import BaseModel
from typing import Optional
from datetime import date


# =========================================================
# RECIPES
# =========================================================
class RecipeBase(BaseModel):
    code: Optional[str] = None
    naam: str
    categorie: Optional[str] = None
    menu_groep: Optional[str] = None
    actief: Optional[bool] = True


class RecipeCreate(RecipeBase):
    pass


class RecipeUpdate(BaseModel):
    code: Optional[str] = None
    naam: Optional[str] = None
    categorie: Optional[str] = None
    menu_groep: Optional[str] = None
    actief: Optional[bool] = None


class RecipeResponse(RecipeBase):
    id: int

    class Config:
        from_attributes = True


# =========================================================
# HANDELINGEN (🔥 BELANGRIJK VOOR PLANNER)
# =========================================================
class HandelingUpdateRequest(BaseModel):
    naam: str
    post: Optional[str] = None
    toestel: Optional[str] = None

    dag_offset: int
    passieve_tijd: Optional[int] = 0

    is_vaste_taak: Optional[bool] = False

    # bestaande velden (die je al gebruikt in planning.py)
    min_offset_dagen: Optional[int] = None
    max_offset_dagen: Optional[int] = None

    heeft_vast_startuur: Optional[bool] = False
    vast_startuur: Optional[str] = None  # "HH:MM"

    # 🔥 NIEUWE VELDEN (jouw upgrade)
    planning_type: Optional[str] = "floating"   # hard | soft | floating
    actief_vanaf: Optional[date] = None
    actief_tot: Optional[date] = None


# =========================================================
# STAPPEN
# =========================================================
class StapUpdateRequest(BaseModel):
    naam: str
    tijd: int