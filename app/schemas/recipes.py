from datetime import date
from typing import Optional

from pydantic import BaseModel


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


class RecipeCreateRequest(BaseModel):
    code: str
    naam: str
    categorie: Optional[str] = None
    menu_groep: Optional[str] = None


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
# HANDELINGEN
# =========================================================
class HandelingBase(BaseModel):
    code: str
    naam: str
    post: Optional[str] = None
    toestel: Optional[str] = None

    dag_offset: int = 0
    min_offset_dagen: Optional[int] = 0
    max_offset_dagen: Optional[int] = 0

    passieve_tijd: Optional[int] = 0

    is_vaste_taak: Optional[bool] = False
    heeft_vast_startuur: Optional[bool] = False
    vast_startuur: Optional[str] = None

    planning_type: Optional[str] = "floating"
    actief_vanaf: Optional[date] = None
    actief_tot: Optional[date] = None


class HandelingCreateRequest(HandelingBase):
    pass


class HandelingUpdateRequest(BaseModel):
    naam: str
    post: Optional[str] = None
    toestel: Optional[str] = None

    dag_offset: int
    min_offset_dagen: Optional[int] = None
    max_offset_dagen: Optional[int] = None

    passieve_tijd: Optional[int] = 0

    is_vaste_taak: Optional[bool] = False
    heeft_vast_startuur: Optional[bool] = False
    vast_startuur: Optional[str] = None

    planning_type: Optional[str] = "floating"
    actief_vanaf: Optional[date] = None
    actief_tot: Optional[date] = None


# =========================================================
# STAPPEN
# =========================================================
class StapCreateRequest(BaseModel):
    naam: str
    tijd: int = 0


class StapUpdateRequest(BaseModel):
    naam: str
    tijd: int