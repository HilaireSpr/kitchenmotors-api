from pydantic import BaseModel


class MenuSelectionRequest(BaseModel):
    selectie_ids: list[int]


class MenuGenerateRequest(BaseModel):
    start_monday: str
    start_week: int
    cycles: int


class MenuItemCreateRequest(BaseModel):
    recept_id: int
    serveerdag: str
    cyclus_week: int | None = None
    cyclus_dag: int | None = None
    menu_groep: str | None = None
    ritme_type: str | None = None
    ritme_interval_weken: int | None = None
    bron: str | None = "manual"
    prognose_aantal: float | None = None
    periode_naam: str | None = None
    is_exception: int | None = 0
    opmerking: str | None = None

class MenuItemUpdateRequest(BaseModel):
    serveerdag: str
    cyclus_week: int | None = None
    cyclus_dag: int | None = None
    menu_groep: str | None = None
    ritme_type: str | None = None
    ritme_interval_weken: int | None = None
    prognose_aantal: float | None = None
    periode_naam: str | None = None
    is_exception: int | None = 0
    opmerking: str | None = None

class MenuItemDeleteRequest(BaseModel):
    menu_item_id: int


class MenuOverrideRequest(BaseModel):
    serveerdag: str
    recept_id: int
    menu_groep: str | None = None
    prognose_aantal: float | None = None
    opmerking: str | None = None
    cyclus_week: int | None = None
    cyclus_dag: int | None = None

class MenuReplaceOverrideRequest(BaseModel):
    serveerdag: str
    recept_id: int
    menu_groep: str | None = None
    prognose_aantal: float | None = None
    override_reason: str | None = None