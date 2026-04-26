from pydantic import BaseModel


class MenuPeriodeCreateRequest(BaseModel):
    naam: str
    menu_groep: str
    startdatum: str
    einddatum: str
    rotatielengte_weken: int = 1
    startweek_in_cyclus: int = 1
    default_prognose_aantal: float | None = None
    actief: int = 1


class MenuPeriodeUpdateRequest(BaseModel):
    naam: str
    menu_groep: str
    startdatum: str
    einddatum: str
    rotatielengte_weken: int = 1
    startweek_in_cyclus: int = 1
    default_prognose_aantal: float | None = None
    actief: int = 1


class MenuPeriodeDeleteRequest(BaseModel):
    periode_id: int

class MenuPeriodeGenerateToMenuRequest(BaseModel):
    periode_id: int
    clear_existing_generated: bool = True