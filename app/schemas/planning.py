from typing import Literal, Optional

from pydantic import BaseModel, Field

class PlanningOverride(BaseModel):
    planningId: str
    locked: bool = True
    post: Optional[str] = None
    toestel: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None


class MenuRotationContext(BaseModel):
    menu_type: Literal["patient", "staff"]
    rotation_length: int
    week_in_cycle: Optional[int] = None


class PlanningRequest(BaseModel):
    start_monday: str
    start_week: int
    cycles: int = 1
    end_date: Optional[str] = None
    planning_naam: Optional[str] = None
    explain: bool = False
    overrides: list[PlanningOverride] = Field(default_factory=list)
    menu_rotation: Optional[MenuRotationContext] = None
    menu_groep: Optional[str] = None


class PlanningMoveOverrideRequest(BaseModel):
    planning_id: str
    werkdag_override: str
    planning_run_id: Optional[int] = None


class PlanningPostOverrideRequest(BaseModel):
    planning_id: str
    post_override: str
    planning_run_id: Optional[int] = None


class PlanningLockRequest(BaseModel):
    planning_id: str
    locked: bool
    planning_run_id: Optional[int] = None


class PlanningResetRequest(BaseModel):
    planning_id: str
    planning_run_id: Optional[int] = None


class PlanningStartuurUpdateRequest(BaseModel):
    werkdag: str
    post: str
    starttijd: str
    
class PlanningReorderRequest(BaseModel):
    planning_id: str
    move_after_planning_id: str
    planning_run_id: Optional[int] = None