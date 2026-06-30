"""
KitchenMotors Planner V3 - Models

Dit bestand bevat alleen de dataclasses van Planner V3.

Geen planninglogica.
Geen databasequeries.
Geen scheduling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional


@dataclass
class PlanningContextV3:
    start_monday: date
    start_week: int
    cycles: int
    menu_rotation: Optional[str] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)
    rules_applied: List[str] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProductionItemV3:
    recept_id: Optional[int]
    handeling_id: Optional[int]
    recept_naam: str
    handeling_naam: str
    productiestroom: str
    serveerdag: date
    werkdag: date
    deadline: Optional[time] = None
    rule_trace: List[str] = field(default_factory=list)
    package_id: Optional[str] = None
    package_code: Optional[str] = None
    actieve_tijd: int = 0
    passieve_tijd: int = 0
    totale_duur: int = 0


@dataclass
class WorkPackageV3:
    package_id: str
    productiestroom: str
    werkdag: date
    items: List[ProductionItemV3]
    preferred_post: Optional[str] = None
    deadline: Optional[time] = None
    rule_trace: List[str] = field(default_factory=list)
    actieve_tijd: int = 0
    passieve_tijd: int = 0
    totale_duur: int = 0


@dataclass
class ExecutionLaneV3:
    """
    Digitale productiepost.
    Eén lane = één post op één werkdag.
    """

    post: str
    werkdag: date

    startuur: time
    einduur: time

    capaciteit_minuten: int

    next_available: datetime

    gebruikte_minuten: int = 0
    ingeplande_minuten: int = 0
    belasting_pct: float = 0.0

    packages: List[WorkPackageV3] = field(default_factory=list)

    blocks: List[ExecutionBlockV3] = field(default_factory=list)


@dataclass
class PlannerDecisionV3:
    rule: str
    title: str
    explanation: str

@dataclass
class ExecutionBlockV3:
    """
    Eén bezet of gereserveerd tijdsblok op een Execution Lane.
    """

    start: datetime
    einde: datetime

    block_type: str
    # WORK
    # BREAK
    # PASSIVE
    # RESERVED

    package_id: Optional[str] = None

    toestel: Optional[str] = None

    locked: bool = False

@dataclass
class FreeBlockV3:
    """
    Eén vrij tijdsblok op een Execution Lane.
    """

    start: datetime
    einde: datetime

    duur_minuten: int

@dataclass
class ScheduledWorkPackageV3:
    package: WorkPackageV3
    post: str
    start: datetime
    einde: datetime
    toestel: Optional[str] = None
    rule_trace: List[str] = field(default_factory=list)
    decisions: List[PlannerDecisionV3] = field(default_factory=list)