"""
KitchenMotors Planner V3 - Context

Verantwoordelijk voor:
- datum normaliseren
- PlanningContextV3 bouwen

Geen productie-opbouw.
Geen scheduling.
Geen dataframe.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.services.planning_v3_models import PlanningContextV3
from app.services.planning_v3_rules import RULE_001_PRODUCTION_BEFORE_EXECUTION


def normalize_date(value: Any) -> date:
    """
    Zet inkomende datumwaarden veilig om naar date.
    """

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        return date.fromisoformat(value)

    raise ValueError(f"Ongeldige datumwaarde voor Planner V3: {value!r}")


def build_planning_context(
    start_monday: date,
    start_week: int,
    cycles: int,
    menu_rotation: Optional[str],
    overrides: List[Dict[str, Any]],
) -> PlanningContextV3:
    """
    Bouwt alle inputcontext voor Planner V3.
    """

    return PlanningContextV3(
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
        menu_rotation=menu_rotation,
        raw_data={
            "overrides": overrides,
        },
        rules_applied=[
            RULE_001_PRODUCTION_BEFORE_EXECUTION,
        ],
    )