"""
KitchenMotors Planner V3 - Sequence Engine

Verantwoordelijk voor:
- volgorde van work packages
- deadlines vóór gewone flow
- productiestroomprioriteit
"""

from __future__ import annotations

from datetime import time
from typing import List

from app.services.planning_v3_models import (
    PlanningContextV3,
    WorkPackageV3,
)
from app.services.planning_v3_rules import (
    RULE_021_DEADLINES_BEFORE_FLOW,
)


def sequence_work_packages(
    context: PlanningContextV3,
    work_packages: List[WorkPackageV3],
) -> List[WorkPackageV3]:
    """
    Bepaalt de volgorde van werkpakketten.

    Regel 021:
    Harde deadlines en vaste starturen krijgen voorrang binnen de werkdag.
    """

    stream_priority = {
        "PAT": 10,
        "FOOD": 20,
        "SOEP": 30,
        "REF": 40,
    }

    def sort_key(package: WorkPackageV3):
        has_deadline = package.deadline is not None

        return (
            package.werkdag,
            0 if has_deadline else 1,
            package.deadline or time(23, 59),
            stream_priority.get(package.productiestroom, 999),
            package.package_id,
        )

    sequenced = sorted(work_packages, key=sort_key)

    for package in sequenced:
        if package.deadline is not None:
            package.rule_trace.append(RULE_021_DEADLINES_BEFORE_FLOW)

    return sequenced