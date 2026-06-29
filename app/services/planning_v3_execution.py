"""
KitchenMotors Planner V3 - Execution Layer

Verantwoordelijk voor:

- Execution Lanes
- Capaciteitsberekening
- Lane-opbouw

Nog niet:

- Scheduling
- Toestellen
- Pauzes
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Dict, List

from app.services.planning_v3_models import (
    ExecutionLaneV3,
    PlanningContextV3,
    WorkPackageV3,
)


def calculate_capacity_minutes(
    startuur: time,
    einduur: time,
) -> int:
    """
    Berekent de beschikbare capaciteit van een post.
    """

    start = datetime.combine(date.today(), startuur)
    einde = datetime.combine(date.today(), einduur)

    return int((einde - start).total_seconds() / 60)


def build_execution_lanes(
    context: PlanningContextV3,
    work_packages: List[WorkPackageV3],
) -> Dict[tuple[str, date], ExecutionLaneV3]:
    """
    Bouwt alle execution lanes.

    De horizon wordt afgeleid uit de werkelijke werkdagen
    van de work packages.
    """

    lanes: Dict[tuple[str, date], ExecutionLaneV3] = {}

    if not work_packages:
        return lanes

    eerste_dag = min(package.werkdag for package in work_packages)
    laatste_dag = max(package.werkdag for package in work_packages)

    aantal_dagen = (laatste_dag - eerste_dag).days + 1

    default_config = {
        "PAT": (time(6, 30), time(14, 30)),
        "FOOD": (time(6, 0), time(14, 0)),
        "SOEP": (time(7, 30), time(15, 30)),
        "REF": (time(6, 0), time(14, 0)),
    }

    for dag in range(aantal_dagen):

        werkdag = eerste_dag + timedelta(days=dag)

        for post, (startuur, einduur) in default_config.items():

            lane = ExecutionLaneV3(
                post=post,
                werkdag=werkdag,
                startuur=startuur,
                einduur=einduur,
                capaciteit_minuten=calculate_capacity_minutes(
                    startuur,
                    einduur,
                ),
                next_available=datetime.combine(
                    werkdag,
                    startuur,
                ),
            )

            lanes[(post, werkdag)] = lane

    context.debug["execution_horizon"] = {
        "eerste_dag": eerste_dag.isoformat(),
        "laatste_dag": laatste_dag.isoformat(),
        "aantal_dagen": aantal_dagen,
    }

    return lanes