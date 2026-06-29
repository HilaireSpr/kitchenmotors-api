"""
KitchenMotors Planner V3 - Scheduler

Verantwoordelijk voor:

- Package durations
- Scheduling op Execution Lanes

Niet verantwoordelijk voor:

- Sequencing
- DataFrame
- Productie-opbouw
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List

from app.services.planning_v3_execution import build_execution_lanes
from app.services.planning_v3_models import (
    PlannerDecisionV3,
    PlanningContextV3,
    ScheduledWorkPackageV3,
    WorkPackageV3,
)
from app.services.planning_v3_rules import (
    RULE_031_BATCH_MOVES_AS_ONE,
    RULE_040_STREAMS_BEFORE_POSTS,
    RULE_070_EXECUTION_LANE,
    RULE_071_REAL_DURATION,
)


def estimate_package_duration_minutes(
    package: WorkPackageV3,
) -> int:
    """
    Geeft de echte package-duur terug.
    """

    if package.totale_duur > 0:
        return package.totale_duur

    return max(1, len(package.items) * 30)


def schedule_work_packages(
    context: PlanningContextV3,
    work_packages: List[WorkPackageV3],
) -> List[ScheduledWorkPackageV3]:
    """
    Plant werkpakketten op de execution lanes.
    """

    execution_lanes = build_execution_lanes(context, work_packages)

    context.debug["execution_lane_count"] = len(execution_lanes)

    scheduled: List[ScheduledWorkPackageV3] = []
    missing_lanes: List[Dict[str, Any]] = []

    for package in work_packages:

        post = package.preferred_post or package.productiestroom or "ONBEKEND"

        lane_key = (post, package.werkdag)

        lane = execution_lanes.get(lane_key)

        if lane is None:
            missing_lanes.append(
                {
                    "package_id": package.package_id,
                    "post": post,
                    "werkdag": package.werkdag.isoformat(),
                    "items": len(package.items),
                }
            )
            continue

        duration_minutes = estimate_package_duration_minutes(package)

        start = lane.next_available
        einde = start + timedelta(minutes=duration_minutes)

        lane.next_available = einde
        lane.gebruikte_minuten += duration_minutes
        lane.ingeplande_minuten += duration_minutes

        lane.belasting_pct = (
            round(
                lane.ingeplande_minuten
                / lane.capaciteit_minuten
                * 100,
                1,
            )
            if lane.capaciteit_minuten
            else 0.0
        )

        lane.packages.append(package)

        scheduled.append(
            ScheduledWorkPackageV3(
                package=package,
                post=lane.post,
                start=start,
                einde=einde,
                toestel=None,
                rule_trace=[
                    *package.rule_trace,
                    RULE_070_EXECUTION_LANE,
                    RULE_071_REAL_DURATION,
                ],
                decisions=[
                    PlannerDecisionV3(
                        rule=RULE_031_BATCH_MOVES_AS_ONE,
                        title="Batch samenhouden",
                        explanation="Alle taken binnen dit werkpakket blijven samen als één productie-eenheid.",
                    ),
                    PlannerDecisionV3(
                        rule=RULE_040_STREAMS_BEFORE_POSTS,
                        title="Productiestroom eerst",
                        explanation=f"Dit werkpakket behoort tot productiestroom {package.productiestroom}.",
                    ),
                    PlannerDecisionV3(
                        rule=RULE_070_EXECUTION_LANE,
                        title="Execution lane gekozen",
                        explanation=(
                            f"Het werkpakket is gepland op post {lane.post} "
                            f"op {lane.werkdag.isoformat()} vanaf {start.strftime('%H:%M')}."
                        ),
                    ),
                    PlannerDecisionV3(
                        rule=RULE_071_REAL_DURATION,
                        title="Echte duurtijd gebruikt",
                        explanation=(
                            f"De totale duur van dit pakket is {duration_minutes} minuten "
                            f"op basis van actieve en passieve tijd."
                        ),
                    ),
                ],
            )
        )

    context.debug["scheduled_packages_count"] = len(scheduled)
    context.debug["missing_lanes_count"] = len(missing_lanes)
    context.debug["missing_lanes_first_20"] = missing_lanes[:20]

    context.debug["execution_lane_load_first_20"] = [
        {
            "post": lane.post,
            "werkdag": lane.werkdag.isoformat(),
            "startuur": lane.startuur.strftime("%H:%M"),
            "einduur": lane.einduur.strftime("%H:%M"),
            "capaciteit_minuten": lane.capaciteit_minuten,
            "ingeplande_minuten": lane.ingeplande_minuten,
            "belasting_pct": lane.belasting_pct,
            "packages": len(lane.packages),
        }
        for lane in sorted(
            execution_lanes.values(),
            key=lambda item: (item.werkdag, item.post),
        )[:20]
    ]

    return scheduled