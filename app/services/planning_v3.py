"""
KitchenMotors Planner V3

Planner V3 is gebouwd volgens het Operating Model:

1. Eerst productie bepalen
2. Daarna uitvoering plannen
3. Iedere beslissing moet later koppelbaar zijn aan een Planner Rule
4. Planner V1 blijft behouden als fallback

Dit bestand bevat bewust eerst de architectuur-skeleton.
De echte plannerlogica wordt stap voor stap toegevoegd.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from app.services.planning_v3_scheduler import schedule_work_packages
from app.services.planning_v3_sequence import sequence_work_packages
from app.services.planning_v3_context import (
    build_planning_context,
    normalize_date,
)

from app.services.planning_v3_production import (
    build_production_plan,
    build_work_packages,
)

from app.services.planning_v3_dataframe import (
    build_planning_dataframe,
)

# ============================================================
# Public entrypoint
# ============================================================

def build_planning_v3_df(
    start_monday: date,
    start_week: int = 1,
    cycles: int = 1,
    menu_rotation: Optional[str] = None,
    explain: bool = True,
    overrides: Optional[List[Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    Hoofdfunctie voor Planner V3.

    Deze functie vervangt Planner V1 nog NIET.
    Ze bouwt voorlopig een veilige, lege V3-planning op.

    Later wordt deze functie gekoppeld aan een aparte V3-route.
    """
    start_monday = normalize_date(start_monday)

    context = build_planning_context(
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
        menu_rotation=menu_rotation,
        overrides=overrides or [],
    )

    production_plan = build_production_plan(context)
    work_packages = build_work_packages(context, production_plan)
    context.debug["work_packages_count"] = len(work_packages)

    package_stream_counts: Dict[str, int] = {}

    for package in work_packages:
        package_stream_counts[package.productiestroom] = (
            package_stream_counts.get(package.productiestroom, 0) + 1
        )

    context.debug["work_package_streams"] = package_stream_counts
    sequenced_packages = sequence_work_packages(context, work_packages)

    context.debug["sequenced_packages_count"] = len(sequenced_packages)
    context.debug["first_20_sequence"] = [
        {
            "package_id": package.package_id,
            "productiestroom": package.productiestroom,
            "werkdag": package.werkdag.isoformat(),
            "deadline": package.deadline.strftime("%H:%M") if package.deadline else None,
            "items": len(package.items),
        }
        for package in sequenced_packages[:20]
    ]

    scheduled_packages = schedule_work_packages(context, sequenced_packages)

    df = build_planning_dataframe(
        context=context,
        scheduled_packages=scheduled_packages,
        explain=explain,
    )

    df.attrs["debug"] = context.debug

    return df

