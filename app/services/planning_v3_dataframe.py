"""
KitchenMotors Planner V3 - DataFrame Builder

Verantwoordelijk voor:

- omzetting naar DataFrame
- Swagger output
- explainability

Geen scheduling.
Geen sequencing.
Geen productie-opbouw.
"""

from __future__ import annotations

import pandas as pd

from app.services.planning_v3_models import (
    PlanningContextV3,
    ScheduledWorkPackageV3,
)
from app.services.planning_v3_production import (
    build_planning_id_v3,
)


def build_planning_dataframe(
    context: PlanningContextV3,
    scheduled_packages: list[ScheduledWorkPackageV3],
    explain: bool = True,
) -> pd.DataFrame:

    rows = []

    for scheduled in scheduled_packages:

        for item in scheduled.package.items:

            rows.append(
                {
                    "Planner versie": "V3",
                    "Planning ID": build_planning_id_v3(item),
                    "Recept ID": item.recept_id,
                    "Handeling ID": item.handeling_id,
                    "Recept": item.recept_naam,
                    "Taak": item.handeling_naam,
                    "Productiestroom": item.productiestroom,
                    "Post": scheduled.post,
                    "Toestel": scheduled.toestel or "Geen",
                    "Serveerdag": item.serveerdag.isoformat(),
                    "Werkdag": item.werkdag.isoformat(),
                    "Werkdag_iso": item.werkdag.isoformat(),
                    "Start": scheduled.start.strftime("%H:%M"),
                    "Einde": scheduled.einde.strftime("%H:%M"),
                    "Planner reden": (
                        " | ".join(scheduled.rule_trace)
                        if explain
                        else ""
                    ),
                }
            )

    return pd.DataFrame(rows)