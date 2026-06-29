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

from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from app.db import get_db_connection
from app.services.production_engine import build_production_plan as build_shared_production_plan

from app.services.planning_v3_models import (
    PlanningContextV3,
    ProductionItemV3,
    WorkPackageV3,
)

from app.services.planning_v3_rules import (
    RULE_001_PRODUCTION_BEFORE_EXECUTION,
    RULE_031_BATCH_MOVES_AS_ONE,
    RULE_040_STREAMS_BEFORE_POSTS,
)

from app.services.planning_v3_scheduler import schedule_work_packages
from app.services.planning_v3_sequence import sequence_work_packages
from app.services.planning_v3_context import (
    build_planning_context,
    normalize_date,
)

from app.services.planning_v3_production import (
    build_planning_id_v3,
    build_production_plan,
    build_work_packages,
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
# ============================================================
# Layer 6 - DataFrame output
# ============================================================

def build_planning_dataframe(
    context: PlanningContextV3,
    scheduled_packages: List[ScheduledWorkPackageV3],
    explain: bool = True,
) -> pd.DataFrame:
    """
    Bouwt de Planner V3 output als DataFrame.

    Deze output is voorlopig minimaal.
    Later maken we ze compatibel met de bestaande frontendkolommen.
    """

    rows: List[Dict[str, Any]] = []

    for scheduled in scheduled_packages:
        package = scheduled.package

        for item in package.items:
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
                    "Planner reden": " | ".join(scheduled.rule_trace) if explain else "",
                    "Planner beslissingen": [
                        {
                            "rule": decision.rule,
                            "title": decision.title,
                            "explanation": decision.explanation,
                        }
                        for decision in scheduled.decisions
                    ] if explain else [],
                }
            )

    return pd.DataFrame(rows)


# ============================================================
# Helpers
# ============================================================

def estimate_work_package_duration_minutes(
    package: WorkPackageV3,
) -> int:
    """
    Tijdelijke inschatting.

    Later vervangen door:
    - som actieve tijd van handelingen/stappen
    - passieve tijd apart
    - toestelblokkering apart
    """

    return 30

def dict_factory(cursor, row):
    """
    SQLite row factory die rijen als dictionaries teruggeeft.
    """

    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def fetch_menu_rows_for_period(
    conn,
    context: PlanningContextV3,
) -> List[Dict[str, Any]]:
    """
    Haalt actieve menu-items op voor de gevraagde periode.

    Voorlopig houden we dit bewust eenvoudig:
    - alleen status active of lege status
    - serveerdag binnen periode
    - optioneel filter op menu_groep komt later
    """

    period_start = context.start_monday
    period_end = context.start_monday + timedelta(days=(context.cycles * 7) - 1)

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM menu
        WHERE date(serveerdag) BETWEEN date(?) AND date(?)
          AND (status IS NULL OR status = '' OR status = 'active')
        ORDER BY date(serveerdag), cyclus_week, cyclus_dag, id
        """,
        (
            period_start.isoformat(),
            period_end.isoformat(),
        ),
    )

    return cursor.fetchall()


def fetch_recept_by_id(
    conn,
    recept_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Haalt één recept op.
    """

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM recepten
        WHERE id = ?
        """,
        (recept_id,),
    )

    return cursor.fetchone()


def fetch_handelingen_for_recept(
    conn,
    recept_id: int,
) -> List[Dict[str, Any]]:
    """
    Haalt alle handelingen van een recept op in logische volgorde.
    """

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM handelingen
        WHERE recept_id = ?
        ORDER BY
            COALESCE(dag_offset, 0),
            COALESCE(sort_order, 9999),
            id
        """,
        (recept_id,),
    )

    return cursor.fetchall()


def determine_productiestroom_v3(
    menu_row: Dict[str, Any],
    recept: Dict[str, Any],
    handeling: Dict[str, Any],
) -> str:
    """
    Bepaalt de productiestroom.

    Planner V3 denkt eerst in stromen, niet in posten.

    Voorlopige volgorde:
    1. menu_groep uit menu
    2. menu_groep uit recept
    3. post van handeling
    4. ONBEKEND

    Later vervangen we dit door echte Productiestroom-regels.
    """

    value = (
        menu_row.get("menu_groep")
        or recept.get("menu_groep")
        or handeling.get("post")
        or "ONBEKEND"
    )

    return normalize_productiestroom(value)


def normalize_productiestroom(value: Any) -> str:
    """
    Normaliseert productiestroomnamen naar de bekende V3-stromen.
    """

    raw = str(value or "").strip().upper()

    mapping = {
        "AA9": "FOOD",
        "FOODBANK": "FOOD",
        "FOOD": "FOOD",
        "AD8": "PAT",
        "PAZO": "PAT",
        "PATIENTEN": "PAT",
        "PATIËNTEN": "PAT",
        "PAT": "PAT",
        "C8": "SOEP",
        "SOEP": "SOEP",
        "RAD8": "REF",
        "AD8R": "REF",
        "REFTER": "REF",
        "REF": "REF",
    }

    return mapping.get(raw, raw or "ONBEKEND")
