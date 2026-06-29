"""
KitchenMotors Planner V3 - Production Layer

Verantwoordelijk voor:
- gedeelde Production Engine aanroepen
- ProductionItems bouwen
- WorkPackages bouwen

Geen sequencing.
Geen scheduling.
Geen dataframe.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List

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


def build_production_plan(
    context: PlanningContextV3,
) -> List[ProductionItemV3]:
    """
    Bouwt de V3-productie-input via de gedeelde Production Engine.
    """

    conn = get_db_connection()

    try:
        shared_plan = build_shared_production_plan(
            conn=conn,
            start_monday=context.start_monday.isoformat(),
            start_week=context.start_week,
            cycles=context.cycles,
            menu_groep=context.raw_data.get("menu_groep"),
        )

        context.raw_data["production_engine_debug"] = shared_plan.debug

        context.debug["production_engine"] = shared_plan.debug
        context.debug["production_items_count"] = 0
        context.debug["production_streams"] = {}

        production_items: List[ProductionItemV3] = []

        for package in shared_plan.packages:
            for task in package.tasks:
                handeling = task.handeling
                menu_item = task.menu_item

                werkdag = package.serveerdatum + timedelta(
                    days=int(task.preferred_offset or 0)
                )

                production_items.append(
                    ProductionItemV3(
                        recept_id=safe_get(menu_item, "recept_id"),
                        handeling_id=safe_get(handeling, "id"),
                        recept_naam=(
                            safe_get(menu_item, "recept_naam")
                            or safe_get(menu_item, "naam")
                            or ""
                        ),
                        handeling_naam=(
                            safe_get(handeling, "naam")
                            or safe_get(handeling, "code")
                            or ""
                        ),
                        productiestroom=package.productiestroom or "ONBEKEND",
                        serveerdag=package.serveerdatum,
                        werkdag=werkdag,
                        deadline=determine_deadline_v3(handeling),
                        rule_trace=[
                            RULE_001_PRODUCTION_BEFORE_EXECUTION,
                            RULE_040_STREAMS_BEFORE_POSTS,
                        ],
                        package_id=package.package_id,
                        package_code=package.package_code,
                        actieve_tijd=int(task.actieve_tijd or 0),
                        passieve_tijd=int(task.passieve_tijd or 0),
                        totale_duur=int(
                            (task.actieve_tijd or 0)
                            + (task.passieve_tijd or 0)
                        ),
                    )
                )

        stream_counts: Dict[str, int] = {}

        for item in production_items:
            stream_counts[item.productiestroom] = (
                stream_counts.get(item.productiestroom, 0) + 1
            )

        context.debug["production_items_count"] = len(production_items)
        context.debug["production_streams"] = stream_counts

        return production_items

    finally:
        conn.close()


def build_work_packages(
    context: PlanningContextV3,
    production_plan: List[ProductionItemV3],
) -> List[WorkPackageV3]:
    """
    Zet productie-items om naar echte werkpakketten.

    Regel 031:
    Batch / package beweegt bij voorkeur als één geheel.
    """

    grouped: Dict[str, List[ProductionItemV3]] = {}

    for item in production_plan:
        package_key = item.package_id or build_planning_id_v3(item)
        grouped.setdefault(package_key, []).append(item)

    packages: List[WorkPackageV3] = []

    for package_id, items in grouped.items():
        first_item = items[0]

        deadlines = [
            item.deadline
            for item in items
            if item.deadline is not None
        ]

        werkdag = min(item.werkdag for item in items)

        actieve_tijd = sum(int(item.actieve_tijd or 0) for item in items)
        passieve_tijd = sum(int(item.passieve_tijd or 0) for item in items)
        totale_duur = actieve_tijd + passieve_tijd

        packages.append(
            WorkPackageV3(
                package_id=package_id,
                productiestroom=first_item.productiestroom,
                werkdag=werkdag,
                items=items,
                preferred_post=first_item.productiestroom,
                deadline=min(deadlines) if deadlines else None,
                rule_trace=[
                    RULE_031_BATCH_MOVES_AS_ONE,
                    RULE_040_STREAMS_BEFORE_POSTS,
                ],
                actieve_tijd=actieve_tijd,
                passieve_tijd=passieve_tijd,
                totale_duur=totale_duur,
            )
        )

    return packages


def determine_deadline_v3(handeling: Any):
    """
    Bepaalt voorlopig een deadline op basis van vast_startuur.
    """

    from datetime import time

    raw = safe_get(handeling, "vast_startuur")

    if not raw:
        return None

    if isinstance(raw, time):
        return raw

    if isinstance(raw, str):
        try:
            return time.fromisoformat(raw[:5])
        except ValueError:
            return None

    return None


def build_planning_id_v3(
    item: ProductionItemV3,
) -> str:
    """
    Bouwt een stabiele Planner V3 ID.
    """

    return (
        f"V3|"
        f"{item.serveerdag.isoformat()}|"
        f"{item.recept_id}|"
        f"{item.handeling_id}|"
        f"{item.productiestroom}"
    )


def safe_get(row: Any, key: str, default: Any = None) -> Any:
    """
    Leest veilig waarden uit dict, sqlite3.Row of objecten.
    """

    if row is None:
        return default

    if isinstance(row, dict):
        return row.get(key, default)

    try:
        return row[key]
    except Exception:
        return default