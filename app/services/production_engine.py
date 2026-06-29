"""
KitchenMotors Production Engine

Deze service bepaalt WAT geproduceerd moet worden.

Niet:
- wanneer
- op welke post
- op welk toestel
- met welke capaciteit

Wel:
- menu-items
- recepten
- handelingen
- productiepakketten
- productie-input voor planners

Deze engine wordt gedeeld door Planner V1 en Planner V3.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional
from app.services.planning import (
    _get_menu_items,
    expand_menu_items,
    _get_handelingen_for_recept,
    _build_packages_for_menu_item,
    get_post_capaciteiten,
    get_post_planning_fases,
    get_posten,
    GEEN_POST,
    parse_iso_date,
)


@dataclass
class ProductionEngineContext:
    start_monday: str
    start_week: int
    cycles: int
    menu_groep: Optional[str] = None


@dataclass
class ProductionTask:
    menu_item: Dict[str, Any]
    handeling: Dict[str, Any]
    preferred_offset: int
    min_offset: int
    max_offset: int
    actieve_tijd: int
    passieve_tijd: int
    stappen_text: str
    gevraagd_toestel: Optional[str]
    planning_type: str
    override: Optional[Dict[str, Any]] = None


@dataclass
class ProductionPackage:
    package_id: str
    package_code: str
    menu_item: Dict[str, Any]
    serveerdatum: date
    productiestroom: str
    planning_fase: int = 100
    tasks: List[ProductionTask] = field(default_factory=list)


@dataclass
class ProductionPlan:
    context: ProductionEngineContext
    packages: List[ProductionPackage] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


def build_production_plan(
    conn,
    start_monday: str,
    start_week: int,
    cycles: int,
    menu_groep: Optional[str] = None,
) -> ProductionPlan:
    """
    Bouwt de gedeelde productie-input voor planners.

    Sprint 1A:
    - gebruikt bestaande V1-menu-expansie
    - gebruikt bestaande V1-handelinglogica
    - gebruikt bestaande V1-package-opbouw

    Nog niet:
    - scheduling
    - postkeuze
    - toestelkeuze
    - capaciteit
    """

    context = ProductionEngineContext(
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
        menu_groep=menu_groep,
    )

    if conn is None:
        return ProductionPlan(
            context=context,
            packages=[],
            debug={
                "status": "Geen databaseconnectie meegegeven",
                "package_count": 0,
            },
        )

    post_capaciteiten = get_post_capaciteiten(conn)
    post_planning_fases = get_post_planning_fases(conn)

    alle_posten = sorted([
        post for post in post_capaciteiten.keys()
        if post and post != GEEN_POST
    ])

    if not alle_posten:
        alle_posten = get_posten(conn)

    raw_menu_items = _get_menu_items(conn, menu_groep=menu_groep)

    menu_items = expand_menu_items(
        raw_menu_items,
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
    )

    override_rows = conn.execute(
        """
        SELECT
            planning_id,
            werkdag_override,
            start_offset_minutes,
            post_override,
            toestel_override,
            locked
        FROM planning_overrides
        """
    ).fetchall()

    planning_override_map = {
        row["planning_id"]: dict(row)
        for row in override_rows
    }

    production_packages: list[ProductionPackage] = []

    for menu_item in menu_items:
        serveerdatum = parse_iso_date(menu_item["serveerdag"])

        handelingen = _get_handelingen_for_recept(
            conn,
            menu_item["recept_id"],
        )

        packages = _build_packages_for_menu_item(
            conn=conn,
            menu_item=menu_item,
            handelingen=handelingen,
            override_map=planning_override_map,
            alle_posten=alle_posten,
            post_planning_fases=post_planning_fases,
        )

        for package in packages:
            tasks: list[ProductionTask] = []

            for task in package["tasks"]:
                tasks.append(
                    ProductionTask(
                        menu_item=menu_item,
                        handeling=task["handeling"],
                        preferred_offset=int(task.get("preferred_offset", 0) or 0),
                        min_offset=int(task.get("min_offset", 0) or 0),
                        max_offset=int(task.get("max_offset", 0) or 0),
                        actieve_tijd=int(task.get("actieve_tijd", 0) or 0),
                        passieve_tijd=int(task.get("passieve_tijd", 0) or 0),
                        stappen_text=str(task.get("stappen_text", "") or ""),
                        gevraagd_toestel=task.get("gevraagd_toestel"),
                        planning_type=str(task.get("planning_type", "") or ""),
                        override=task.get("override"),
                    )
                )

            raw_productiestroom = ""

            if tasks:
                first_handeling = tasks[0].handeling
                try:
                    raw_productiestroom = first_handeling["post"] or ""
                except Exception:
                    raw_productiestroom = ""

            if not raw_productiestroom:
                raw_productiestroom = (
                    package.get("package_post")
                    or package.get("post")
                    or ""
                )

            production_packages.append(
                ProductionPackage(
                    package_id=str(package.get("package_id") or ""),
                    package_code=str(package.get("package_code") or ""),
                    menu_item=menu_item,
                    serveerdatum=serveerdatum,
                    productiestroom=normalize_productiestroom(raw_productiestroom),
                    planning_fase=int(package.get("planning_fase", 100) or 100),
                    tasks=tasks,
                )
            )

    return ProductionPlan(
        context=context,
        packages=production_packages,
        debug={
            "status": "Production Engine gebruikt V1-inputlogica",
            "raw_menu_item_count": len(raw_menu_items),
            "expanded_menu_item_count": len(menu_items),
            "package_count": len(production_packages),
        },
    )

def normalize_productiestroom(value: Any) -> str:
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