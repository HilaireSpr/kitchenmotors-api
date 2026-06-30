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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.services.planning_v3_execution import build_execution_lanes
from app.services.planning_v3_models import (
    ExecutionBlockV3,
    FreeBlockV3,
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

def find_free_blocks(
    lane,
) -> List[FreeBlockV3]:
    """
    Zoekt vrije tijdsblokken op een Execution Lane.

    Sprint 4A:
    Voorlopig gebruiken we bestaande WORK-blocks.
    Later komen BREAK, PASSIVE en RESERVED erbij.
    """

    free_blocks: List[FreeBlockV3] = []

    current_start = lane.next_available

    if not lane.blocks:
        current_start = lane.next_available

    sorted_blocks = sorted(
        lane.blocks,
        key=lambda block: block.start,
    )

    cursor = lane.start_datetime if hasattr(lane, "start_datetime") else None

    if cursor is None:
        from datetime import datetime

        cursor = datetime.combine(lane.werkdag, lane.startuur)

    for block in sorted_blocks:
        if block.start > cursor:
            duur_minuten = int((block.start - cursor).total_seconds() / 60)

            free_blocks.append(
                FreeBlockV3(
                    start=cursor,
                    einde=block.start,
                    duur_minuten=duur_minuten,
                )
            )

        if block.einde > cursor:
            cursor = block.einde

    lane_end = datetime.combine(lane.werkdag, lane.einduur)

    if cursor < lane_end:
        duur_minuten = int((lane_end - cursor).total_seconds() / 60)

        free_blocks.append(
            FreeBlockV3(
                start=cursor,
                einde=lane_end,
                duur_minuten=duur_minuten,
            )
        )

    return free_blocks

def find_candidate_blocks(
    lane,
    required_minutes: int,
) -> List[FreeBlockV3]:
    """
    Geeft alle vrije blokken terug die groot genoeg zijn
    voor het gevraagde werkpakket.

    Sprint 4B:
    Nog geen scoring.
    Alleen filteren.
    """

    return [
        block
        for block in find_free_blocks(lane)
        if block.duur_minuten >= required_minutes
    ]

@dataclass
class BlockChoiceScore:
    block: FreeBlockV3
    score: int
    reason: str
    remaining_minutes: int = 0
    projected_load_pct: float = 0.0
    capacity_penalty: int = 0

@dataclass
class LaneScore:
    """
    Score van een volledige Execution Lane.

    Deze score staat los van het gekozen vrije blok.
    """

    score: int

    load_pct: float

    capacity_penalty: int

    reason: str

def score_lane(
    lane,
    required_minutes: int,
) -> LaneScore:
    """
    Scoort een volledige Execution Lane.

    Sprint 4C:
    Alleen capaciteit.

    Later komen erbij:
    - bottlenecks
    - forecast
    - alternatieve posten
    - vaste posten
    """

    projected_minutes = lane.ingeplande_minuten + required_minutes

    load_pct = (
        projected_minutes / lane.capaciteit_minuten * 100
        if lane.capaciteit_minuten
        else 0
    )

    capacity_penalty = 0

    if load_pct > 100:
        capacity_penalty = 1000 + int(load_pct - 100)

    elif load_pct > 80:
        capacity_penalty = 100 + int(load_pct - 80)

    return LaneScore(
        score=capacity_penalty,
        load_pct=round(load_pct, 1),
        capacity_penalty=capacity_penalty,
        reason=(
            f"Projected lane load: {load_pct:.1f}% "
            f"(penalty {capacity_penalty})"
        ),
    )

def score_candidate_block(
    block: FreeBlockV3,
    required_minutes: int,
    lane,
) -> BlockChoiceScore:
    """
    Scoort een kandidaatblok.

    RULE_080:
    Vermijd overbelaste lanes.

    Score-opbouw:
    - resttijd in het blok
    - extra penalty wanneer lane na planning boven 80% komt
    - zware penalty wanneer lane na planning boven 100% komt
    """

    remaining_minutes = block.duur_minuten - required_minutes

    projected_minutes = lane.ingeplande_minuten + required_minutes

    projected_load_pct = (
        projected_minutes / lane.capaciteit_minuten * 100
        if lane.capaciteit_minuten
        else 0
    )

    capacity_penalty = 0

    if projected_load_pct > 100:
        capacity_penalty = 1000 + int(projected_load_pct - 100)
    elif projected_load_pct > 80:
        capacity_penalty = 100 + int(projected_load_pct - 80)

    score = remaining_minutes + capacity_penalty

    return BlockChoiceScore(
        block=block,
        score=score,
        reason=(
            f"Best-fit: {remaining_minutes} minuten resttijd. "
            f"Projected load: {projected_load_pct:.1f}%. "
            f"Capacity penalty: {capacity_penalty}."
        ),
        remaining_minutes=remaining_minutes,
        projected_load_pct=round(projected_load_pct, 1),
        capacity_penalty=capacity_penalty,
    )

def choose_best_block(
    candidate_blocks: List[FreeBlockV3],
    required_minutes: int,
    lane,
) -> Optional[BlockChoiceScore]:
    """
    Kiest het beste vrije blok.

    Voorlopig:
    - laagste resttijd wint
    """

    if not candidate_blocks:
        return None

    scores = [
        score_candidate_block(
        block=block,
        required_minutes=required_minutes,
        lane=lane,
    )
        for block in candidate_blocks
    ]

    return min(scores, key=lambda item: item.score)

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

    scheduled_from_free_blocks = 0
    scheduled_from_fallback = 0
    scheduled_with_multiple_candidates = 0
    block_choice_debug: List[Dict[str, Any]] = []

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

        candidate_blocks = find_candidate_blocks(
            lane,
            required_minutes=duration_minutes,
        )

        if candidate_blocks:
            if len(candidate_blocks) > 1:
                scheduled_with_multiple_candidates += 1

            chosen_score = choose_best_block(
                candidate_blocks,
                required_minutes=duration_minutes,
                lane=lane,
            )

            chosen_block = chosen_score.block
            start = chosen_block.start
            block_choice_debug.append(
                {
                    "package_id": package.package_id,
                    "post": lane.post,
                    "werkdag": lane.werkdag.isoformat(),
                    "required_minutes": duration_minutes,
                    "chosen_start": chosen_block.start.strftime("%H:%M"),
                    "chosen_end": chosen_block.einde.strftime("%H:%M"),
                    "chosen_block_minutes": chosen_block.duur_minuten,
                    "score": chosen_score.score,
                    "remaining_minutes": chosen_score.remaining_minutes,
                    "projected_load_pct": chosen_score.projected_load_pct,
                    "capacity_penalty": chosen_score.capacity_penalty,
                    "reason": chosen_score.reason,
                    "candidate_count": len(candidate_blocks),
                }
            )
            scheduling_reason = "Gekozen uit vrije blokken"
            scheduled_from_free_blocks += 1
        else:
            start = lane.next_available
            scheduling_reason = "Fallback naar next_available"
            scheduled_from_fallback += 1

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

        lane.blocks.append(
            ExecutionBlockV3(
                start=start,
                einde=einde,
                block_type="WORK",
                package_id=package.package_id,
                toestel=None,
                locked=False,
            )
        )

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
                            f"op {lane.werkdag.isoformat()} vanaf {start.strftime('%H:%M')}. "
                            f"{scheduling_reason}."
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
    context.debug["scheduled_from_free_blocks"] = scheduled_from_free_blocks
    context.debug["scheduled_from_fallback"] = scheduled_from_fallback
    context.debug["scheduled_with_multiple_candidates"] = scheduled_with_multiple_candidates
    context.debug["block_choice_first_20"] = block_choice_debug[:20]
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
            "blocks": len(lane.blocks),
            "first_blocks": [
                {
                    "start": block.start.strftime("%H:%M"),
                    "einde": block.einde.strftime("%H:%M"),
                    "block_type": block.block_type,
                    "package_id": block.package_id,
                    "toestel": block.toestel,
                    "locked": block.locked,
                }
                for block in lane.blocks[:5]
            ],
            "free_blocks": [
                {
                    "start": block.start.strftime("%H:%M"),
                    "einde": block.einde.strftime("%H:%M"),
                    "duur_minuten": block.duur_minuten,
                }
                for block in find_free_blocks(lane)[:5]
            ],
            "candidate_blocks": [
                {
                    "start": block.start.strftime("%H:%M"),
                    "einde": block.einde.strftime("%H:%M"),
                    "duur_minuten": block.duur_minuten,
                }
                for block in find_candidate_blocks(
                    lane,
                    required_minutes=60,
                )[:5]
            ],
        }
        for lane in sorted(
            execution_lanes.values(),
            key=lambda item: (item.werkdag, item.post),
        )[:20]
    ]

    return scheduled