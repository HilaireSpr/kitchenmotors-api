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

from datetime import date, timedelta
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
    ExecutionLaneV3,
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
    block_score: int = 0
    lane_score: int = 0

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

@dataclass
class PreferenceScore:
    """
    Score gebaseerd op planner-voorkeuren.

    Heeft niets te maken met capaciteit,
    maar met KitchenMotors business rules.
    """

    score: int

    reason: str

@dataclass
class CandidateLaneV3:
    """
    Een mogelijke Execution Lane voor een werkpakket.
    """

    lane: ExecutionLaneV3

    score: LaneScore

def score_lane(
    lane,
    required_minutes: int,
) -> LaneScore:
    """
    Scoort een volledige Execution Lane.

    Sprint 4C:
    Alleen capaciteit.
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

def score_preference(
    lane,
    package,
) -> PreferenceScore:
    """
    Sprint 4E.

    Voorlopig nog neutraal.

    Later:
    - preferred post
    - fixed post
    - alternatieve posten
    - post_policy
    - overrides
    """

    return PreferenceScore(
        score=0,
        reason="No preference rules applied.",
    )

def get_possible_posts_for_package(
    package: WorkPackageV3,
) -> List[str]:
    """
    Geeft mogelijke posten voor een werkpakket.

    Sprint 4D.2:
    Tijdelijke testlogica.

    Later vervangen door:
    - post_policy
    - alternatieve_posten
    - vaste posten
    - toestellen
    """

    if package.productiestroom == "PAT":
        return ["PAT", "FOOD", "REF"]

    if package.productiestroom == "FOOD":
        return ["FOOD", "PAT", "REF"]

    if package.productiestroom == "REF":
        return ["REF", "FOOD", "PAT"]

    if package.productiestroom == "SOEP":
        return ["SOEP", "FOOD", "PAT"]

    return [package.productiestroom]

def choose_best_lane(
    candidate_lanes: List[CandidateLaneV3],
) -> Optional[CandidateLaneV3]:
    """
    Kiest de beste execution lane.

    Sprint 4D:
    Voorlopig wint de lane met de laagste lane score.
    """

    if not candidate_lanes:
        return None

    return min(
        candidate_lanes,
        key=lambda candidate: candidate.score.score,
    )

def find_candidate_lanes(
    execution_lanes: Dict[tuple[str, date], ExecutionLaneV3],
    package: WorkPackageV3,
) -> List[CandidateLaneV3]:
    """
    Geeft alle mogelijke lanes terug.

    Sprint 4D.2:
    Tijdelijk meerdere kandidaatposten op basis van productiestroom.

    Later:
    - alternatieve posten uit handeling
    - post_policy
    - vaste posten
    - toestellen
    """

    duration = estimate_package_duration_minutes(package)

    candidate_lanes: List[CandidateLaneV3] = []

    for post in get_possible_posts_for_package(package):
        key = (
            post,
            package.werkdag,
        )

        lane = execution_lanes.get(key)

        if lane is None:
            continue

        candidate_lanes.append(
            CandidateLaneV3(
                lane=lane,
                score=score_lane(
                    lane=lane,
                    required_minutes=duration,
                ),
            )
        )

    return candidate_lanes

def score_candidate_block(
    block: FreeBlockV3,
    required_minutes: int,
) -> BlockChoiceScore:
    """
    Scoort alleen het vrije blok.

    Sprint 4C:
    Best-fit principe:
    hoe minder resttijd, hoe beter.
    """

    remaining_minutes = block.duur_minuten - required_minutes

    return BlockChoiceScore(
        block=block,
        score=remaining_minutes,
        block_score=remaining_minutes,
        remaining_minutes=remaining_minutes,
        reason=f"Best-fit: {remaining_minutes} minuten resttijd.",
    )

def choose_best_block(
    candidate_blocks: List[FreeBlockV3],
    required_minutes: int,
    lane,
) -> Optional[BlockChoiceScore]:
    """
    Kiest het beste vrije blok.

    Final score =
    lane score + block score
    """

    if not candidate_blocks:
        return None

    lane_score = score_lane(
        lane=lane,
        required_minutes=required_minutes,
    )

    scores: List[BlockChoiceScore] = []

    for block in candidate_blocks:
        block_score = score_candidate_block(
            block=block,
            required_minutes=required_minutes,
        )

        final_score = lane_score.score + block_score.score

        scores.append(
            BlockChoiceScore(
                block=block,
                score=final_score,
                reason=(
                    f"{block_score.reason} "
                    f"{lane_score.reason}. "
                    f"Final score: {final_score}."
                ),
                remaining_minutes=block_score.remaining_minutes,
                projected_load_pct=lane_score.load_pct,
                capacity_penalty=lane_score.capacity_penalty,
                block_score=block_score.score,
                lane_score=lane_score.score,
            )
        )

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

    candidate_lane_debug: List[Dict[str, Any]] = []

    for package in work_packages:

        candidate_lanes = find_candidate_lanes(
            execution_lanes=execution_lanes,
            package=package,
        )

        chosen_lane = choose_best_lane(candidate_lanes)

        if chosen_lane is None:
            missing_lanes.append(
                {
                    "package_id": package.package_id,
                    "post": package.productiestroom,
                    "werkdag": package.werkdag.isoformat(),
                    "items": len(package.items),
                }
            )
            continue

        lane = chosen_lane.lane

        candidate_lane_debug.append(
            {
                "package_id": package.package_id,
                "productiestroom": package.productiestroom,
                "werkdag": package.werkdag.isoformat(),
                "candidate_count": len(candidate_lanes),
                "chosen_post": chosen_lane.lane.post if chosen_lane else None,
                "chosen_score": chosen_lane.score.score if chosen_lane else None,
                "chosen_load_pct": (
                    chosen_lane.score.load_pct
                    if chosen_lane
                    else None
                ),
                "candidates": [
                    {
                        "post": candidate.lane.post,
                        "load_pct": candidate.score.load_pct,
                        "score": candidate.score.score,
                        "capacity_penalty": candidate.score.capacity_penalty,
                        "reason": candidate.score.reason,
                    }
                    for candidate in candidate_lanes
                ],
            }
        )

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
                    "block_score": chosen_score.block_score,
                    "lane_score": chosen_score.lane_score,
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
    context.debug["candidate_lanes_first_20"] = candidate_lane_debug[:20]
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
    overloaded_lanes = []

    for lane in execution_lanes.values():
        if lane.belasting_pct > 100:
            overloaded_lanes.append(
                {
                    "post": lane.post,
                    "werkdag": lane.werkdag.isoformat(),
                    "belasting_pct": lane.belasting_pct,
                    "ingeplande_minuten": lane.ingeplande_minuten,
                    "capaciteit_minuten": lane.capaciteit_minuten,
                    "overload_minutes": max(
                        0,
                        lane.ingeplande_minuten - lane.capaciteit_minuten,
                    ),
                    "packages": len(lane.packages),
                    "blocks": len(lane.blocks),
                }
            )

    context.debug["overloaded_lanes_count"] = len(overloaded_lanes)
    context.debug["overloaded_lanes_first_20"] = sorted(
        overloaded_lanes,
        key=lambda item: item["belasting_pct"],
        reverse=True,
    )[:20]

    return scheduled