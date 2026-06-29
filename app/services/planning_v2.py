from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd

from app.services.planning import (
    BREAK_AFTER_ACTIVE_MINUTES,
    BREAK_DURATION_MINUTES,
    DEFAULT_CAPACITEIT_MINUTEN,
    DEFAULT_STARTTIJD,
    GEEN_POST,
    GEEN_TOESTEL,
    PLANNING_COLUMNS,
    _append_conflict,
    _build_task_row,
    _filter_posts_active_on_day,
    _get_fixed_start_dt,
    _get_handelingen_for_recept,
    _get_menu_items,
    _get_offset_window,
    _get_planning_type,
    _get_post_starttijd,
    _get_post_state,
    _is_handeling_active_for_serveerdatum,
    _match_toestel_candidates,
    _normalize_post_policy,
    _parse_alternatieve_posten,
    _planning_id_for,
    _reserve_toestel,
    _is_toestel_available,
    _find_first_available_toestel_start,
    expand_menu_items,
    format_time_value,
    get_actieve_tijd,
    get_handeling_task_code,
    get_post_capaciteiten,
    get_post_planning_fases,
    get_post_weekdag_actief_map,
    get_stappen_text,
    get_toestellen,
    parse_iso_date,
    parse_task_sequence_code,
    row_get,
    sync_starturen,
    get_planning_starturen,
)


@dataclass
class PlanningTask:
    menu_item: dict
    handeling: Any
    planning_id: str

    recept_id: int
    handeling_id: int
    serveerdatum: date

    task_code: str | None
    group_code: str
    group_sequence: int

    preferred_post: str
    alternative_posts: list[str]
    post_policy: str
    allowed_posts: list[str]

    planning_fase: int

    preferred_offset: int
    min_offset: int
    max_offset: int

    active_minutes: int
    passive_minutes: int
    device: str

    planning_type: str
    fixed_start_dt_by_day: str | None
    deadline_time: str | None

    steps_text: str


@dataclass
class PlanningGroup:
    group_id: str
    menu_item: dict
    serveerdatum: date
    group_code: str
    tasks: list[PlanningTask] = field(default_factory=list)

    preferred_post: str = GEEN_POST
    planning_fase: int = 100


@dataclass
class Candidate:
    group: PlanningGroup
    werkdag: date
    werkdag_str: str
    post: str
    score: tuple
    reason: str
    debug: str


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except Exception:
        return default


def _task_group_code_and_sequence(task_code: str | None, handeling_id: int) -> tuple[str, int]:
    parsed = parse_task_sequence_code(task_code)
    if parsed:
        group_code, sequence = parsed
        return group_code, sequence

    if task_code:
        return task_code, 1

    return f"handeling_{handeling_id}", 1


def _build_allowed_posts(
    preferred_post: str,
    post_policy: str,
    alternative_posts: list[str],
) -> list[str]:
    """
    Regels:
    - taak met maar 1 post mag nooit zomaar naar een andere post
    - alternatieven zijn fallback
    - fixed/flexible bepaalt niet blind 'alle posten'
    """

    preferred_post = str(preferred_post or GEEN_POST).strip() or GEEN_POST
    policy = _normalize_post_policy(post_policy)

    result: list[str] = []

    if preferred_post and preferred_post != GEEN_POST:
        result.append(preferred_post)

    if policy == "flexible":
        for post in alternative_posts:
            post = str(post or "").strip()
            if post and post != GEEN_POST and post not in result:
                result.append(post)

    if policy == "fixed":
        # Fixed betekent hier: voorkeurspost + eventueel expliciet toegestane alternatieven.
        # Geen automatische willekeurige fallback.
        for post in alternative_posts:
            post = str(post or "").strip()
            if post and post != GEEN_POST and post not in result:
                result.append(post)

    return result or [preferred_post]


def _prepare_task(
    conn,
    menu_item: dict,
    handeling,
    serveerdatum: date,
    post_planning_fases: dict[str, int],
) -> PlanningTask:
    preferred_offset, min_offset, max_offset = _get_offset_window(handeling)

    planning_id = _planning_id_for(menu_item, handeling)
    task_code = get_handeling_task_code(handeling)
    group_code, group_sequence = _task_group_code_and_sequence(task_code, handeling["id"])

    preferred_post = str(row_get(handeling, "post", GEEN_POST) or GEEN_POST).strip() or GEEN_POST
    alternative_posts = _parse_alternatieve_posten(row_get(handeling, "alternatieve_posten"))
    post_policy = _normalize_post_policy(row_get(handeling, "post_policy", "flexible"))
    allowed_posts = _build_allowed_posts(preferred_post, post_policy, alternative_posts)

    return PlanningTask(
        menu_item=menu_item,
        handeling=handeling,
        planning_id=planning_id,
        recept_id=menu_item["recept_id"],
        handeling_id=handeling["id"],
        serveerdatum=serveerdatum,
        task_code=task_code,
        group_code=group_code,
        group_sequence=group_sequence,
        preferred_post=preferred_post,
        alternative_posts=alternative_posts,
        post_policy=post_policy,
        allowed_posts=allowed_posts,
        planning_fase=int(post_planning_fases.get(preferred_post, 100) or 100),
        preferred_offset=preferred_offset,
        min_offset=min_offset,
        max_offset=max_offset,
        active_minutes=get_actieve_tijd(conn, handeling["id"]),
        passive_minutes=_safe_int(row_get(handeling, "passieve_tijd", 0), 0),
        device=str(row_get(handeling, "toestel", GEEN_TOESTEL) or GEEN_TOESTEL),
        planning_type=_get_planning_type(handeling),
        fixed_start_dt_by_day=row_get(handeling, "vast_startuur"),
        deadline_time=row_get(handeling, "klaar_tegen") if "klaar_tegen" in handeling.keys() else None,
        steps_text=get_stappen_text(conn, handeling["id"]),
    )


def _build_groups_for_menu_item(
    conn,
    menu_item: dict,
    post_planning_fases: dict[str, int],
) -> list[PlanningGroup]:
    serveerdatum = parse_iso_date(menu_item["serveerdag"])
    handelingen = _get_handelingen_for_recept(conn, menu_item["recept_id"])

    active_handelingen = [
        h for h in handelingen
        if _is_handeling_active_for_serveerdatum(h, serveerdatum)
    ]

    groups_by_code: dict[str, PlanningGroup] = {}

    for h in active_handelingen:
        task = _prepare_task(
            conn=conn,
            menu_item=menu_item,
            handeling=h,
            serveerdatum=serveerdatum,
            post_planning_fases=post_planning_fases,
        )

        group_id = (
            f"{menu_item['id']}|{menu_item['serveerdag']}|"
            f"{menu_item['recept_id']}|{task.group_code}"
        )

        if group_id not in groups_by_code:
            groups_by_code[group_id] = PlanningGroup(
                group_id=group_id,
                menu_item=menu_item,
                serveerdatum=serveerdatum,
                group_code=task.group_code,
            )

        groups_by_code[group_id].tasks.append(task)

    groups = list(groups_by_code.values())

    for group in groups:
        group.tasks.sort(
            key=lambda t: (
                t.group_sequence,
                _safe_int(row_get(t.handeling, "sort_order", 0), 0),
                str(t.task_code or ""),
                t.handeling_id,
            )
        )

        group.preferred_post = group.tasks[0].preferred_post if group.tasks else GEEN_POST
        group.planning_fase = min((t.planning_fase for t in group.tasks), default=100)

    groups.sort(
        key=lambda g: (
            g.planning_fase,
            min((t.preferred_offset for t in g.tasks), default=0),
            g.serveerdatum,
            g.group_code,
        )
    )

    return groups


def _candidate_offsets_for_group(group: PlanningGroup) -> list[int]:
    min_common = max((t.min_offset for t in group.tasks), default=0)
    max_common = min((t.max_offset for t in group.tasks), default=0)

    if min_common <= max_common:
        preferred_offsets = sorted({t.preferred_offset for t in group.tasks})
        window_offsets = list(range(min_common, max_common + 1))
        return sorted(set(preferred_offsets + window_offsets))

    # Geen perfecte overlap: laat planner kiezen, maar met zware penalty.
    low = min((t.min_offset for t in group.tasks), default=0)
    high = max((t.max_offset for t in group.tasks), default=0)
    return list(range(low, high + 1))


def _candidate_posts_for_group(group: PlanningGroup) -> list[str]:
    """
    Eerst gemeenschappelijke voorkeurspost.
    Daarna expliciete alternatieven.
    Geen automatische alle-posten fallback.
    """

    if not group.tasks:
        return [GEEN_POST]

    preferred_order: list[str] = []

    for task in group.tasks:
        if task.preferred_post and task.preferred_post != GEEN_POST:
            if task.preferred_post not in preferred_order:
                preferred_order.append(task.preferred_post)

    allowed_common = set(group.tasks[0].allowed_posts)

    for task in group.tasks[1:]:
        allowed_common = allowed_common.intersection(set(task.allowed_posts))

    common_ordered = [p for p in preferred_order if p in allowed_common]

    for task in group.tasks:
        for post in task.allowed_posts:
            if post in allowed_common and post not in common_ordered:
                common_ordered.append(post)

    if common_ordered:
        return common_ordered

    # Geen gemeenschappelijke post: probeer voorkeursposten, maar markeer later als split/conflict.
    result: list[str] = []
    for task in group.tasks:
        for post in task.allowed_posts:
            if post and post != GEEN_POST and post not in result:
                result.append(post)

    return result or [group.preferred_post]


def _active_load_for_post(planning_rows: list[dict], werkdag_str: str, post: str) -> int:
    total = 0
    for row in planning_rows:
        if row.get("Werkdag_iso") == werkdag_str and row.get("Post") == post:
            total += _safe_int(row.get("Actieve tijd", 0), 0)
    return total


def _score_group_candidate(
    group: PlanningGroup,
    werkdag: date,
    post: str,
    planning_rows: list[dict],
    post_states: dict,
    starturen_map: dict,
    post_capaciteiten: dict[str, int],
) -> Candidate:
    werkdag_str = werkdag.isoformat()

    hard_conflicts = 0
    preferred_post_penalty = 0
    alternative_post_penalty = 0
    offset_penalty = 0
    deadline_penalty = 0
    group_split_penalty = 0
    capacity_penalty = 0
    wait_penalty = 0

    reasons: list[str] = []
    debug_parts: list[str] = []

    group_active_minutes_by_post: dict[str, int] = {}

    for task in group.tasks:
        chosen_task_post = post

        if chosen_task_post not in task.allowed_posts:
            # Deze groep kan niet volledig op deze post.
            # Taak blijft dan op eigen voorkeur; groep is deels gesplitst.
            chosen_task_post = task.preferred_post
            group_split_penalty += 5000
            reasons.append(f"groep deels gesplitst voor {task.task_code}")

        if len(task.allowed_posts) == 1 and chosen_task_post != task.allowed_posts[0]:
            hard_conflicts += 1
            reasons.append(f"{task.task_code}: taak heeft maar 1 toegelaten post")

        if chosen_task_post == task.preferred_post:
            preferred_post_penalty += 0
        elif chosen_task_post in task.alternative_posts:
            alternative_post_penalty += 1000
            reasons.append(f"{task.task_code}: alternatief {chosen_task_post}")
        else:
            preferred_post_penalty += 10000
            reasons.append(f"{task.task_code}: niet-voorkeurspost {chosen_task_post}")

        offset = (werkdag - task.serveerdatum).days

        if offset < task.min_offset or offset > task.max_offset:
            offset_penalty += 20000
            reasons.append(
                f"{task.task_code}: offset {offset} buiten venster {task.min_offset}..{task.max_offset}"
            )
        else:
            offset_penalty += abs(offset - task.preferred_offset) * 500

        group_active_minutes_by_post[chosen_task_post] = (
            group_active_minutes_by_post.get(chosen_task_post, 0) + task.active_minutes
        )

        starttijd = _get_post_starttijd(starturen_map, werkdag_str, chosen_task_post)
        state = post_states.get((werkdag_str, chosen_task_post))
        if state:
            default_start = datetime.combine(werkdag, starttijd)
            wait = max(
                0,
                int((state["post_available_at"] - default_start).total_seconds() // 60),
            )
            wait_penalty += wait * 3

    for task_post, active_minutes in group_active_minutes_by_post.items():
        existing = _active_load_for_post(planning_rows, werkdag_str, task_post)
        projected = existing + active_minutes
        capacity = int(post_capaciteiten.get(task_post, DEFAULT_CAPACITEIT_MINUTEN) or DEFAULT_CAPACITEIT_MINUTEN)

        ratio = projected / max(capacity, 1)

        if ratio > 1:
            capacity_penalty += 30000 + int((projected - capacity) * 200)
            reasons.append(f"{task_post}: over capaciteit")
        elif ratio >= 0.9:
            capacity_penalty += 5000
        elif ratio >= 0.8:
            capacity_penalty += 1500
        elif ratio >= 0.7:
            capacity_penalty += 500

        debug_parts.append(
            f"{task_post}: existing={existing}, group={active_minutes}, projected={projected}, capacity={capacity}, ratio={round(ratio, 2)}"
        )

    score = (
        hard_conflicts,
        preferred_post_penalty,
        alternative_post_penalty,
        offset_penalty,
        deadline_penalty,
        group_split_penalty,
        capacity_penalty,
        wait_penalty,
        werkdag.isoformat(),
        post,
    )

    if not reasons:
        reasons.append("voorkeurspost en offset gerespecteerd")

    return Candidate(
        group=group,
        werkdag=werkdag,
        werkdag_str=werkdag_str,
        post=post,
        score=score,
        reason=" | ".join(dict.fromkeys(reasons)),
        debug=" || ".join(debug_parts),
    )


def _choose_group_candidate(
    group: PlanningGroup,
    planning_rows: list[dict],
    post_states: dict,
    starturen_map: dict,
    post_capaciteiten: dict[str, int],
    post_weekdag_actief_map: dict[str, dict[int, bool]],
) -> Candidate:
    offsets = _candidate_offsets_for_group(group)
    posts = _candidate_posts_for_group(group)

    candidates: list[Candidate] = []

    for offset in offsets:
        werkdag = group.serveerdatum + timedelta(days=offset)

        active_posts = _filter_posts_active_on_day(
            posten=posts,
            werkdag=werkdag,
            post_weekdag_actief_map=post_weekdag_actief_map,
        )

        for post in active_posts:
            candidates.append(
                _score_group_candidate(
                    group=group,
                    werkdag=werkdag,
                    post=post,
                    planning_rows=planning_rows,
                    post_states=post_states,
                    starturen_map=starturen_map,
                    post_capaciteiten=post_capaciteiten,
                )
            )

    if candidates:
        return min(candidates, key=lambda c: c.score)

    fallback_day = group.serveerdatum
    fallback_post = group.preferred_post

    return Candidate(
        group=group,
        werkdag=fallback_day,
        werkdag_str=fallback_day.isoformat(),
        post=fallback_post,
        score=("geen_kandidaat",),
        reason="Geen actieve kandidaatpost gevonden",
        debug="Geen kandidaat gevonden",
    )


def _task_post_inside_group(task: PlanningTask, group_post: str) -> tuple[str, bool, str]:
    if group_post in task.allowed_posts:
        return group_post, False, "groep-post gebruikt"

    if task.preferred_post in task.allowed_posts:
        return task.preferred_post, True, "taak blijft op eigen voorkeurspost"

    return task.allowed_posts[0], True, "taak wijkt uit naar eerste toegelaten post"


def _insert_break_if_needed_v2(
    planning_rows: list[dict],
    post_state: dict,
    werkdag: date,
    werkdag_str: str,
    post: str,
    starttijd: time,
    post_capaciteit_minuten: int,
) -> None:
    if post == GEEN_POST:
        return

    if post_capaciteit_minuten <= 300:
        return

    active_since_break = _safe_int(post_state.get("active_minutes_since_break", 0), 0)

    if active_since_break < BREAK_AFTER_ACTIVE_MINUTES:
        return

    pauze_start = post_state["post_available_at"]
    pauze_einde = pauze_start + timedelta(minutes=BREAK_DURATION_MINUTES)

    planning_rows.append(
        {
            "Planning ID": f"{werkdag_str}|{post}|pauze|{pauze_start.strftime('%H:%M')}",
            "Recept ID": None,
            "Handeling ID": None,
            "Onderdeel": "",
            "Cyclus": "",
            "Serveerdag": werkdag.strftime("%d/%m/%Y"),
            "Recept": "",
            "Taak": "🕒 Pauze",
            "Post": post,
            "Toestel": GEEN_TOESTEL,
            "Werkdag": werkdag.strftime("%d/%m/%Y"),
            "Werkdag_iso": werkdag_str,
            "Startuur post": format_time_value(starttijd),
            "Start": pauze_start,
            "Einde": pauze_einde,
            "Actieve tijd": 0,
            "Passieve tijd": BREAK_DURATION_MINUTES,
            "Totale duur": BREAK_DURATION_MINUTES,
            "Stappen": "",
        }
    )

    post_state["post_available_at"] = pauze_einde
    post_state["active_minutes_since_break"] = 0


def build_planning_v2_df(
    conn,
    start_monday: str,
    start_week: int,
    cycles: int,
    menu_groep: str | None = None,
) -> pd.DataFrame:
    """
    Experimentele menselijke planner.

    Kernprincipes:
    - menu_groep bepaalt welke recepten gebruikt worden
    - voorkeurspost weegt zeer zwaar
    - alternatieve posten zijn fallback
    - taken met 1 toegelaten post blijven op die post
    - handelinggroep blijft liefst zelfde dag/post
    - volgorde binnen groep blijft altijd gerespecteerd
    - actieve tijd blokkeert post
    - passieve tijd blokkeert afhankelijkheid en toestel, niet de post
    - pauze na ongeveer 4 uur actieve tijd, niet bij werkdagen <= 5u
    """

    sync_starturen(
        conn=conn,
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
        menu_groep=menu_groep,
    )

    starturen_map = get_planning_starturen(conn)
    post_capaciteiten = get_post_capaciteiten(conn)
    post_planning_fases = get_post_planning_fases(conn)
    post_weekdag_actief_map = get_post_weekdag_actief_map(conn)
    alle_toestellen = get_toestellen(conn)

    raw_menu_items = _get_menu_items(conn, menu_groep=menu_groep)
    menu_items = expand_menu_items(
        raw_menu_items,
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
    )

    all_groups: list[PlanningGroup] = []

    for menu_item in menu_items:
        all_groups.extend(
            _build_groups_for_menu_item(
                conn=conn,
                menu_item=menu_item,
                post_planning_fases=post_planning_fases,
            )
        )

    all_groups.sort(
        key=lambda g: (
            g.planning_fase,
            min((t.preferred_offset for t in g.tasks), default=0),
            g.serveerdatum,
            g.group_code,
        )
    )

    planning_rows: list[dict] = []
    post_states: dict[tuple[str, str], dict] = {}
    toestel_bezetting: dict[tuple[str, str], list[tuple[datetime, datetime, str]]] = {}

    for group in all_groups:
        candidate = _choose_group_candidate(
            group=group,
            planning_rows=planning_rows,
            post_states=post_states,
            starturen_map=starturen_map,
            post_capaciteiten=post_capaciteiten,
            post_weekdag_actief_map=post_weekdag_actief_map,
        )

        previous_dependency_ready_at: datetime | None = None
        group_status = "Samen gehouden"

        for index, task in enumerate(group.tasks, start=1):
            werkdag = candidate.werkdag
            werkdag_str = candidate.werkdag_str

            post, fragmented, post_reason = _task_post_inside_group(task, candidate.post)

            if fragmented:
                group_status = "Gedeeltelijk gesplitst"

            starttijd = _get_post_starttijd(starturen_map, werkdag_str, post)
            post_state = _get_post_state(post_states, werkdag, werkdag_str, post, starttijd)

            fixed_start_dt = _get_fixed_start_dt(
                werkdag,
                row_get(task.handeling, "heeft_vast_startuur"),
                row_get(task.handeling, "vast_startuur"),
            )

            earliest_candidates = [post_state["post_available_at"]]

            if previous_dependency_ready_at is not None:
                earliest_candidates.append(previous_dependency_ready_at)

            if fixed_start_dt is not None:
                earliest_candidates.append(fixed_start_dt)

            if task.planning_type == "hard" and fixed_start_dt is not None:
                start_dt = fixed_start_dt
            else:
                start_dt = max(earliest_candidates)

            actieve_tijd = int(task.active_minutes or 0)
            passieve_tijd = int(task.passive_minutes or 0)
            totale_duur = actieve_tijd + passieve_tijd

            gekozen_toestel = GEEN_TOESTEL
            kandidaat_toestellen = _match_toestel_candidates(task.device, alle_toestellen)

            if kandidaat_toestellen:
                beste_toestel = None
                beste_start = None

                for toestel in kandidaat_toestellen:
                    candidate_start = _find_first_available_toestel_start(
                        toestel_bezetting=toestel_bezetting,
                        werkdag_str=werkdag_str,
                        toestel=toestel,
                        earliest_start=start_dt,
                        duration_minutes=totale_duur,
                    )

                    if beste_start is None or candidate_start < beste_start or (
                        candidate_start == beste_start and toestel < str(beste_toestel)
                    ):
                        beste_toestel = toestel
                        beste_start = candidate_start

                gekozen_toestel = beste_toestel or GEEN_TOESTEL

                if beste_start is not None:
                    start_dt = beste_start

            eind_dt = start_dt + timedelta(minutes=totale_duur)

            conflict = False
            conflict_reason = ""

            if gekozen_toestel != GEEN_TOESTEL and not _is_toestel_available(
                toestel_bezetting=toestel_bezetting,
                werkdag_str=werkdag_str,
                toestel=gekozen_toestel,
                start_dt=start_dt,
                eind_dt=eind_dt,
            ):
                conflict = True
                conflict_reason = _append_conflict(
                    conflict_reason,
                    f"Toestel {gekozen_toestel} bezet tussen {start_dt.strftime('%H:%M')} en {eind_dt.strftime('%H:%M')}",
                )

            chosen_offset = int((werkdag - task.serveerdatum).days)

            if chosen_offset < task.min_offset or chosen_offset > task.max_offset:
                conflict = True
                conflict_reason = _append_conflict(
                    conflict_reason,
                    f"Offset {chosen_offset} buiten venster {task.min_offset}..{task.max_offset}",
                )

            planner_debug = {
                "preferred_offset": task.preferred_offset,
                "min_offset": task.min_offset,
                "max_offset": task.max_offset,
                "chosen_offset": chosen_offset,
                "chosen_score": str(candidate.score),
                "reason_summary": (
                    f"{candidate.reason} | {post_reason} | "
                    f"planning fase {group.planning_fase}"
                ),
                "candidate_debug_text": candidate.debug,
            }

            row = _build_task_row(
                menu_item=task.menu_item,
                handeling=task.handeling,
                serveerdatum=task.serveerdatum,
                werkdag=werkdag,
                werkdag_str=werkdag_str,
                post=post,
                gekozen_toestel=gekozen_toestel,
                starttijd=starttijd,
                start_dt=start_dt,
                eind_dt=eind_dt,
                actieve_tijd=actieve_tijd,
                passieve_tijd=passieve_tijd,
                totale_duur=totale_duur,
                stappen_text=task.steps_text,
                planner_debug=planner_debug,
                locked=False,
                is_vaste_taak=bool(int(row_get(task.handeling, "is_vaste_taak", 0) or 0)),
                planning_type=task.planning_type,
                conflict=conflict,
                conflict_reason=conflict_reason,
            )

            row["Pakket ID"] = group.group_id
            row["Pakket code"] = group.group_code
            row["Pakket volgorde"] = index
            row["Pakket status"] = group_status
            row["Planning fase"] = group.planning_fase

            planning_rows.append(row)

            # Post is vrij na actieve tijd, niet na passieve tijd.
            post_state["post_available_at"] = start_dt + timedelta(minutes=actieve_tijd)
            post_state["active_minutes_since_break"] += actieve_tijd

            # Afhankelijkheid binnen groep wacht wel tot na passieve tijd.
            previous_dependency_ready_at = eind_dt

            if gekozen_toestel != GEEN_TOESTEL:
                _reserve_toestel(
                    toestel_bezetting=toestel_bezetting,
                    werkdag_str=werkdag_str,
                    toestel=gekozen_toestel,
                    start_dt=start_dt,
                    eind_dt=eind_dt,
                    planning_id=row["Planning ID"],
                )

            _insert_break_if_needed_v2(
                planning_rows=planning_rows,
                post_state=post_state,
                werkdag=werkdag,
                werkdag_str=werkdag_str,
                post=post,
                starttijd=starttijd,
                post_capaciteit_minuten=int(
                    post_capaciteiten.get(post, DEFAULT_CAPACITEIT_MINUTEN)
                    or DEFAULT_CAPACITEIT_MINUTEN
                ),
            )

    if not planning_rows:
        return pd.DataFrame(columns=PLANNING_COLUMNS)

    df = pd.DataFrame(planning_rows)

    for col in PLANNING_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df.sort_values(
        ["Werkdag_iso", "Start", "Post", "Pakket ID", "Pakket volgorde", "Taak"]
    ).reset_index(drop=True)

    return df[PLANNING_COLUMNS]