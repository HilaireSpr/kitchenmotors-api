from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd


DEFAULT_STARTTIJD = "08:00"
GEEN_TOESTEL = "Geen"
GEEN_POST = "-"
BREAK_LABEL = "🕒 Pauze"

# 8u werkdag inclusief 30 min pauze = 450 min netto capaciteit
DEFAULT_CAPACITEIT_MINUTEN = 450

# Pauze-logica
BREAK_AFTER_ACTIVE_MINUTES = 240
BREAK_DURATION_MINUTES = 30


# =========================================================
# BASIS HELPERS
# =========================================================
def parse_time_string(value: str) -> time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except Exception:
        return time(8, 0)


def format_time_value(t: time) -> str:
    return t.strftime("%H:%M")


def format_minutes_to_hhmm(minutes: int) -> str:
    minutes = int(minutes or 0)
    uren = minutes // 60
    resten = minutes % 60
    return f"{uren:02d}:{resten:02d}"


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def looks_like_iso_date(value: str) -> bool:
    if not value:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False


def normalize_toestel(value: Any) -> str:
    if value is None:
        return GEEN_TOESTEL

    value = str(value).strip()
    if value.lower() in {"", "-", "none", "geen", "n.v.t.", "nvt"}:
        return GEEN_TOESTEL

    return value


def row_get(row: Any, key: str, default=None):
    try:
        return row[key]
    except Exception:
        return default


def row_has_key(row: Any, key: str) -> bool:
    try:
        return key in row.keys()
    except Exception:
        return False


# =========================================================
# CAPACITEIT / POSTEN / TOESTELLEN
# =========================================================
def get_post_capaciteiten(conn) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT naam, capaciteit_minuten
        FROM posten
        """
    ).fetchall()

    result: dict[str, int] = {}
    for r in rows:
        result[r["naam"]] = int(r["capaciteit_minuten"] or DEFAULT_CAPACITEIT_MINUTEN)

    return result


def get_capacity_status(totale_minuten: int, capaciteit_minuten: int) -> str:
    if capaciteit_minuten <= 0:
        return "Geen capaciteit"

    ratio = totale_minuten / capaciteit_minuten
    if ratio > 1:
        return "Overbelast"
    if ratio >= 0.8:
        return "Zwaar"
    return "OK"


def get_post_kleuren(conn) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT naam, kleur
        FROM posten
        ORDER BY naam
        """
    ).fetchall()

    kleurmap: dict[str, str] = {}
    for r in rows:
        kleur = (r["kleur"] or "").strip()
        kleurmap[r["naam"]] = (
            kleur if kleur and kleur.startswith("#") and len(kleur) in [4, 7] else "#1f77b4"
        )

    return kleurmap


def get_toestellen(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT naam
        FROM toestellen
        ORDER BY naam
        """
    ).fetchall()

    return [str(r["naam"]).strip() for r in rows if str(r["naam"]).strip()]


# =========================================================
# LEGACY MENU GENERATIE (oude flow, behouden voor compatibiliteit)
# =========================================================
def clear_menu(conn) -> None:
    conn.execute("DELETE FROM menu")
    conn.commit()


def template_to_real_date(start_monday, start_week, target_week, target_day, cycle_index):
    base_offset_weeks = cycle_index * 4
    relative_week = (target_week - start_week) % 4
    total_days = (base_offset_weeks + relative_week) * 7 + (target_day - 1)
    return start_monday + timedelta(days=total_days)


def generate_menu(conn, start_monday, start_week, cycles):
    """
    Legacy functie voor oude planning_templates-flow.
    Behouden zodat bestaande imports niet breken.
    """
    clear_menu(conn)

    templates = conn.execute(
        """
        SELECT pt.*, r.categorie
        FROM planning_templates pt
        JOIN recepten r ON r.id = pt.recept_id
        JOIN menu_recept_selectie mrs ON mrs.recept_id = r.id
        WHERE mrs.actief = 1
        ORDER BY pt.week, pt.dag, pt.recept_id
        """
    ).fetchall()

    for cycle_index in range(cycles):
        for t in templates:
            serveerdag = template_to_real_date(
                start_monday,
                start_week,
                int(t["week"]),
                int(t["dag"]),
                cycle_index,
            )

            conn.execute(
                """
                INSERT INTO menu (recept_id, cyclus_week, cyclus_dag, serveerdag)
                VALUES (?, ?, ?, ?)
                """,
                (
                    t["recept_id"],
                    t["week"],
                    t["dag"],
                    serveerdag.isoformat(),
                ),
            )

    conn.commit()


# =========================================================
# MENU EXPANSIE (nieuwe flow)
# =========================================================
def parse_weekday_to_index(value: str) -> int:
    mapping = {
        "maandag": 1,
        "dinsdag": 2,
        "woensdag": 3,
        "donderdag": 4,
        "vrijdag": 5,
        "zaterdag": 6,
        "zondag": 7,
    }

    if not value:
        raise ValueError("Lege serveerdag")

    normalized = str(value).strip().lower()
    if normalized not in mapping:
        raise ValueError(f"Ongeldige serveerdag: {value}")

    return mapping[normalized]


def week_in_4_cycle(start_week: int, week_offset: int) -> int:
    return ((start_week - 1 + week_offset) % 4) + 1


def should_include_menu_item_for_week(item, current_cycle_week: int) -> bool:
    ritme_type = str(row_get(item, "ritme_type", "") or "").strip().lower()
    cyclus_week = row_get(item, "cyclus_week")

    if ritme_type in {"", "none", "weekly", "manual"}:
        return True

    if ritme_type == "daily":
        # huidige interpretatie:
        # dagelijks = elke week meenemen op de gekozen serveerdag
        return True

    if ritme_type == "2_weeks":
        basis_week = int(cyclus_week or 1)
        return (current_cycle_week % 2) == (basis_week % 2)

    if ritme_type == "4_weeks":
        basis_week = int(cyclus_week or 1)
        return current_cycle_week == basis_week

    interval_weken = row_get(item, "ritme_interval_weken")
    if ritme_type in {"interval", "custom"} and interval_weken:
        basis_week = int(cyclus_week or 1)
        interval = max(1, int(interval_weken))
        return (current_cycle_week - basis_week) % interval == 0

    return True


def _normalize_menu_item_to_dict(item) -> dict:
    return {
        "id": row_get(item, "id"),
        "recept_id": row_get(item, "recept_id"),
        "cyclus_week": row_get(item, "cyclus_week"),
        "cyclus_dag": row_get(item, "cyclus_dag"),
        "serveerdag": row_get(item, "serveerdag"),
        "menu_groep": row_get(item, "menu_groep"),
        "ritme_type": row_get(item, "ritme_type"),
        "ritme_interval_weken": row_get(item, "ritme_interval_weken"),
        "prognose_aantal": row_get(item, "prognose_aantal"),
        "periode_naam": row_get(item, "periode_naam"),
        "code": row_get(item, "code"),
        "naam": row_get(item, "naam"),
        "categorie": row_get(item, "categorie"),
    }


def expand_menu_items(
    menu_items,
    start_monday: str,
    start_week: int,
    cycles: int,
) -> list[dict]:
    """
    Zet menu-items met weekdag/cyclus om naar concrete serveerdatums.
    """
    start_date = parse_iso_date(start_monday)
    expanded: list[dict] = []

    for item in menu_items:
        base_item = _normalize_menu_item_to_dict(item)
        serveerdag_raw = str(base_item["serveerdag"] or "").strip()

        # Achterwaartse compatibiliteit: oude data met echte datum
        if looks_like_iso_date(serveerdag_raw):
            concrete_date = parse_iso_date(serveerdag_raw)
            expanded.append(
                {
                    **base_item,
                    "serveerdag": concrete_date.isoformat(),
                }
            )
            continue

        weekday_index = int(base_item["cyclus_dag"] or 0) or parse_weekday_to_index(serveerdag_raw)

        for week_offset in range(cycles):
            current_cycle_week = week_in_4_cycle(start_week, week_offset)

            if not should_include_menu_item_for_week(base_item, current_cycle_week):
                continue

            concrete_date = start_date + timedelta(days=(week_offset * 7) + (weekday_index - 1))

            expanded.append(
                {
                    **base_item,
                    "serveerdag": concrete_date.isoformat(),
                    "cyclus_week": current_cycle_week,
                    "cyclus_dag": weekday_index,
                }
            )

    expanded.sort(
        key=lambda r: (
            str(r["serveerdag"]),
            str(r.get("menu_groep") or ""),
            str(r.get("code") or ""),
            str(r.get("naam") or ""),
        )
    )

    return expanded


def _get_menu_items(conn, menu_groep: str | None = None):
    if menu_groep:
        return conn.execute(
            """
            SELECT
                m.id,
                m.recept_id,
                m.cyclus_week,
                m.cyclus_dag,
                m.serveerdag,
                m.menu_groep,
                m.ritme_type,
                m.ritme_interval_weken,
                m.prognose_aantal,
                m.periode_naam,
                r.code,
                r.naam,
                r.categorie
            FROM menu m
            JOIN recepten r ON r.id = m.recept_id
            WHERE COALESCE(m.status, 'active') = 'active'
              AND COALESCE(m.menu_groep, r.menu_groep) = ?
            ORDER BY COALESCE(m.menu_groep, r.menu_groep), r.code, r.naam
            """,
            (menu_groep,),
        ).fetchall()

    return conn.execute(
        """
        SELECT
            m.id,
            m.recept_id,
            m.cyclus_week,
            m.cyclus_dag,
            m.serveerdag,
            m.menu_groep,
            m.ritme_type,
            m.ritme_interval_weken,
            m.prognose_aantal,
            m.periode_naam,
            r.code,
            r.naam,
            r.categorie
        FROM menu m
        JOIN recepten r ON r.id = m.recept_id
        WHERE COALESCE(m.status, 'active') = 'active'
        ORDER BY COALESCE(m.menu_groep, r.menu_groep), r.code, r.naam
        """
    ).fetchall()


# =========================================================
# STARTUREN
# =========================================================
def _get_handelingen_for_recept(conn, recept_id: int):
    return conn.execute(
        """
        SELECT *
        FROM handelingen
        WHERE recept_id=?
        ORDER BY
            dag_offset,
            CASE
                WHEN COALESCE(is_vaste_taak, 0) = 1 THEN 0
                ELSE 1
            END,
            CASE
                WHEN heeft_vast_startuur = 1
                     AND TRIM(COALESCE(vast_startuur, '')) <> ''
                THEN 0
                ELSE 1
            END,
            vast_startuur,
            sort_order,
            code,
            naam
        """,
        (recept_id,),
    ).fetchall()


def get_required_workday_post_pairs(
    conn,
    start_monday: str,
    start_week: int,
    cycles: int,
    menu_groep: str | None = None,
):
    raw_menu_items = _get_menu_items(conn, menu_groep=menu_groep)
    expanded_menu_items = expand_menu_items(
        raw_menu_items,
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
    )

    pairs = set()

    for menu_item in expanded_menu_items:
        handelingen = _get_handelingen_for_recept(conn, menu_item["recept_id"])
        serveerdag = parse_iso_date(menu_item["serveerdag"])

        for h in handelingen:
            post = str(h["post"] or "").strip()
            if not post or post == GEEN_POST:
                continue

            preferred_offset = int(h["dag_offset"] or 0)
            min_offset = int(h["min_offset_dagen"] if h["min_offset_dagen"] is not None else preferred_offset)
            max_offset = int(h["max_offset_dagen"] if h["max_offset_dagen"] is not None else preferred_offset)

            if min_offset > max_offset:
                min_offset, max_offset = max_offset, min_offset

            for offset in range(min_offset, max_offset + 1):
                werkdag = serveerdag + timedelta(days=offset)
                pairs.add((werkdag.isoformat(), post))

    return sorted(pairs, key=lambda x: (x[0], x[1]))


def sync_starturen(
    conn,
    start_monday: str,
    start_week: int,
    cycles: int,
    menu_groep: str | None = None,
) -> None:
    required_pairs = get_required_workday_post_pairs(
        conn=conn,
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
        menu_groep=menu_groep,
    )

    for werkdag, post in required_pairs:
        exists = conn.execute(
            """
            SELECT id
            FROM planning_starturen
            WHERE werkdag=? AND post=?
            """,
            (werkdag, post),
        ).fetchone()

        if not exists:
            conn.execute(
                """
                INSERT INTO planning_starturen (werkdag, post, starttijd)
                VALUES (?, ?, ?)
                """,
                (werkdag, post, DEFAULT_STARTTIJD),
            )

    conn.commit()


def get_planning_starturen(conn) -> dict[tuple[str, str], str]:
    rows = conn.execute(
        """
        SELECT *
        FROM planning_starturen
        ORDER BY werkdag, post
        """
    ).fetchall()

    result: dict[tuple[str, str], str] = {}
    for r in rows:
        result[(r["werkdag"], r["post"])] = r["starttijd"]

    return result


def update_startuur(conn, werkdag: str, post: str, starttijd: str) -> None:
    existing = conn.execute(
        """
        SELECT id
        FROM planning_starturen
        WHERE werkdag=? AND post=?
        """,
        (werkdag, post),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE planning_starturen
            SET starttijd=?
            WHERE werkdag=? AND post=?
            """,
            (starttijd, werkdag, post),
        )
    else:
        conn.execute(
            """
            INSERT INTO planning_starturen (werkdag, post, starttijd)
            VALUES (?, ?, ?)
            """,
            (werkdag, post, starttijd),
        )

    conn.commit()


# =========================================================
# PLANNING HELPERS
# =========================================================
def get_actieve_tijd(conn, handeling_id: int) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(tijd), 0) AS totaal
        FROM stappen
        WHERE handeling_id=?
        """,
        (handeling_id,),
    ).fetchone()

    return int(row["totaal"] if row else 0)


def get_stappen_for_handeling(conn, handeling_id: int):
    return conn.execute(
        """
        SELECT *
        FROM stappen
        WHERE handeling_id=?
        ORDER BY sort_order, id
        """,
        (handeling_id,),
    ).fetchall()


def get_stappen_text(conn, handeling_id: int) -> str:
    stappen = get_stappen_for_handeling(conn, handeling_id)
    return " | ".join([f"{s['sort_order']}. {s['naam']} ({s['tijd']} min)" for s in stappen])


def _get_fixed_start_dt(werkdag: date, heeft_vast_startuur, vast_startuur):
    if int(heeft_vast_startuur or 0) != 1:
        return None

    vast_startuur = str(vast_startuur or "").strip()
    if not vast_startuur:
        return None

    try:
        vast_time = datetime.strptime(vast_startuur, "%H:%M").time()
        return datetime.combine(werkdag, vast_time)
    except Exception:
        return None

def _get_planning_type(handeling) -> str:
    value = str(row_get(handeling, "planning_type", "") or "").strip().lower()
    if value in {"hard", "soft", "floating"}:
        return value

    # backward-compatible fallback
    if int(row_get(handeling, "is_vaste_taak", 0) or 0) == 1:
        if int(row_get(handeling, "heeft_vast_startuur", 0) or 0) == 1:
            return "soft"
        return "floating"

    return "floating"


def _is_handeling_active_for_serveerdatum(handeling, serveerdatum: date) -> bool:
    actief_vanaf = row_get(handeling, "actief_vanaf")
    actief_tot = row_get(handeling, "actief_tot")

    if actief_vanaf:
        try:
            if serveerdatum < parse_iso_date(str(actief_vanaf)):
                return False
        except Exception:
            pass

    if actief_tot:
        try:
            if serveerdatum > parse_iso_date(str(actief_tot)):
                return False
        except Exception:
            pass

    return True

def _match_toestel_candidates(gevraagd_toestel: str, alle_toestellen: list[str]) -> list[str]:
    gevraagd_toestel = normalize_toestel(gevraagd_toestel)
    if gevraagd_toestel == GEEN_TOESTEL:
        return []

    gevraagd_lower = gevraagd_toestel.lower()

    exact = [t for t in alle_toestellen if t.lower() == gevraagd_lower]
    if exact:
        return exact

    prefix_matches = []
    for t in alle_toestellen:
        t_lower = t.lower()
        if (
            t_lower.startswith(gevraagd_lower + " ")
            or t_lower.startswith(gevraagd_lower + "-")
            or t_lower.startswith(gevraagd_lower + "_")
        ):
            prefix_matches.append(t)

    return prefix_matches if prefix_matches else [gevraagd_toestel]


def _choose_best_toestel_start(
    kandidaat_toestellen: list[str],
    toestel_cursors: dict[str, datetime],
    earliest_start: datetime,
):
    if not kandidaat_toestellen:
        return None, earliest_start

    beste_toestel = None
    beste_start = None

    for toestel in kandidaat_toestellen:
        beschikbaar_vanaf = toestel_cursors.get(toestel, earliest_start)
        candidate_start = max(earliest_start, beschikbaar_vanaf)

        if beste_start is None or candidate_start < beste_start or (
            candidate_start == beste_start and str(toestel) < str(beste_toestel)
        ):
            beste_toestel = toestel
            beste_start = candidate_start

    return beste_toestel, beste_start


def _create_break_row(
    werkdag: date,
    werkdag_str: str,
    post: str,
    starttijd: time,
    pauze_start: datetime,
    pauze_einde: datetime,
) -> dict:
    return {
        "Planning ID": f"{werkdag_str}|{post}|pauze|{pauze_start.strftime('%H:%M')}",
        "Recept ID": None,
        "Handeling ID": None,
        "Onderdeel": "",
        "Cyclus": "",
        "Serveerdag": werkdag.strftime("%d/%m/%Y"),
        "Recept": "",
        "Taak": BREAK_LABEL,
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


def _insert_break_if_needed(
    planning_rows: list[dict],
    post_state: dict,
    werkdag: date,
    werkdag_str: str,
    post: str,
    starttijd: time,
) -> bool:
    if post == GEEN_POST:
        return False

    actieve_minuten = int(post_state.get("active_minutes_since_break", 0) or 0)
    if actieve_minuten < BREAK_AFTER_ACTIVE_MINUTES:
        return False

    pauze_start = post_state["post_available_at"]
    pauze_einde = pauze_start + timedelta(minutes=BREAK_DURATION_MINUTES)

    planning_rows.append(
        _create_break_row(
            werkdag=werkdag,
            werkdag_str=werkdag_str,
            post=post,
            starttijd=starttijd,
            pauze_start=pauze_start,
            pauze_einde=pauze_einde,
        )
    )

    post_state["post_available_at"] = pauze_einde
    post_state["active_minutes_since_break"] = 0
    return True


def _get_post_starttijd(starturen_map: dict, werkdag_str: str, post: str) -> time:
    if post == GEEN_POST:
        return parse_time_string(DEFAULT_STARTTIJD)
    return parse_time_string(starturen_map.get((werkdag_str, post), DEFAULT_STARTTIJD))


def _get_post_state(
    post_states: dict,
    werkdag: date,
    werkdag_str: str,
    post: str,
    starttijd: time,
) -> dict:
    post_key = (werkdag_str, post)
    if post_key not in post_states:
        post_states[post_key] = {
            "post_available_at": datetime.combine(werkdag, starttijd),
            "active_minutes_since_break": 0,
        }
    return post_states[post_key]


def _calculate_day_post_load(planning_rows: list[dict], werkdag_str: str, post: str) -> int:
    total = 0
    for row in planning_rows:
        if row.get("Werkdag_iso") == werkdag_str and row.get("Post") == post:
            total += int(row.get("Totale duur", 0) or 0)
    return total


def _score_candidate_day(
    planning_rows: list[dict],
    post_states: dict,
    toestel_cursors: dict,
    starturen_map: dict,
    alle_toestellen: list[str],
    gevraagd_toestel: str,
    serveerdatum: date,
    post: str,
    preferred_offset: int,
    offset: int,
    heeft_vast_startuur,
    vast_startuur,
):
    werkdag = serveerdatum + timedelta(days=offset)
    werkdag_str = werkdag.isoformat()

    post_load = _calculate_day_post_load(planning_rows, werkdag_str, post)
    starttijd = _get_post_starttijd(starturen_map, werkdag_str, post)
    default_post_start = datetime.combine(werkdag, starttijd)

    post_key = (werkdag_str, post)
    post_available_at = post_states.get(
        post_key,
        {"post_available_at": default_post_start},
    )["post_available_at"]

    fixed_start_dt = _get_fixed_start_dt(werkdag, heeft_vast_startuur, vast_startuur)
    earliest_start = max(post_available_at, fixed_start_dt) if fixed_start_dt is not None else post_available_at

    kandidaat_toestellen = _match_toestel_candidates(gevraagd_toestel, alle_toestellen)

    if kandidaat_toestellen:
        toestel_available_times = [
            toestel_cursors.get((werkdag_str, toestel), datetime.combine(werkdag, time(0, 0)))
            for toestel in kandidaat_toestellen
        ]
        earliest_toestel_available = min(toestel_available_times)
        toestel_penalty_minutes = max(
            0,
            int((earliest_toestel_available - earliest_start).total_seconds() // 60),
        )
    else:
        toestel_penalty_minutes = 0

    post_penalty_minutes = max(
        0,
        int((earliest_start - default_post_start).total_seconds() // 60),
    )

    afstand_tot_voorkeur = abs(offset - preferred_offset)

    toestel_blokkade = 1 if toestel_penalty_minutes > 90 else 0
    post_blokkade = 1 if post_penalty_minutes > 120 else 0

    toestel_score = toestel_penalty_minutes * 3
    post_score = post_penalty_minutes * 2
    load_score = int(post_load / 10)

    if afstand_tot_voorkeur == 0:
        voorkeur_score = 0
    elif afstand_tot_voorkeur == 1:
        voorkeur_score = 4
    elif afstand_tot_voorkeur == 2:
        voorkeur_score = 10
    else:
        voorkeur_score = 20 + (afstand_tot_voorkeur * 4)

    score_tuple = (
        toestel_blokkade,
        post_blokkade,
        toestel_score,
        post_score,
        load_score,
        voorkeur_score,
        offset,
    )

    debug = {
        "offset": offset,
        "werkdag": werkdag_str,
        "post_load": post_load,
        "toestel_penalty_minutes": toestel_penalty_minutes,
        "post_penalty_minutes": post_penalty_minutes,
        "afstand_tot_voorkeur": afstand_tot_voorkeur,
        "toestel_blokkade": toestel_blokkade,
        "post_blokkade": post_blokkade,
        "toestel_score": toestel_score,
        "post_score": post_score,
        "load_score": load_score,
        "voorkeur_score": voorkeur_score,
        "score_tuple": score_tuple,
        "fixed_start_used": fixed_start_dt is not None,
    }

    return score_tuple, debug


def _build_reason_summary(best_debug: dict, preferred_offset: int) -> str:
    reasons = []

    if best_debug["offset"] != preferred_offset:
        reasons.append("afwijking van voorkeur")

    if best_debug["toestel_blokkade"]:
        reasons.append("toestelblokkade")
    elif best_debug["toestel_penalty_minutes"] > 0:
        reasons.append(f"toestelwacht {best_debug['toestel_penalty_minutes']} min")

    if best_debug["post_blokkade"]:
        reasons.append("postblokkade")
    elif best_debug["post_penalty_minutes"] > 0:
        reasons.append(f"postwacht {best_debug['post_penalty_minutes']} min")

    if best_debug["post_load"] > 0:
        reasons.append(f"load {best_debug['post_load']} min")

    if best_debug["fixed_start_used"]:
        reasons.append("vast startuur")

    if not reasons:
        reasons.append("voorkeursdag zonder conflict")

    return " | ".join(reasons)


def _format_candidate_debug(candidate_debugs: list[dict]) -> str:
    parts = []

    for d in candidate_debugs:
        part = (
            f"{d['werkdag']} (off {d['offset']}): "
            f"score={d['score_tuple']}, "
            f"load={d['post_load']}, "
            f"toestel={d['toestel_penalty_minutes']}m, "
            f"post={d['post_penalty_minutes']}m, "
            f"pref={d['afstand_tot_voorkeur']}"
        )
        parts.append(part)

    return " || ".join(parts)


def _choose_best_offset_day(
    planning_rows: list[dict],
    post_states: dict,
    toestel_cursors: dict,
    starturen_map: dict,
    alle_toestellen: list[str],
    gevraagd_toestel: str,
    serveerdatum: date,
    post: str,
    preferred_offset: int,
    min_offset: int,
    max_offset: int,
    heeft_vast_startuur,
    vast_startuur,
) -> tuple[date, str, dict]:
    if min_offset > max_offset:
        min_offset, max_offset = max_offset, min_offset

    kandidaten = list(range(min_offset, max_offset + 1))
    candidate_debugs = []

    for offset in kandidaten:
        score_tuple, debug = _score_candidate_day(
            planning_rows=planning_rows,
            post_states=post_states,
            toestel_cursors=toestel_cursors,
            starturen_map=starturen_map,
            alle_toestellen=alle_toestellen,
            gevraagd_toestel=gevraagd_toestel,
            serveerdatum=serveerdatum,
            post=post,
            preferred_offset=preferred_offset,
            offset=offset,
            heeft_vast_startuur=heeft_vast_startuur,
            vast_startuur=vast_startuur,
        )
        candidate_debugs.append(debug)

    best_debug = min(candidate_debugs, key=lambda d: d["score_tuple"])
    beste_offset = best_debug["offset"]
    beste_dag = serveerdatum + timedelta(days=beste_offset)

    decision_debug = {
        "preferred_offset": preferred_offset,
        "min_offset": min_offset,
        "max_offset": max_offset,
        "chosen_offset": beste_offset,
        "chosen_werkdag": beste_dag.isoformat(),
        "chosen_score": str(best_debug["score_tuple"]),
        "reason_summary": _build_reason_summary(best_debug, preferred_offset),
        "candidate_debugs": candidate_debugs,
        "candidate_debug_text": _format_candidate_debug(candidate_debugs),
    }

    return beste_dag, beste_dag.isoformat(), decision_debug


def _build_task_row(
    menu_item,
    handeling,
    serveerdatum: date,
    werkdag: date,
    werkdag_str: str,
    post: str,
    gekozen_toestel: str,
    starttijd: time,
    start_dt: datetime,
    eind_dt: datetime,
    actieve_tijd: int,
    passieve_tijd: int,
    totale_duur: int,
    stappen_text: str,
    planner_debug: dict,
    locked: bool = False,
    is_vaste_taak: bool = False,
    planning_type: str = "floating",
    conflict: bool = False,
    conflict_reason: str = "",
) -> dict:
    return {
        "Planning ID": f"{menu_item['id']}|{menu_item['serveerdag']}|{menu_item['recept_id']}|{handeling['id']}|{menu_item['cyclus_week']}|{menu_item['cyclus_dag']}",
        "Recept ID": menu_item["recept_id"],
        "Handeling ID": handeling["id"],
        "Onderdeel": menu_item["categorie"] or "",
        "Cyclus": f"W{menu_item['cyclus_week']}D{menu_item['cyclus_dag']}",
        "Serveerdag": serveerdatum.strftime("%d/%m/%Y"),
        "Recept": f"{menu_item['code']} - {menu_item['naam']}",
        "Prognose aantal": menu_item["prognose_aantal"],
        "Menu-groep": menu_item["menu_groep"] or "",
        "Periode naam": menu_item["periode_naam"] or "",
        "Taak": f"{handeling['code'] or '-'} - {handeling['naam']}",
        "Post": post,
        "Toestel": gekozen_toestel,
        "Werkdag": werkdag.strftime("%d/%m/%Y"),
        "Werkdag_iso": werkdag_str,
        "Startuur post": format_time_value(starttijd),
        "Start": start_dt,
        "Einde": eind_dt,
        "Actieve tijd": actieve_tijd,
        "Passieve tijd": passieve_tijd,
        "Totale duur": totale_duur,
        "Stappen": stappen_text,
        "Voorkeur offset": planner_debug["preferred_offset"],
        "Min offset": planner_debug["min_offset"],
        "Max offset": planner_debug["max_offset"],
        "Gekozen offset": planner_debug["chosen_offset"],
        "Planner score": planner_debug["chosen_score"],
        "Planner reden": planner_debug["reason_summary"],
        "Planner kandidaatdagen": planner_debug["candidate_debug_text"],
        "Locked": bool(locked),
        "Is vaste taak": bool(is_vaste_taak),
        "Planning type": planning_type,
        "Actief vanaf": row_get(handeling, "actief_vanaf"),
        "Actief tot": row_get(handeling, "actief_tot"),
        "Conflict": bool(conflict),
        "Conflict reden": conflict_reason,
    }


# =========================================================
# PLANNING OPBOUWEN
# =========================================================
def build_planning_df(
    conn,
    start_monday: str,
    start_week: int,
    cycles: int,
    menu_groep: str | None = None,
):
    starturen_map = get_planning_starturen(conn)
    alle_toestellen = get_toestellen(conn)

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
        row["planning_id"]: dict(row) for row in override_rows
    }

    planning_rows: list[dict] = []
    post_states: dict[tuple[str, str], dict] = {}
    toestel_cursors: dict[tuple[str, str], datetime] = {}

    for menu_item in menu_items:
        serveerdatum = parse_iso_date(menu_item["serveerdag"])
        handelingen = _get_handelingen_for_recept(conn, menu_item["recept_id"])

        handelingen = [
            h for h in handelingen
            if _is_handeling_active_for_serveerdatum(h, serveerdatum)
        ]

        for h in handelingen:
            planning_type = _get_planning_type(h)
            preferred_offset = int(h["dag_offset"] or 0)
            min_offset = int(h["min_offset_dagen"] if h["min_offset_dagen"] is not None else preferred_offset)
            max_offset = int(h["max_offset_dagen"] if h["max_offset_dagen"] is not None else preferred_offset)

            standaard_post = (h["post"] or GEEN_POST).strip() or GEEN_POST
            gevraagd_toestel = normalize_toestel(h["toestel"])

            planning_id_preview = (
                f"{menu_item['id']}|{menu_item['serveerdag']}|{menu_item['recept_id']}|{h['id']}|"
                f"{menu_item['cyclus_week']}|{menu_item['cyclus_dag']}"
            )
            override = planning_override_map.get(planning_id_preview)

            post = override["post_override"] if override and override.get("post_override") else standaard_post

            if override and override.get("werkdag_override"):
                werkdag_str = override["werkdag_override"]
                werkdag = parse_iso_date(werkdag_str)

                planner_debug = {
                    "preferred_offset": preferred_offset,
                    "min_offset": min_offset,
                    "max_offset": max_offset,
                    "chosen_offset": None,
                    "chosen_score": "0",
                    "reason_summary": (
                        "Locked by override"
                        if int(override.get("locked") or 0) == 1
                        else "Forced by override"
                    ),
                    "candidate_debug_text": "Override applied",
                }

            elif planning_type == "hard":
                vaste_offset = preferred_offset
                werkdag = serveerdatum + timedelta(days=vaste_offset)
                werkdag_str = werkdag.isoformat()

                planner_debug = {
                    "preferred_offset": preferred_offset,
                    "min_offset": min_offset,
                    "max_offset": max_offset,
                    "chosen_offset": vaste_offset,
                    "chosen_score": "0",
                    "reason_summary": "Hard fixed taak: verplichte dag",
                    "candidate_debug_text": f"Hard fixed op offset {vaste_offset} ({werkdag_str})",
                }

            elif planning_type == "soft":
                werkdag, werkdag_str, planner_debug = _choose_best_offset_day(
                    planning_rows=planning_rows,
                    post_states=post_states,
                    toestel_cursors=toestel_cursors,
                    starturen_map=starturen_map,
                    alle_toestellen=alle_toestellen,
                    gevraagd_toestel=gevraagd_toestel,
                    serveerdatum=serveerdatum,
                    post=post,
                    preferred_offset=preferred_offset,
                    min_offset=min_offset,
                    max_offset=max_offset,
                    heeft_vast_startuur=h["heeft_vast_startuur"],
                    vast_startuur=h["vast_startuur"],
                )
                planner_debug["reason_summary"] = f"Soft fixed: {planner_debug['reason_summary']}"

            else:
                werkdag, werkdag_str, planner_debug = _choose_best_offset_day(
                    planning_rows=planning_rows,
                    post_states=post_states,
                    toestel_cursors=toestel_cursors,
                    starturen_map=starturen_map,
                    alle_toestellen=alle_toestellen,
                    gevraagd_toestel=gevraagd_toestel,
                    serveerdatum=serveerdatum,
                    post=post,
                    preferred_offset=preferred_offset,
                    min_offset=min_offset,
                    max_offset=max_offset,
                    heeft_vast_startuur=0,
                    vast_startuur="",
                )
                planner_debug["reason_summary"] = f"Floating: {planner_debug['reason_summary']}"

            starttijd = _get_post_starttijd(starturen_map, werkdag_str, post)
            post_state = _get_post_state(post_states, werkdag, werkdag_str, post, starttijd)

            actieve_tijd = get_actieve_tijd(conn, h["id"])
            passieve_tijd = int(h["passieve_tijd"] or 0)
            totale_duur = actieve_tijd + passieve_tijd

            fixed_start_dt = _get_fixed_start_dt(werkdag, h["heeft_vast_startuur"], h["vast_startuur"])
            kandidaat_toestellen = _match_toestel_candidates(gevraagd_toestel, alle_toestellen)

            conflict = False
            conflict_reason = ""

            if planning_type == "hard" and fixed_start_dt is not None:
                start_dt = fixed_start_dt
                gekozen_toestel = GEEN_TOESTEL

                if kandidaat_toestellen:
                    gekozen_toestel = kandidaat_toestellen[0]

                # conflict detectie: post bezet
                if start_dt < post_state["post_available_at"]:
                    conflict = True
                    conflict_reason = "Post bezet op hard fixed startuur"

                # conflict detectie: toestel bezet
                if gekozen_toestel != GEEN_TOESTEL:
                    toestel_busy_until = toestel_cursors.get((werkdag_str, gekozen_toestel))
                    if toestel_busy_until and start_dt < toestel_busy_until:
                        conflict = True
                        if conflict_reason:
                            conflict_reason += " | "
                        conflict_reason += "Toestel bezet op hard fixed startuur"

            else:
                earliest_start = (
                    max(post_state["post_available_at"], fixed_start_dt)
                    if fixed_start_dt is not None
                    else post_state["post_available_at"]
                )

                if kandidaat_toestellen:
                    gekozen_toestel, start_dt = _choose_best_toestel_start(
                        kandidaat_toestellen=kandidaat_toestellen,
                        toestel_cursors={
                            t: toestel_cursors.get((werkdag_str, t), datetime.combine(werkdag, time(0, 0)))
                            for t in kandidaat_toestellen
                        },
                        earliest_start=earliest_start,
                    )
                else:
                    gekozen_toestel = GEEN_TOESTEL
                    start_dt = earliest_start

            eind_dt = start_dt + timedelta(minutes=totale_duur)

            task_row = _build_task_row(
                menu_item=menu_item,
                handeling=h,
                serveerdatum=serveerdatum,
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
                stappen_text=get_stappen_text(conn, h["id"]),
                planner_debug=planner_debug,
                locked=bool(override and int(override.get("locked") or 0) == 1),
                is_vaste_taak=bool(int(row_get(h, "is_vaste_taak", 0) or 0)),
                planning_type=planning_type,
                conflict=conflict,
                conflict_reason=conflict_reason,
            )

            planning_id = task_row.get("Planning ID")
            override = planning_override_map.get(planning_id)

            if override and override.get("toestel_override"):
                task_row["Toestel"] = override["toestel_override"]

            planning_rows.append(task_row)

            post_block_end = start_dt + timedelta(minutes=actieve_tijd)
            post_state["post_available_at"] = post_block_end
            post_state["active_minutes_since_break"] += actieve_tijd

            if gekozen_toestel != GEEN_TOESTEL:
                toestel_cursors[(werkdag_str, gekozen_toestel)] = eind_dt

            _insert_break_if_needed(
                planning_rows=planning_rows,
                post_state=post_state,
                werkdag=werkdag,
                werkdag_str=werkdag_str,
                post=post,
                starttijd=starttijd,
            )

    if not planning_rows:
        return pd.DataFrame(
            columns=[
                "Planning ID",
                "Recept ID",
                "Handeling ID",
                "Onderdeel",
                "Cyclus",
                "Serveerdag",
                "Recept",
                "Prognose aantal",
                "Menu-groep",
                "Periode naam",
                "Taak",
                "Post",
                "Toestel",
                "Werkdag",
                "Werkdag_iso",
                "Startuur post",
                "Start",
                "Einde",
                "Actieve tijd",
                "Passieve tijd",
                "Totale duur",
                "Stappen",
                "Voorkeur offset",
                "Min offset",
                "Max offset",
                "Gekozen offset",
                "Planner score",
                "Planner reden",
                "Planner kandidaatdagen",
                "Locked",
                "Is vaste taak",
                "Planning type",
                "Actief vanaf",
                "Actief tot",
                "Conflict",
                "Conflict reden",
            ]
        )

    planning_df = pd.DataFrame(planning_rows)
    planning_df = planning_df.sort_values(["Werkdag_iso", "Post", "Start", "Taak"]).reset_index(drop=True)
    return planning_df