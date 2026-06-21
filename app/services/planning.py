from __future__ import annotations

import re

from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd


DEFAULT_STARTTIJD = "06:00"
GEEN_TOESTEL = "Geen"
GEEN_POST = "-"
BREAK_LABEL = "🕒 Pauze"

# Werkdag = 8u30 inclusief 30 min pauze
# Netto actieve capaciteit = 8u00 = 480 min
DEFAULT_CAPACITEIT_MINUTEN = 480

# Pauze-logica
BREAK_AFTER_ACTIVE_MINUTES = 240
BREAK_DURATION_MINUTES = 30

TASK_SEQUENCE_RE = re.compile(r"^(.+)_(\d+)$")


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

def _intervals_overlap(start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime) -> bool:
    return start_a < end_b and start_b < end_a


def _get_device_intervals(
    toestel_bezetting: dict[tuple[str, str], list[tuple[datetime, datetime, str]]],
    werkdag_str: str,
    toestel: str,
) -> list[tuple[datetime, datetime, str]]:
    toestel = normalize_toestel(toestel)
    if toestel == GEEN_TOESTEL:
        return []

    return toestel_bezetting.get((werkdag_str, toestel), [])


def _is_toestel_available(
    toestel_bezetting: dict[tuple[str, str], list[tuple[datetime, datetime, str]]],
    werkdag_str: str,
    toestel: str,
    start_dt: datetime,
    eind_dt: datetime,
) -> bool:
    toestel = normalize_toestel(toestel)
    if toestel == GEEN_TOESTEL:
        return True

    for existing_start, existing_end, _planning_id in _get_device_intervals(
        toestel_bezetting,
        werkdag_str,
        toestel,
    ):
        if _intervals_overlap(start_dt, eind_dt, existing_start, existing_end):
            return False

    return True


def _reserve_toestel(
    toestel_bezetting: dict[tuple[str, str], list[tuple[datetime, datetime, str]]],
    werkdag_str: str,
    toestel: str,
    start_dt: datetime,
    eind_dt: datetime,
    planning_id: str,
) -> None:
    toestel = normalize_toestel(toestel)
    if toestel == GEEN_TOESTEL:
        return

    key = (werkdag_str, toestel)
    toestel_bezetting.setdefault(key, []).append((start_dt, eind_dt, planning_id))
    toestel_bezetting[key].sort(key=lambda item: item[0])


def _find_first_available_toestel_start(
    toestel_bezetting: dict[tuple[str, str], list[tuple[datetime, datetime, str]]],
    werkdag_str: str,
    toestel: str,
    earliest_start: datetime,
    duration_minutes: int,
) -> datetime:
    toestel = normalize_toestel(toestel)
    if toestel == GEEN_TOESTEL:
        return earliest_start

    candidate_start = earliest_start
    duration = timedelta(minutes=max(0, int(duration_minutes or 0)))

    for existing_start, existing_end, _planning_id in _get_device_intervals(
        toestel_bezetting,
        werkdag_str,
        toestel,
    ):
        candidate_end = candidate_start + duration

        if candidate_end <= existing_start:
            return candidate_start

        if _intervals_overlap(candidate_start, candidate_end, existing_start, existing_end):
            candidate_start = existing_end

    return candidate_start

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

def parse_task_sequence_code(value: str | None) -> tuple[str, int] | None:
    """
    Parseert KitchenMotors taakcodes naar:
    (productiepakket/subgroep, volgnummer)

    Ondersteunde vormen:

    1. Met subgroep:
       PAZO_1_ZE_1 -> ("PAZO_1_ZE", 1)
       PAZO_1_ZE_10 -> ("PAZO_1_ZE", 10)

    2. Zonder subgroep:
       GG14_1 -> ("GG14", 1)
       GG14_10 -> ("GG14", 10)
       SR_2 -> ("SR", 2)
       PO_3 -> ("PO", 3)
       KE_4 -> ("KE", 4)
       RE_5 -> ("RE", 5)

    3. Groepscode zonder expliciet volgnummer:
       PAZO_1_ZE -> ("PAZO_1_ZE", 1)
    """
    if not value:
        return None

    task_code = str(value).strip()
    parts = task_code.split("_")

    # Algemene regel:
    # Als het laatste deel numeriek is, is dat altijd het volgnummer.
    # De groep is alles daarvoor.
    if len(parts) >= 2 and parts[-1].isdigit():
        group = "_".join(parts[:-1])
        step = int(parts[-1])
        return group, step

    # Backward-compatible regel voor subgroepcodes zonder expliciet volgnummer.
    if len(parts) == 3:
        return task_code, 1

    return None

def get_handeling_task_code(handeling) -> str | None:
    code = row_get(handeling, "code")
    if not code:
        return None

    return str(code).strip()

def get_dependency_step_sort_key(handeling):
    task_code = get_handeling_task_code(handeling)
    parsed = parse_task_sequence_code(task_code)

    if not parsed:
        return (9999, task_code or "")

    _, step = parsed
    return (step, task_code)

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

def get_post_planning_fases(conn) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT
            naam,
            COALESCE(planning_fase, 100) AS planning_fase
        FROM posten
        WHERE COALESCE(actief, 1) = 1
        """
    ).fetchall()

    result: dict[str, int] = {}

    for row in rows:
        naam = str(row["naam"] or "").strip()
        if not naam:
            continue

        try:
            fase = int(row["planning_fase"] or 100)
        except Exception:
            fase = 100

        if fase <= 0:
            fase = 100

        result[naam] = fase

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

def get_posten(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT naam
        FROM posten
        WHERE COALESCE(actief, 1) = 1
        ORDER BY naam
        """
    ).fetchall()

    return [str(r["naam"]).strip() for r in rows if str(r["naam"]).strip()]

def get_post_weekdag_actief_map(conn) -> dict[str, dict[int, bool]]:
    rows = conn.execute(
        """
        SELECT
            naam,
            COALESCE(actief_maandag, 1) AS actief_maandag,
            COALESCE(actief_dinsdag, 1) AS actief_dinsdag,
            COALESCE(actief_woensdag, 1) AS actief_woensdag,
            COALESCE(actief_donderdag, 1) AS actief_donderdag,
            COALESCE(actief_vrijdag, 1) AS actief_vrijdag,
            COALESCE(actief_zaterdag, 1) AS actief_zaterdag,
            COALESCE(actief_zondag, 1) AS actief_zondag
        FROM posten
        WHERE COALESCE(actief, 1) = 1
        """
    ).fetchall()

    result: dict[str, dict[int, bool]] = {}

    for row in rows:
        naam = str(row["naam"] or "").strip()
        if not naam:
            continue

        result[naam] = {
            0: bool(int(row["actief_maandag"] or 0)),
            1: bool(int(row["actief_dinsdag"] or 0)),
            2: bool(int(row["actief_woensdag"] or 0)),
            3: bool(int(row["actief_donderdag"] or 0)),
            4: bool(int(row["actief_vrijdag"] or 0)),
            5: bool(int(row["actief_zaterdag"] or 0)),
            6: bool(int(row["actief_zondag"] or 0)),
        }

    return result


def _filter_posts_active_on_day(
    posten: list[str],
    werkdag: date,
    post_weekdag_actief_map: dict[str, dict[int, bool]],
) -> list[str]:
    weekday_index = werkdag.weekday()

    result = [
        post
        for post in posten
        if post_weekdag_actief_map.get(post, {}).get(weekday_index, True)
    ]

    return result

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
                AND m.menu_groep = ?
            ORDER BY m.menu_groep, r.code, r.naam
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

    post_starturen = get_post_starturen(conn)

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
                (
                    werkdag,
                    post,
                    post_starturen.get(post, DEFAULT_STARTTIJD),
                ),
            )

    conn.commit()

def get_post_starturen(conn) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT
            naam,
            COALESCE(startuur, ?) AS startuur
        FROM posten
        """,
        (DEFAULT_STARTTIJD,),
    ).fetchall()

    result: dict[str, str] = {}

    for row in rows:
        naam = str(row["naam"] or "").strip()
        startuur = str(row["startuur"] or DEFAULT_STARTTIJD).strip()

        if naam:
            result[naam] = startuur or DEFAULT_STARTTIJD

    return result

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
            return "hard"
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

    prefix_matches = []
    for t in alle_toestellen:
        t_lower = t.lower()
        if (
            t_lower.startswith(gevraagd_lower + " ")
            or t_lower.startswith(gevraagd_lower + "-")
            or t_lower.startswith(gevraagd_lower + "_")
        ):
            prefix_matches.append(t)

    if prefix_matches:
        return sorted(prefix_matches)

    exact = [t for t in alle_toestellen if t.lower() == gevraagd_lower]
    if exact:
        return sorted(exact)

    return [gevraagd_toestel]

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
    """
    Capaciteitsbelasting per post.

    Belangrijk:
    - actieve tijd belast post/medewerker
    - passieve tijd belast geen postcapaciteit
    - pauzes tellen niet als actieve productietijd
    """
    total = 0

    for row in planning_rows:
        if row.get("Werkdag_iso") != werkdag_str:
            continue

        if row.get("Post") != post:
            continue

        total += int(row.get("Actieve tijd", 0) or 0)

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
    dependency_ready_at,
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
    earliest_candidates = [post_available_at]

    if fixed_start_dt is not None:
        earliest_candidates.append(fixed_start_dt)

    if dependency_ready_at is not None:
        earliest_candidates.append(dependency_ready_at)

    earliest_start = max(earliest_candidates)
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
    load_score = int(post_load / 3)

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
            f"{d['werkdag']} / {d.get('post', '-')} (off {d['offset']}): "
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
    dependency_ready_at=None,
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
            dependency_ready_at=dependency_ready_at,
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
def get_task_group_key(task_code: str | None) -> str | None:
    parsed = parse_task_sequence_code(task_code)

    if not parsed:
        return None

    group_key, _step = parsed
    return group_key

def get_task_group_day_key(
    task_code: str | None,
    min_offset: int,
    max_offset: int,
):
    group_key = get_task_group_key(task_code)

    if not group_key:
        return None

    return f"{group_key}|{min_offset}|{max_offset}"


def _choose_best_post_and_offset_day(
    planning_rows: list[dict],
    post_states: dict,
    toestel_cursors: dict,
    starturen_map: dict,
    alle_toestellen: list[str],
    kandidaat_posten: list[str],
    gevraagd_toestel: str,
    serveerdatum: date,
    preferred_post: str,
    preferred_offset: int,
    min_offset: int,
    max_offset: int,
    heeft_vast_startuur,
    vast_startuur,
    dependency_ready_at=None,
) -> tuple[str, date, str, dict]:
    if min_offset > max_offset:
        min_offset, max_offset = max_offset, min_offset

    kandidaten = list(range(min_offset, max_offset + 1))
    candidate_debugs: list[dict] = []

    for post in kandidaat_posten:
        for offset in kandidaten:
            base_score, debug = _score_candidate_day(
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
                dependency_ready_at=dependency_ready_at,
            )

            post_preference_score = 0 if post == preferred_post else 25

            final_score = (
                debug["toestel_blokkade"],
                debug["post_blokkade"],
                debug["load_score"],
                debug["toestel_score"],
                debug["post_score"],
                post_preference_score,
                debug["voorkeur_score"],
                offset,
                post,
            )

            debug = {
                **debug,
                "post": post,
                "preferred_post": preferred_post,
                "post_preference_score": post_preference_score,
                "score_tuple": final_score,
            }
            candidate_debugs.append(debug)

    best_debug = min(candidate_debugs, key=lambda d: d["score_tuple"])
    beste_offset = best_debug["offset"]
    beste_post = best_debug["post"]
    beste_dag = serveerdatum + timedelta(days=beste_offset)

    reason = _build_reason_summary(best_debug, preferred_offset)
    if beste_post != preferred_post:
        reason = f"post gebalanceerd naar {beste_post} i.p.v. {preferred_post} | {reason}"

    decision_debug = {
        "preferred_offset": preferred_offset,
        "min_offset": min_offset,
        "max_offset": max_offset,
        "chosen_offset": beste_offset,
        "chosen_werkdag": beste_dag.isoformat(),
        "chosen_score": str(best_debug["score_tuple"]),
        "reason_summary": reason,
        "candidate_debugs": candidate_debugs,
        "candidate_debug_text": _format_candidate_debug(candidate_debugs),
    }

    return beste_post, beste_dag, beste_dag.isoformat(), decision_debug

def _normalize_post_policy(value) -> str:
    normalized = str(value or "").strip().lower()

    if normalized in {"fixed", "flexible"}:
        return normalized

    return "flexible"


def _parse_alternatieve_posten(value) -> list[str]:
    if not value:
        return []

    return [
        part.strip()
        for part in str(value).replace(";", ",").split(",")
        if part.strip()
    ]


def _get_candidate_posts(
    standaard_post: str,
    post_policy: str,
    alternatieve_posten: str | None,
    alle_posten: list[str],
) -> list[str]:
    standaard_post = (standaard_post or GEEN_POST).strip() or GEEN_POST
    post_policy = _normalize_post_policy(post_policy)

    if post_policy == "fixed":
        return [standaard_post]

    alternatieven = _parse_alternatieve_posten(alternatieve_posten)

    if alternatieven:
        result = [standaard_post] + alternatieven
    else:
        result = alle_posten[:]

    cleaned = []
    seen = set()

    for post in result:
        post = str(post or "").strip()
        if not post or post == GEEN_POST:
            continue

        if post not in seen:
            cleaned.append(post)
            seen.add(post)

    return cleaned or [standaard_post]


def _choose_best_post_for_day(
    planning_rows: list[dict],
    post_states: dict,
    werkdag: date,
    werkdag_str: str,
    kandidaat_posten: list[str],
    voorkeur_post: str,
    starturen_map: dict,
    actieve_tijd: int,
    passieve_tijd: int,
):
    beste_post = kandidaat_posten[0]
    beste_score = None
    debug_rows = []

    totale_duur = int(actieve_tijd or 0) + int(passieve_tijd or 0)

    for post in kandidaat_posten:
        post_load = _calculate_day_post_load(planning_rows, werkdag_str, post)
        starttijd = _get_post_starttijd(starturen_map, werkdag_str, post)
        post_state = _get_post_state(post_states, werkdag, werkdag_str, post, starttijd)

        wacht_minuten = max(
            0,
            int(
                (
                    post_state["post_available_at"]
                    - datetime.combine(werkdag, starttijd)
                ).total_seconds()
                // 60
            ),
        )

        projected_load = post_load + totale_duur
        voorkeur_penalty = 0 if post == voorkeur_post else 45

        score_tuple = (
            projected_load,
            wacht_minuten,
            voorkeur_penalty,
            post,
        )

        debug_rows.append(
            f"{post}: projected_load={projected_load}m, wait={wacht_minuten}m, preference_penalty={voorkeur_penalty}, score={score_tuple}"
        )

        if beste_score is None or score_tuple < beste_score:
            beste_score = score_tuple
            beste_post = post

    return beste_post, " || ".join(debug_rows)


PLANNING_COLUMNS = [
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
    "Pakket ID",
    "Pakket code",
    "Pakket volgorde",
    "Pakket status",
    "Planning fase",
]


def _planning_id_for(menu_item, handeling) -> str:
    return (
        f"{menu_item['id']}|{menu_item['serveerdag']}|{menu_item['recept_id']}|{handeling['id']}|"
        f"{menu_item['cyclus_week']}|{menu_item['cyclus_dag']}"
    )


def _get_offset_window(handeling) -> tuple[int, int, int]:
    preferred_offset = int(row_get(handeling, "dag_offset", 0) or 0)
    min_offset = int(
        row_get(handeling, "min_offset_dagen")
        if row_get(handeling, "min_offset_dagen") is not None
        else preferred_offset
    )
    max_offset = int(
        row_get(handeling, "max_offset_dagen")
        if row_get(handeling, "max_offset_dagen") is not None
        else preferred_offset
    )
    if min_offset > max_offset:
        min_offset, max_offset = max_offset, min_offset
    return preferred_offset, min_offset, max_offset


def _calculate_active_day_post_load(planning_rows: list[dict], werkdag_str: str, post: str) -> int:
    total = 0
    for row in planning_rows:
        if row.get("Werkdag_iso") == werkdag_str and row.get("Post") == post:
            total += int(row.get("Actieve tijd", 0) or 0)
    return total

def _get_package_planning_fase(package: dict, post_planning_fases: dict[str, int]) -> int:
    fases: list[int] = []

    for task in package.get("tasks", []):
        standaard_post = str(task.get("standaard_post") or GEEN_POST).strip()

        if standaard_post and standaard_post != GEEN_POST:
            fases.append(int(post_planning_fases.get(standaard_post, 100) or 100))

    if not fases:
        return 100

    return min(fases)

def _build_package_id(menu_item, package_code: str) -> str:
    return (
        f"{menu_item['id']}|{menu_item['serveerdag']}|{menu_item['recept_id']}|"
        f"{menu_item['cyclus_week']}|{menu_item['cyclus_dag']}|{package_code}"
    )


def _get_task_package_code(handeling) -> str:
    task_code = get_handeling_task_code(handeling)

    parsed = parse_task_sequence_code(task_code)
    if parsed:
        group_key, _step = parsed
        return group_key

    if task_code:
        return task_code

    return f"handeling_{handeling['id']}"

def _prepare_handeling_runtime(conn, menu_item, handeling, override_map: dict) -> dict:
    preferred_offset, min_offset, max_offset = _get_offset_window(handeling)
    standaard_post = (row_get(handeling, "post", GEEN_POST) or GEEN_POST).strip() or GEEN_POST
    post_policy = _normalize_post_policy(row_get(handeling, "post_policy", "flexible"))
    planning_id = _planning_id_for(menu_item, handeling)

    return {
        "handeling": handeling,
        "planning_id": planning_id,
        "override": override_map.get(planning_id),
        "planning_type": _get_planning_type(handeling),
        "task_code": get_handeling_task_code(handeling),
        "parsed_task_code": parse_task_sequence_code(get_handeling_task_code(handeling)),
        "preferred_offset": preferred_offset,
        "min_offset": min_offset,
        "max_offset": max_offset,
        "standaard_post": standaard_post,
        "post_policy": post_policy,
        "alternatieve_posten": row_get(handeling, "alternatieve_posten"),
        "gevraagd_toestel": normalize_toestel(row_get(handeling, "toestel")),
        "actieve_tijd": get_actieve_tijd(conn, handeling["id"]),
        "passieve_tijd": int(row_get(handeling, "passieve_tijd", 0) or 0),
        "stappen_text": get_stappen_text(conn, handeling["id"]),
    }


def _task_candidate_posts(task: dict, alle_posten: list[str]) -> list[str]:
    override = task.get("override")
    if override and override.get("post_override"):
        return [str(override["post_override"]).strip()]

    return _get_candidate_posts(
        standaard_post=task["standaard_post"],
        post_policy=task["post_policy"],
        alternatieve_posten=task["alternatieve_posten"],
        alle_posten=alle_posten,
    )


def _post_allowed_for_task(task: dict, post: str, alle_posten: list[str]) -> bool:
    return post in _task_candidate_posts(task, alle_posten)


def _choose_task_post_from_package(task: dict, package_post: str, alle_posten: list[str]) -> tuple[str, bool]:
    candidates = _task_candidate_posts(task, alle_posten)
    if package_post in candidates:
        return package_post, False
    if candidates:
        return candidates[0], True
    return task["standaard_post"], True


def _build_packages_for_menu_item(
    conn,
    menu_item,
    handelingen,
    override_map: dict,
    alle_posten: list[str],
    post_planning_fases: dict[str, int],
) -> list[dict]:
    packages_by_code: dict[str, dict] = {}

    for h in handelingen:
        package_code = _get_task_package_code(h)
        if package_code not in packages_by_code:
            packages_by_code[package_code] = {
                "package_code": package_code,
                "package_id": _build_package_id(menu_item, package_code),
                "menu_item": menu_item,
                "tasks": [],
            }

        task = _prepare_handeling_runtime(conn, menu_item, h, override_map)
        task["candidate_posts"] = _task_candidate_posts(task, alle_posten)
        packages_by_code[package_code]["tasks"].append(task)

    packages = list(packages_by_code.values())

    for package in packages:
        package["planning_fase"] = _get_package_planning_fase(package, post_planning_fases)
        package["tasks"] = sorted(
            package["tasks"],
            key=lambda t: (
                get_dependency_step_sort_key(t["handeling"]),
                int(row_get(t["handeling"], "sort_order", 0) or 0),
                str(t.get("task_code") or ""),
                int(t["handeling"]["id"]),
            ),
        )

    packages.sort(
        key=lambda p: (
            int(p.get("planning_fase", 100) or 100),
            min(t["preferred_offset"] for t in p["tasks"]),
            p["package_code"],
        )
    )

    return packages


def _candidate_package_offsets(tasks: list[dict]) -> list[int]:
    automatic_tasks = [
        t for t in tasks
        if not (t.get("override") and t["override"].get("werkdag_override"))
    ]

    if not automatic_tasks:
        return [0]

    min_common = max(t["min_offset"] for t in automatic_tasks)
    max_common = min(t["max_offset"] for t in automatic_tasks)

    hard_offsets = {
        t["preferred_offset"]
        for t in automatic_tasks
        if t["planning_type"] == "hard"
    }

    if hard_offsets:
        candidates = sorted(hard_offsets)
        return candidates

    if min_common <= max_common:
        return list(range(min_common, max_common + 1))

    # Fallback: er is geen echte intersectie. We houden het pakket toch samen
    # en markeren later welke taken buiten hun individuele venster vallen.
    low = min(t["min_offset"] for t in automatic_tasks)
    high = max(t["max_offset"] for t in automatic_tasks)
    preferred_values = sorted({t["preferred_offset"] for t in automatic_tasks})
    range_values = list(range(low, high + 1))
    return sorted(set(preferred_values + range_values))


def _candidate_package_posts(tasks: list[dict], alle_posten: list[str]) -> list[str]:
    automatic_tasks = [
        t for t in tasks
        if not (t.get("override") and t["override"].get("post_override"))
    ]

    if not automatic_tasks:
        return [tasks[0]["candidate_posts"][0] if tasks[0]["candidate_posts"] else tasks[0]["standaard_post"]]

    post_sets = [set(t["candidate_posts"]) for t in automatic_tasks if t["candidate_posts"]]
    if post_sets:
        common = set.intersection(*post_sets)
        if common:
            preferred_order = []
            for t in automatic_tasks:
                if t["standaard_post"] in common and t["standaard_post"] not in preferred_order:
                    preferred_order.append(t["standaard_post"])
            return preferred_order + sorted(common - set(preferred_order))

    # Geen gemeenschappelijke post. Geef de scorer de standaardposten als kandidaten;
    # individuele taken die daar niet mogen staan, wijken gecontroleerd uit.
    result = []
    for t in automatic_tasks:
        for post in [t["standaard_post"]] + t["candidate_posts"]:
            if post and post != GEEN_POST and post not in result:
                result.append(post)
    return result or alle_posten[:1] or [GEEN_POST]


def _score_package_candidate(
    package: dict,
    offset: int,
    package_post: str,
    planning_rows: list[dict],
    post_states: dict,
    starturen_map: dict,
    post_capaciteiten: dict[str, int],
    alle_posten: list[str],
    serveerdatum: date,
) -> tuple[tuple, str]:
    werkdag = serveerdatum + timedelta(days=offset)
    werkdag_str = werkdag.isoformat()

    active_by_post: dict[str, int] = {}
    fragmentation_count = 0
    offset_violation_count = 0
    fixed_violation_count = 0
    preference_distance = 0
    wait_minutes = 0
    non_preferred_post_count = 0

    for task in package["tasks"]:
        override = task.get("override")
        if override and override.get("werkdag_override"):
            continue

        task_post, fragmented = _choose_task_post_from_package(
            task,
            package_post,
            alle_posten,
        )

        if fragmented:
            fragmentation_count += 1

        if task_post != task["standaard_post"]:
            non_preferred_post_count += 1

        active_by_post[task_post] = active_by_post.get(task_post, 0) + int(task["actieve_tijd"] or 0)

        if offset < task["min_offset"] or offset > task["max_offset"]:
            offset_violation_count += 1

        if task["planning_type"] == "hard" and offset != task["preferred_offset"]:
            fixed_violation_count += 1

        preference_distance += abs(offset - task["preferred_offset"])

        starttijd = _get_post_starttijd(starturen_map, werkdag_str, task_post)
        default_start = datetime.combine(werkdag, starttijd)
        state = post_states.get((werkdag_str, task_post))

        if state:
            wait_minutes += max(
                0,
                int((state["post_available_at"] - default_start).total_seconds() // 60),
            )

    over_capacity_minutes = 0
    overload_penalty = 0
    high_load_penalty = 0
    projected_ratio_points = 0
    projected_load_debug: dict[str, dict] = {}

    for post, active_minutes in active_by_post.items():
        existing = _calculate_active_day_post_load(planning_rows, werkdag_str, post)
        projected = existing + active_minutes
        capacity = int(
            post_capaciteiten.get(post, DEFAULT_CAPACITEIT_MINUTEN)
            or DEFAULT_CAPACITEIT_MINUTEN
        )

        ratio = projected / max(capacity, 1)
        over_minutes = max(0, projected - capacity)

        # Extra spreidingsdruk:
        # als een post al richting vol gaat, moet dat zwaarder wegen
        # dan alleen de actuele pakketbelasting.
        if ratio >= 0.8:
            high_load_penalty += int(ratio * 2000)

        if ratio >= 1.0:
            overload_penalty += 20000

        over_capacity_minutes += over_minutes
        projected_ratio_points += int(ratio * 100)

        # Menselijke plannerlogica:
        # 0-70%  = prima
        # 70-80% = licht vol
        # 80-90% = liever spreiden
        # 90-100% = zwaar
        # >100% = alleen als er geen goed alternatief is
        if ratio > 1:
            overload_penalty += 50000 + over_minutes * 150 + int((ratio - 1) * 20000)
        elif ratio >= 0.9:
            high_load_penalty += 3000 + int((ratio - 0.9) * 3000)
        elif ratio >= 0.8:
            high_load_penalty += 1200 + int((ratio - 0.8) * 2000)
        elif ratio >= 0.7:
            high_load_penalty += 300 + int((ratio - 0.7) * 1000)

        projected_load_debug[post] = {
            "existing": existing,
            "package_active": active_minutes,
            "projected": projected,
            "capacity": capacity,
            "ratio": round(ratio, 2),
            "over": over_minutes,
        }

    score = (
        fixed_violation_count,
        offset_violation_count,

        overload_penalty,

        # capaciteit belangrijker
        over_capacity_minutes * 5,

        projected_ratio_points * 5,

        high_load_penalty,

        fragmentation_count * 2,

        non_preferred_post_count * 2,

        wait_minutes,
        preference_distance,

        offset,
        package_post,
    )

    debug = (
        f"{werkdag_str}/{package_post}: score={score}, "
        f"loads={projected_load_debug}, "
        f"overcap={over_capacity_minutes}, "
        f"overload_penalty={overload_penalty}, "
        f"high_load_penalty={high_load_penalty}, "
        f"fragments={fragmentation_count}, "
        f"offset_violations={offset_violation_count}, "
        f"pref_distance={preference_distance}, "
        f"non_preferred_posts={non_preferred_post_count}"
    )

    return score, debug


def _choose_package_placement(
    package: dict,
    planning_rows: list[dict],
    post_states: dict,
    starturen_map: dict,
    post_capaciteiten: dict[str, int],
    alle_posten: list[str],
    serveerdatum: date,
    post_weekdag_actief_map: dict[str, dict[int, bool]],
) -> dict:
    offsets = _candidate_package_offsets(package["tasks"])
    posts = _candidate_package_posts(package["tasks"], alle_posten)

    candidates = []
    for offset in offsets:
        werkdag = serveerdatum + timedelta(days=offset)
        active_posts = _filter_posts_active_on_day(
            posten=posts,
            werkdag=werkdag,
            post_weekdag_actief_map=post_weekdag_actief_map,
        )

        if not active_posts:
            continue

        for post in active_posts:
            score, debug = _score_package_candidate(
                package=package,
                offset=offset,
                package_post=post,
                planning_rows=planning_rows,
                post_states=post_states,
                starturen_map=starturen_map,
                post_capaciteiten=post_capaciteiten,
                alle_posten=alle_posten,
                serveerdatum=serveerdatum,
            )
            candidates.append({"offset": offset, "post": post, "score": score, "debug": debug})

    if not candidates:
        fallback_offset = offsets[0] if offsets else 0
        fallback_werkdag = serveerdatum + timedelta(days=fallback_offset)
        fallback_post = package["tasks"][0]["standaard_post"]

        return {
            "offset": fallback_offset,
            "werkdag": fallback_werkdag,
            "werkdag_str": fallback_werkdag.isoformat(),
            "post": fallback_post,
            "score": ("geen_actieve_post",),
            "reason": "Geen actieve toegelaten post op kandidaatdagen",
            "candidate_debug_text": "Geen actieve toegelaten post gevonden volgens post-weekdagen.",
        }
    best = min(candidates, key=lambda c: c["score"])
    werkdag = serveerdatum + timedelta(days=best["offset"])

    reason_parts = ["Pakketplanning"]
    if best["score"][2] > 0:
        reason_parts.append(f"overcapaciteit {best['score'][2]} min")
    if best["score"][3] > 0:
        reason_parts.append(f"{best['score'][3]} taak/taken buiten pakketpost")
    if best["score"][6] > 0:
        reason_parts.append("offset afgewogen")
    if len(reason_parts) == 1:
        reason_parts.append("zelfde dag/post zonder zware conflicten")

    return {
        "offset": best["offset"],
        "werkdag": werkdag,
        "werkdag_str": werkdag.isoformat(),
        "post": best["post"],
        "score": best["score"],
        "reason": " | ".join(reason_parts),
        "candidate_debug_text": " || ".join(c["debug"] for c in candidates),
    }


def _apply_start_override_if_any(start_dt: datetime, starttijd: time, werkdag: date, override) -> datetime:
    if not override:
        return start_dt

    if override.get("start_offset_minutes") is None:
        return start_dt

    try:
        offset_minutes = int(override["start_offset_minutes"] or 0)
    except Exception:
        return start_dt

    return datetime.combine(werkdag, starttijd) + timedelta(minutes=offset_minutes)


def _append_conflict(existing: str, extra: str) -> str:
    if not extra:
        return existing or ""
    if existing:
        return f"{existing} | {extra}"
    return extra


def _planner_debug_for_task(task: dict, placement: dict, conflict_notes: list[str]) -> dict:
    chosen_offset = placement.get("offset")
    reason = placement.get("reason", "Pakketplanning")
    if conflict_notes:
        reason = f"{reason} | {' | '.join(conflict_notes)}"

    return {
        "preferred_offset": task["preferred_offset"],
        "min_offset": task["min_offset"],
        "max_offset": task["max_offset"],
        "chosen_offset": chosen_offset,
        "chosen_score": str(placement.get("score", "0")),
        "reason_summary": reason,
        "candidate_debug_text": placement.get("candidate_debug_text", ""),
    }


def build_planning_df(
    conn,
    start_monday: str,
    start_week: int,
    cycles: int,
    menu_groep: str | None = None,
):
    """
    Bouwt een planning op met productiepakketten als primaire planningseenheid.

    Kernregels:
    - taken met dezelfde pakketcode worden samen geëvalueerd;
    - het pakket krijgt eerst één voorkeursdag en één voorkeurspost;
    - taken binnen het pakket worden daarna sequentieel ingepland;
    - actieve tijd belast capaciteit;
    - passieve tijd bepaalt wel de taak-eindtijd en toestelbezetting, maar niet de postcapaciteit;
    - bestaande overrides blijven taakniveau-ingrepen van de menselijke planner.
    """
    sync_starturen(
        conn=conn,
        start_monday=start_monday,
        start_week=start_week,
        cycles=cycles,
        menu_groep=menu_groep,
    )

    starturen_map = get_planning_starturen(conn)
    alle_toestellen = get_toestellen(conn)
    post_capaciteiten = get_post_capaciteiten(conn)
    post_planning_fases = get_post_planning_fases(conn)
    post_weekdag_actief_map = get_post_weekdag_actief_map(conn)
    alle_posten = sorted([post for post in post_capaciteiten.keys() if post and post != GEEN_POST])
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

    planning_override_map = {row["planning_id"]: dict(row) for row in override_rows}

    planning_rows: list[dict] = []
    post_states: dict[tuple[str, str], dict] = {}
    toestel_cursors: dict[tuple[str, str], datetime] = {}
    toestel_bezetting: dict[tuple[str, str], list[tuple[datetime, datetime, str]]] = {}

    for menu_item in menu_items:
        serveerdatum = parse_iso_date(menu_item["serveerdag"])
        handelingen = _get_handelingen_for_recept(conn, menu_item["recept_id"])
        handelingen = [h for h in handelingen if _is_handeling_active_for_serveerdatum(h, serveerdatum)]

        handelingen = sorted(
            handelingen,
            key=lambda h: (
                get_task_group_key(get_handeling_task_code(h)) or get_handeling_task_code(h) or "",
                get_dependency_step_sort_key(h),
                int(row_get(h, "dag_offset", 0) or 0),
                int(row_get(h, "sort_order", 0) or 0),
                str(row_get(h, "code", "") or ""),
            ),
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
            placement = _choose_package_placement(
                package=package,
                planning_rows=planning_rows,
                post_states=post_states,
                starturen_map=starturen_map,
                post_capaciteiten=post_capaciteiten,
                alle_posten=alle_posten,
                serveerdatum=serveerdatum,
                post_weekdag_actief_map=post_weekdag_actief_map,
            )

            previous_task_end: datetime | None = None
            package_status = "OK"

            for index, task in enumerate(package["tasks"], start=1):
                h = task["handeling"]
                override = task.get("override")
                planning_type = task["planning_type"]

                conflict = False
                conflict_reason = ""
                conflict_notes: list[str] = []

                if override and override.get("werkdag_override"):
                    werkdag_str = override["werkdag_override"]
                    werkdag = parse_iso_date(werkdag_str)
                    chosen_offset = int((werkdag - serveerdatum).days)
                    task_placement = {
                        **placement,
                        "offset": chosen_offset,
                        "werkdag": werkdag,
                        "werkdag_str": werkdag_str,
                        "reason": "Menselijke override op taakniveau",
                        "score": "override",
                        "candidate_debug_text": "Override toegepast; pakket kan hierdoor bewust gesplitst zijn.",
                    }
                    if werkdag_str != placement["werkdag_str"]:
                        package_status = "Door override gesplitst"
                        conflict_notes.append("pakketdag door override doorbroken")
                else:
                    werkdag = placement["werkdag"]
                    werkdag_str = placement["werkdag_str"]
                    chosen_offset = placement["offset"]
                    task_placement = placement

                if chosen_offset < task["min_offset"] or chosen_offset > task["max_offset"]:
                    conflict = True
                    conflict_reason = _append_conflict(
                        conflict_reason,
                        f"Gekozen pakketoffset {chosen_offset} buiten venster {task['min_offset']}..{task['max_offset']}",
                    )

                if planning_type == "hard" and chosen_offset != task["preferred_offset"]:
                    conflict = True
                    conflict_reason = _append_conflict(
                        conflict_reason,
                        "Hard fixed taak buiten verplichte offset",
                    )

                if override and override.get("post_override"):
                    post = str(override["post_override"]).strip()
                    if post != placement["post"]:
                        package_status = "Door override gesplitst"
                        conflict_notes.append("pakketpost door override doorbroken")
                else:
                    post, fragmented = _choose_task_post_from_package(task, placement["post"], alle_posten)
                    if fragmented:
                        package_status = "Gedeeltelijk andere post"
                        conflict_notes.append("geen gemeenschappelijke toegelaten pakketpost")

                starttijd = _get_post_starttijd(starturen_map, werkdag_str, post)
                post_state = _get_post_state(post_states, werkdag, werkdag_str, post, starttijd)
                fixed_start_dt = _get_fixed_start_dt(werkdag, row_get(h, "heeft_vast_startuur"), row_get(h, "vast_startuur"))
                kandidaat_toestellen = _match_toestel_candidates(task["gevraagd_toestel"], alle_toestellen)

                earliest_candidates = [post_state["post_available_at"]]
                if previous_task_end is not None:
                    earliest_candidates.append(previous_task_end)
                if fixed_start_dt is not None:
                    earliest_candidates.append(fixed_start_dt)

                if planning_type == "hard" and fixed_start_dt is not None:
                    start_dt = fixed_start_dt
                    if previous_task_end is not None and start_dt < previous_task_end:
                        conflict = True
                        conflict_reason = _append_conflict(
                            conflict_reason,
                            f"Pakketvolgorde botst met vast startuur; vorige taak klaar om {previous_task_end.strftime('%H:%M')}",
                        )
                    if start_dt < post_state["post_available_at"]:
                        conflict = True
                        conflict_reason = _append_conflict(conflict_reason, "Post bezet op hard fixed startuur")
                else:
                    start_dt = max(earliest_candidates)

                start_dt = _apply_start_override_if_any(start_dt, starttijd, werkdag, override)

                actieve_tijd = int(task["actieve_tijd"] or 0)
                passieve_tijd = int(task["passieve_tijd"] or 0)
                totale_duur = actieve_tijd + passieve_tijd

                gekozen_toestel = GEEN_TOESTEL

                if override and override.get("toestel_override"):
                    gekozen_toestel = normalize_toestel(override["toestel_override"])
                elif kandidaat_toestellen:
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

                planner_debug = _planner_debug_for_task(task, task_placement, conflict_notes)

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
                    stappen_text=task["stappen_text"],
                    planner_debug=planner_debug,
                    locked=bool(override and int(override.get("locked") or 0) == 1),
                    is_vaste_taak=bool(int(row_get(h, "is_vaste_taak", 0) or 0)),
                    planning_type=planning_type,
                    conflict=conflict,
                    conflict_reason=conflict_reason,
                )

                task_row["Pakket ID"] = package["package_id"]
                task_row["Pakket code"] = package["package_code"]
                task_row["Pakket volgorde"] = index
                task_row["Pakket status"] = package_status
                task_row["Planning fase"] = package.get("planning_fase", 100)

                task_row["Planner reden"] = (
                    f"{task_row['Planner reden']} | "
                    f"planningsfase {package.get('planning_fase', 100)}"
                )

                if str(row_get(h, "code", "") or "").startswith("GG14"):
                    task_row["Planner reden"] = (
                        f"{task_row['Planner reden']} | "
                        f"DEBUG code={row_get(h, 'code')} "
                        f"pakket={package['package_code']} "
                        f"volgorde={index} "
                        f"parsed={parse_task_sequence_code(row_get(h, 'code'))}"
                    )

                planning_rows.append(task_row)

                # Capaciteit: alleen actieve tijd blokkeert post en medewerker.
                post_state["post_available_at"] = start_dt + timedelta(minutes=actieve_tijd)
                post_state["active_minutes_since_break"] += actieve_tijd

                # Toestel blijft bezet tot einde totale duur, dus inclusief passieve tijd.
                if gekozen_toestel != GEEN_TOESTEL:
                    toestel_cursors[(werkdag_str, gekozen_toestel)] = max(
                        toestel_cursors.get((werkdag_str, gekozen_toestel), datetime.combine(werkdag, time(0, 0))),
                        eind_dt,
                    )

                    _reserve_toestel(
                        toestel_bezetting=toestel_bezetting,
                        werkdag_str=werkdag_str,
                        toestel=gekozen_toestel,
                        start_dt=start_dt,
                        eind_dt=eind_dt,
                        planning_id=task_row["Planning ID"],
                    )

                previous_task_end = eind_dt

                _insert_break_if_needed(
                    planning_rows=planning_rows,
                    post_state=post_state,
                    werkdag=werkdag,
                    werkdag_str=werkdag_str,
                    post=post,
                    starttijd=starttijd,
                )

    if not planning_rows:
        return pd.DataFrame(columns=PLANNING_COLUMNS)

    planning_df = pd.DataFrame(planning_rows)

    for col in PLANNING_COLUMNS:
        if col not in planning_df.columns:
            planning_df[col] = None

    planning_df = planning_df.sort_values(
        ["Werkdag_iso", "Start", "Post", "Pakket ID", "Pakket volgorde", "Taak"]
    ).reset_index(drop=True)

    return planning_df[PLANNING_COLUMNS]
