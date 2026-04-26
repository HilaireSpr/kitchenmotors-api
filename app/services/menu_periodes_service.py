from datetime import datetime, timedelta


def _validate_dates(startdatum: str, einddatum: str):
    start_date = datetime.strptime(startdatum, "%Y-%m-%d").date()
    end_date = datetime.strptime(einddatum, "%Y-%m-%d").date()

    if end_date < start_date:
        raise ValueError("einddatum mag niet vóór startdatum liggen")

    return start_date, end_date


def get_menu_periodes(conn):
    rows = conn.execute(
        """
        SELECT *
        FROM menu_periodes
        ORDER BY startdatum, naam
        """
    ).fetchall()

    return [dict(r) for r in rows]


def create_menu_periode(
    conn,
    naam: str,
    menu_groep: str,
    startdatum: str,
    einddatum: str,
    rotatielengte_weken: int = 1,
    startweek_in_cyclus: int = 1,
    default_prognose_aantal: float | None = None,
    actief: int = 1,
):
    _validate_dates(startdatum, einddatum)

    if rotatielengte_weken <= 0:
        raise ValueError("rotatielengte_weken moet groter zijn dan 0")

    if startweek_in_cyclus <= 0 or startweek_in_cyclus > rotatielengte_weken:
        raise ValueError("startweek_in_cyclus ligt buiten de rotatie")

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO menu_periodes
        (
            naam,
            menu_groep,
            startdatum,
            einddatum,
            rotatielengte_weken,
            startweek_in_cyclus,
            default_prognose_aantal,
            actief
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            naam,
            menu_groep,
            startdatum,
            einddatum,
            rotatielengte_weken,
            startweek_in_cyclus,
            default_prognose_aantal,
            int(actief),
        ),
    )
    conn.commit()
    return cur.lastrowid


def update_menu_periode(
    conn,
    periode_id: int,
    naam: str,
    menu_groep: str,
    startdatum: str,
    einddatum: str,
    rotatielengte_weken: int = 1,
    startweek_in_cyclus: int = 1,
    default_prognose_aantal: float | None = None,
    actief: int = 1,
):
    _validate_dates(startdatum, einddatum)

    if rotatielengte_weken <= 0:
        raise ValueError("rotatielengte_weken moet groter zijn dan 0")

    if startweek_in_cyclus <= 0 or startweek_in_cyclus > rotatielengte_weken:
        raise ValueError("startweek_in_cyclus ligt buiten de rotatie")

    exists = conn.execute(
        "SELECT id FROM menu_periodes WHERE id = ?",
        (periode_id,),
    ).fetchone()

    if not exists:
        raise ValueError(f"Menuperiode met id {periode_id} niet gevonden")

    conn.execute(
        """
        UPDATE menu_periodes
        SET
            naam = ?,
            menu_groep = ?,
            startdatum = ?,
            einddatum = ?,
            rotatielengte_weken = ?,
            startweek_in_cyclus = ?,
            default_prognose_aantal = ?,
            actief = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            naam,
            menu_groep,
            startdatum,
            einddatum,
            rotatielengte_weken,
            startweek_in_cyclus,
            default_prognose_aantal,
            int(actief),
            periode_id,
        ),
    )
    conn.commit()


def delete_menu_periode(conn, periode_id: int):
    conn.execute("DELETE FROM menu_periodes WHERE id = ?", (periode_id,))
    conn.commit()

def generate_menu_from_periode(
    conn,
    periode_id: int,
    clear_existing_generated: bool = True,
):
    periode = conn.execute(
        """
        SELECT *
        FROM menu_periodes
        WHERE id = ?
        """,
        (periode_id,),
    ).fetchone()

    if not periode:
        raise ValueError(f"Menuperiode met id {periode_id} niet gevonden")

    if int(periode["actief"] or 0) != 1:
        raise ValueError("Deze menuperiode is niet actief")

    start_date, end_date = _validate_dates(periode["startdatum"], periode["einddatum"])

    menu_groep = periode["menu_groep"]
    rotatielengte_weken = int(periode["rotatielengte_weken"] or 1)
    startweek_in_cyclus = int(periode["startweek_in_cyclus"] or 1)
    default_prognose_aantal = periode["default_prognose_aantal"]
    periode_naam = periode["naam"]

    templates = conn.execute(
        """
        SELECT
            pt.recept_id,
            pt.week,
            pt.dag,
            r.menu_groep
        FROM planning_templates pt
        JOIN recepten r ON r.id = pt.recept_id
        JOIN menu_recept_selectie mrs ON mrs.recept_id = r.id
        WHERE mrs.actief = 1
          AND COALESCE(r.menu_groep, '') = ?
        ORDER BY pt.week, pt.dag, pt.recept_id
        """,
        (menu_groep,),
    ).fetchall()

    if not templates:
        raise ValueError(
            f"Geen actieve planning_templates gevonden voor menu_groep '{menu_groep}'"
        )

    if clear_existing_generated:
        conn.execute(
            """
            DELETE FROM menu
            WHERE periode_naam = ?
              AND bron = 'generated'
              AND COALESCE(is_exception, 0) = 0
            """,
            (periode_naam,),
        )

    inserted = 0
    current_day = start_date

    while current_day <= end_date:
        days_since_start = (current_day - start_date).days
        week_index_since_start = days_since_start // 7
        cyclus_week = ((startweek_in_cyclus - 1 + week_index_since_start) % rotatielengte_weken) + 1
        cyclus_dag = current_day.weekday() + 1  # maandag=1

        matching_templates = [
            t for t in templates
            if int(t["week"]) == cyclus_week and int(t["dag"]) == cyclus_dag
        ]

        for template in matching_templates:
            conn.execute(
                """
                INSERT INTO menu
                (
                    recept_id,
                    cyclus_week,
                    cyclus_dag,
                    serveerdag,
                    menu_groep,
                    ritme_type,
                    ritme_interval_weken,
                    bron,
                    prognose_aantal,
                    periode_naam,
                    is_exception,
                    opmerking
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                """,
                (
                    template["recept_id"],
                    cyclus_week,
                    cyclus_dag,
                    current_day.isoformat(),
                    menu_groep,
                    "period_rotation",
                    rotatielengte_weken,
                    "generated",
                    default_prognose_aantal,
                    periode_naam,
                ),
            )
            inserted += 1

        current_day += timedelta(days=1)

    conn.commit()

    return {
        "periode_id": periode_id,
        "periode_naam": periode_naam,
        "menu_groep": menu_groep,
        "startdatum": periode["startdatum"],
        "einddatum": periode["einddatum"],
        "rotatielengte_weken": rotatielengte_weken,
        "startweek_in_cyclus": startweek_in_cyclus,
        "default_prognose_aantal": default_prognose_aantal,
        "inserted_menu_items": inserted,
    }