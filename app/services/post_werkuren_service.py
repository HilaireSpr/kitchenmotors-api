from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any


WEEKDAG_NAMEN = [
    "maandag",
    "dinsdag",
    "woensdag",
    "donderdag",
    "vrijdag",
    "zaterdag",
    "zondag",
]


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _time_to_minutes(value: str | None) -> int | None:
    if not value:
        return None

    hours, minutes = value.split(":")
    return int(hours) * 60 + int(minutes)


def _calculate_capacity_minutes(startuur: str | None, einduur: str | None) -> int:
    start_minutes = _time_to_minutes(startuur)
    end_minutes = _time_to_minutes(einduur)

    if start_minutes is None or end_minutes is None:
        return 0

    if end_minutes <= start_minutes:
        return 0

    return end_minutes - start_minutes


def get_post_werkuren(conn: sqlite3.Connection, post_id: int) -> dict[str, Any]:
    post = conn.execute(
        """
        SELECT
            id,
            naam,
            post_werkuren_cyclus_weken,
            post_werkuren_cyclus_startdatum
        FROM posten
        WHERE id = ?
        """,
        (post_id,),
    ).fetchone()

    if post is None:
        raise ValueError(f"Post met id {post_id} bestaat niet.")

    dagen = conn.execute(
        """
        SELECT
            id,
            post_id,
            cyclus_week,
            weekdag,
            actief,
            startuur,
            einduur
        FROM post_werkuren
        WHERE post_id = ?
        ORDER BY cyclus_week ASC, weekdag ASC
        """,
        (post_id,),
    ).fetchall()

    return {
        "post_id": post["id"],
        "post_naam": post["naam"],
        "cyclus_weken": post["post_werkuren_cyclus_weken"] or 1,
        "cyclus_startdatum": post["post_werkuren_cyclus_startdatum"],
        "dagen": [dict(row) for row in dagen],
    }


def save_post_werkuren(
    conn: sqlite3.Connection,
    post_id: int,
    cyclus_weken: int,
    cyclus_startdatum: str | None,
    dagen: list[dict[str, Any]],
) -> dict[str, Any]:
    if cyclus_weken not in (1, 2, 3, 4):
        raise ValueError("cyclus_weken moet 1, 2, 3 of 4 zijn.")

    post = conn.execute(
        """
        SELECT id
        FROM posten
        WHERE id = ?
        """,
        (post_id,),
    ).fetchone()

    if post is None:
        raise ValueError(f"Post met id {post_id} bestaat niet.")

    conn.execute(
        """
        UPDATE posten
        SET
            post_werkuren_cyclus_weken = ?,
            post_werkuren_cyclus_startdatum = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (cyclus_weken, cyclus_startdatum, post_id),
    )

    for dag in dagen:
        cyclus_week = int(dag["cyclus_week"])
        weekdag = int(dag["weekdag"])
        actief = 1 if dag.get("actief") else 0
        startuur = dag.get("startuur") if actief else None
        einduur = dag.get("einduur") if actief else None

        if cyclus_week < 1 or cyclus_week > cyclus_weken:
            raise ValueError("cyclus_week valt buiten de ingestelde cyclus.")

        if weekdag < 0 or weekdag > 6:
            raise ValueError("weekdag moet tussen 0 en 6 liggen.")

        if actief and (not startuur or not einduur):
            raise ValueError("Actieve postwerkuren moeten een startuur en einduur hebben.")

        conn.execute(
            """
            INSERT INTO post_werkuren (
                post_id,
                cyclus_week,
                weekdag,
                actief,
                startuur,
                einduur,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(post_id, cyclus_week, weekdag)
            DO UPDATE SET
                actief = excluded.actief,
                startuur = excluded.startuur,
                einduur = excluded.einduur,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                post_id,
                cyclus_week,
                weekdag,
                actief,
                startuur,
                einduur,
            ),
        )

    conn.commit()
    return get_post_werkuren(conn, post_id)


def get_post_werkuren_voor_datum(
    conn: sqlite3.Connection,
    post_id: int,
    datum: date,
) -> dict[str, Any]:
    post = conn.execute(
        """
        SELECT
            id,
            naam,
            startuur,
            capaciteit_minuten,
            einduur,
            post_werkuren_cyclus_weken,
            post_werkuren_cyclus_startdatum,
            actief_maandag,
            actief_dinsdag,
            actief_woensdag,
            actief_donderdag,
            actief_vrijdag,
            actief_zaterdag,
            actief_zondag
        FROM posten
        WHERE id = ?
        """,
        (post_id,),
    ).fetchone()

    if post is None:
        raise ValueError(f"Post met id {post_id} bestaat niet.")

    cyclus_weken = post["post_werkuren_cyclus_weken"] or 1
    cyclus_startdatum = _parse_date(post["post_werkuren_cyclus_startdatum"])
    weekdag = datum.weekday()

    if cyclus_startdatum is not None:
        days_since_start = (datum - cyclus_startdatum).days
        weeks_since_start = days_since_start // 7
        cyclus_week = (weeks_since_start % cyclus_weken) + 1

        werkuren = conn.execute(
            """
            SELECT
                cyclus_week,
                weekdag,
                actief,
                startuur,
                einduur
            FROM post_werkuren
            WHERE post_id = ?
              AND cyclus_week = ?
              AND weekdag = ?
            """,
            (post_id, cyclus_week, weekdag),
        ).fetchone()

        if werkuren is not None:
            actief = bool(werkuren["actief"])
            startuur = werkuren["startuur"]
            einduur = werkuren["einduur"]

            return {
                "post_id": post_id,
                "post_naam": post["naam"],
                "datum": datum.isoformat(),
                "bron": "post_werkuren",
                "cyclus_week": cyclus_week,
                "weekdag": weekdag,
                "weekdag_naam": WEEKDAG_NAMEN[weekdag],
                "actief": actief,
                "startuur": startuur if actief else None,
                "einduur": einduur if actief else None,
                "capaciteit_minuten": _calculate_capacity_minutes(startuur, einduur) if actief else 0,
            }

    legacy_actief_cols = [
        "actief_maandag",
        "actief_dinsdag",
        "actief_woensdag",
        "actief_donderdag",
        "actief_vrijdag",
        "actief_zaterdag",
        "actief_zondag",
    ]

    legacy_actief = bool(post[legacy_actief_cols[weekdag]])
    legacy_startuur = post["startuur"]
    legacy_capaciteit = post["capaciteit_minuten"] or 0

    return {
        "post_id": post_id,
        "post_naam": post["naam"],
        "datum": datum.isoformat(),
        "bron": "legacy_posten",
        "cyclus_week": None,
        "weekdag": weekdag,
        "weekdag_naam": WEEKDAG_NAMEN[weekdag],
        "actief": legacy_actief,
        "startuur": legacy_startuur if legacy_actief else None,
        "einduur": post["einduur"] if legacy_actief else None,
        "capaciteit_minuten": legacy_capaciteit if legacy_actief else 0,
    }