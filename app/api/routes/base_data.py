from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel

from app.db import get_db_connection
from app.schemas.planning import PlanningStartuurUpdateRequest
from app.services.planning import update_startuur
from app.services.post_werkuren_service import (
    get_post_werkuren,
    get_post_werkuren_voor_datum,
    save_post_werkuren,
)

router = APIRouter()

class PostCreate(BaseModel):
    naam: str
    kleur: str = "#dbeafe"
    capaciteit_minuten: int = 480
    startuur: str = "08:00"
    planning_fase: int = 100
    actief_maandag: int = 1
    actief_dinsdag: int = 1
    actief_woensdag: int = 1
    actief_donderdag: int = 1
    actief_vrijdag: int = 1
    actief_zaterdag: int = 1
    actief_zondag: int = 1

class PostUpdate(BaseModel):
    naam: str
    kleur: str = "#dbeafe"
    capaciteit_minuten: int = 480
    startuur: str = "08:00"
    planning_fase: int = 100
    actief_maandag: int = 1
    actief_dinsdag: int = 1
    actief_woensdag: int = 1
    actief_donderdag: int = 1
    actief_vrijdag: int = 1
    actief_zaterdag: int = 1
    actief_zondag: int = 1

class ToestelCreate(BaseModel):
    naam: str

class PostWerkurenDag(BaseModel):
    cyclus_week: int
    weekdag: int
    actief: bool = True
    startuur: str | None = None
    einduur: str | None = None


class PostWerkurenUpdate(BaseModel):
    cyclus_weken: int = 1
    cyclus_startdatum: str | None = None
    dagen: list[PostWerkurenDag] = []

@router.get("/posten")
def get_posten():
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                id,
                naam,
                COALESCE(kleur, '#dbeafe') AS kleur,
                COALESCE(capaciteit_minuten, 480) AS capaciteit_minuten,
                COALESCE(startuur, '08:00') AS startuur,
                COALESCE(planning_fase, 100) AS planning_fase,
                COALESCE(actief_maandag, 1) AS actief_maandag,
                COALESCE(actief_dinsdag, 1) AS actief_dinsdag,
                COALESCE(actief_woensdag, 1) AS actief_woensdag,
                COALESCE(actief_donderdag, 1) AS actief_donderdag,
                COALESCE(actief_vrijdag, 1) AS actief_vrijdag,
                COALESCE(actief_zaterdag, 1) AS actief_zaterdag,
                COALESCE(actief_zondag, 1) AS actief_zondag
            FROM posten
            ORDER BY naam
            """
        ).fetchall()

        return {"result": [dict(row) for row in rows]}
    finally:
        conn.close()


@router.post("/posten")
def create_post(post: PostCreate):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO posten (
                naam,
                kleur,
                capaciteit_minuten,
                startuur,
                planning_fase,
                actief_maandag,
                actief_dinsdag,
                actief_woensdag,
                actief_donderdag,
                actief_vrijdag,
                actief_zaterdag,
                actief_zondag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                post.naam,
                post.kleur,
                post.capaciteit_minuten,
                post.startuur,
                post.planning_fase,
                post.actief_maandag,
                post.actief_dinsdag,
                post.actief_woensdag,
                post.actief_donderdag,
                post.actief_vrijdag,
                post.actief_zaterdag,
                post.actief_zondag,
            ),
        )
        conn.commit()

        return {"success": True}
    finally:
        conn.close()

@router.put("/posten/{post_id}")
def update_post(post_id: int, post: PostUpdate):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            UPDATE posten
            SET
                naam = ?,
                kleur = ?,
                capaciteit_minuten = ?,
                startuur = ?,
                planning_fase = ?,
                actief_maandag = ?,
                actief_dinsdag = ?,
                actief_woensdag = ?,
                actief_donderdag = ?,
                actief_vrijdag = ?,
                actief_zaterdag = ?,
                actief_zondag = ?
            WHERE id = ?
            """,
            (
                post.naam,
                post.kleur,
                post.capaciteit_minuten,
                post.startuur,
                post.planning_fase,
                post.actief_maandag,
                post.actief_dinsdag,
                post.actief_woensdag,
                post.actief_donderdag,
                post.actief_vrijdag,
                post.actief_zaterdag,
                post.actief_zondag,
                post_id,
            ),
        )
        conn.commit()

        return {"success": True}
    finally:
        conn.close()
@router.delete("/posten/{post_id}")
def delete_post(post_id: int):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            DELETE FROM posten
            WHERE id = ?
            """,
            (post_id,),
        )
        conn.commit()

        return {"success": True}
    finally:
        conn.close()

@router.get("/posten/{post_id}/werkuren")
def read_post_werkuren(post_id: int):
    conn = get_db_connection()
    try:
        return get_post_werkuren(conn, post_id)
    finally:
        conn.close()


@router.put("/posten/{post_id}/werkuren")
def update_post_werkuren(post_id: int, payload: PostWerkurenUpdate):
    conn = get_db_connection()
    try:
        return save_post_werkuren(
            conn=conn,
            post_id=post_id,
            cyclus_weken=payload.cyclus_weken,
            cyclus_startdatum=payload.cyclus_startdatum,
            dagen=[dag.model_dump() for dag in payload.dagen],
        )
    finally:
        conn.close()


@router.get("/posten/{post_id}/werkuren/voor-datum")
def read_post_werkuren_voor_datum(post_id: int, datum: str):
    parsed_datum = datetime.strptime(datum, "%Y-%m-%d").date()

    conn = get_db_connection()
    try:
        return get_post_werkuren_voor_datum(conn, post_id, parsed_datum)
    finally:
        conn.close()

@router.get("/toestellen")
def get_toestellen():
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, naam
            FROM toestellen
            ORDER BY naam
            """
        ).fetchall()

        return {"result": [dict(row) for row in rows]}
    finally:
        conn.close()


@router.post("/toestellen")
def create_toestel(toestel: ToestelCreate):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO toestellen (naam)
            VALUES (?)
            """,
            (toestel.naam,),
        )
        conn.commit()

        return {"success": True}
    finally:
        conn.close()


@router.delete("/toestellen/{toestel_id}")
def delete_toestel(toestel_id: int):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            DELETE FROM toestellen
            WHERE id = ?
            """,
            (toestel_id,),
        )
        conn.commit()

        return {"success": True}
    finally:
        conn.close()


@router.get("/planning-starturen")
def list_planning_starturen():
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT ps.werkdag, ps.post, ps.starttijd
            FROM planning_starturen ps
            INNER JOIN posten p ON p.naam = ps.post
            ORDER BY ps.werkdag, ps.post
            """
        ).fetchall()

        return {
            "success": True,
            "result": [dict(row) for row in rows],
        }
    finally:
        conn.close()


@router.put("/planning-starturen")
def save_planning_startuur(payload: PlanningStartuurUpdateRequest):
    conn = get_db_connection()
    try:
        update_startuur(
            conn=conn,
            werkdag=payload.werkdag,
            post=payload.post,
            starttijd=payload.starttijd,
        )

        return {"success": True}
    finally:
        conn.close()

@router.delete("/planning-starturen/reset")
def reset_planning_starturen():
    conn = get_db_connection()
    try:
        deleted_count = conn.execute(
            """
            DELETE FROM planning_starturen
            """
        ).rowcount

        conn.commit()

        return {
            "success": True,
            "deleted_count": deleted_count,
        }
    finally:
        conn.close()