from fastapi import APIRouter
from pydantic import BaseModel

from app.db import get_db_connection
from app.schemas.planning import PlanningStartuurUpdateRequest
from app.services.planning import update_startuur

router = APIRouter()

class PostCreate(BaseModel):
    naam: str
    kleur: str = "#dbeafe"
    capaciteit_minuten: int = 480
    startuur: str = "08:00"

class PostUpdate(BaseModel):
    naam: str
    kleur: str = "#dbeafe"
    capaciteit_minuten: int = 480
    startuur: str = "08:00"

class ToestelCreate(BaseModel):
    naam: str


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
                COALESCE(startuur, '08:00') AS startuur
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
            INSERT INTO posten (naam, kleur, capaciteit_minuten, startuur)
            VALUES (?, ?, ?,? )
            """,
            (post.naam, post.kleur, post.capaciteit_minuten),
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
            SET naam = ?, kleur = ?, capaciteit_minuten = ?, startuur = ?
            WHERE id = ?
            """,
            (
                post.naam,
                post.kleur,
                post.capaciteit_minuten,
                post.startuur,
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