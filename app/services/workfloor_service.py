from datetime import datetime


def init_workfloor_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workfloor_task_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planning_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'done',
            completed_at TEXT NOT NULL
        )
        """
    )

    conn.commit()


def complete_task(conn, planning_id: str, user_id: str):
    init_workfloor_tables(conn)

    conn.execute(
        """
        INSERT INTO workfloor_task_status (
            planning_id,
            user_id,
            status,
            completed_at
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            planning_id,
            user_id,
            "done",
            datetime.now().isoformat(),
        ),
    )

    conn.commit()


def get_completed_task_ids(conn, user_id: str):
    init_workfloor_tables(conn)

    rows = conn.execute(
        """
        SELECT planning_id
        FROM workfloor_task_status
        WHERE user_id = ?
          AND status = 'done'
        """,
        (user_id,),
    ).fetchall()

    return {row["planning_id"] for row in rows}