from datetime import timedelta

import pandas as pd

from app.services.planning_storage import _ensure_default_planning_run

def _get_run_id(conn, planning_run_id=None) -> int:
    if planning_run_id is not None:
        return int(planning_run_id)
    return int(_ensure_default_planning_run(conn))


def init_planning_overrides_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS planning_overrides (
            planning_run_id INTEGER NOT NULL,
            planning_id TEXT NOT NULL,
            start_offset_minutes INTEGER DEFAULT 0,
            post_override TEXT,
            toestel_override TEXT,
            locked INTEGER DEFAULT 0,
            move_before_planning_id TEXT,
            move_after_planning_id TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (planning_run_id, planning_id)
        )
        """
    )

    existing_cols = conn.execute("PRAGMA table_info(planning_overrides)").fetchall()
    col_names = {row["name"] for row in existing_cols}

    if "planning_run_id" not in col_names:
        conn.execute(
            "ALTER TABLE planning_overrides ADD COLUMN planning_run_id INTEGER DEFAULT 1"
        )

    if "move_before_planning_id" not in col_names:
        conn.execute(
            "ALTER TABLE planning_overrides ADD COLUMN move_before_planning_id TEXT"
        )

    if "move_after_planning_id" not in col_names:
        conn.execute(
            "ALTER TABLE planning_overrides ADD COLUMN move_after_planning_id TEXT"
        )

    conn.commit()


def get_planning_overrides_df(conn, planning_run_id=None) -> pd.DataFrame:
    init_planning_overrides_table(conn)
    run_id = _get_run_id(conn, planning_run_id)

    rows = conn.execute(
        """
        SELECT
            planning_run_id,
            planning_id,
            COALESCE(start_offset_minutes, 0) AS start_offset_minutes,
            post_override,
            toestel_override,
            COALESCE(locked, 0) AS locked,
            move_before_planning_id,
            move_after_planning_id,
            updated_at
        FROM planning_overrides
        WHERE planning_run_id = ?
        """,
        (run_id,),
    ).fetchall()

    if not rows:
        return pd.DataFrame(
            columns=[
                "planning_run_id",
                "planning_id",
                "start_offset_minutes",
                "post_override",
                "toestel_override",
                "locked",
                "move_before_planning_id",
                "move_after_planning_id",
                "updated_at",
            ]
        )

    return pd.DataFrame([dict(r) for r in rows])


def get_override_for_planning_id(conn, planning_id: str, planning_run_id=None):
    init_planning_overrides_table(conn)
    run_id = _get_run_id(conn, planning_run_id)

    return conn.execute(
        """
        SELECT
            planning_run_id,
            planning_id,
            COALESCE(start_offset_minutes, 0) AS start_offset_minutes,
            post_override,
            toestel_override,
            COALESCE(locked, 0) AS locked,
            move_before_planning_id,
            move_after_planning_id,
            updated_at
        FROM planning_overrides
        WHERE planning_run_id = ?
          AND planning_id = ?
        """,
        (run_id, planning_id),
    ).fetchone()


def upsert_planning_override(
    conn,
    planning_id: str,
    planning_run_id=None,
    start_offset_minutes=None,
    post_override=None,
    toestel_override=None,
    locked=None,
    move_before_planning_id=None,
    move_after_planning_id=None,
):
    init_planning_overrides_table(conn)
    run_id = _get_run_id(conn, planning_run_id)

    existing = get_override_for_planning_id(conn, planning_id, run_id)

    def _normalize_nullable_text(value):
        if value is None:
            return None
        value = str(value).strip()
        return value if value else None

    def _normalize_move_value(new_value, old_value):
        if new_value is None:
            return old_value
        if new_value == "__CLEAR__":
            return None
        new_value = str(new_value).strip()
        return new_value if new_value else None

    if existing:
        new_start_offset = (
            int(start_offset_minutes)
            if start_offset_minutes is not None
            else int(existing["start_offset_minutes"] or 0)
        )
        new_post_override = (
            _normalize_nullable_text(post_override)
            if post_override is not None
            else existing["post_override"]
        )
        new_toestel_override = (
            _normalize_nullable_text(toestel_override)
            if toestel_override is not None
            else existing["toestel_override"]
        )
        new_locked = int(locked) if locked is not None else int(existing["locked"] or 0)

        new_move_before = _normalize_move_value(
            move_before_planning_id,
            existing["move_before_planning_id"],
        )
        new_move_after = _normalize_move_value(
            move_after_planning_id,
            existing["move_after_planning_id"],
        )

        conn.execute(
            """
            UPDATE planning_overrides
            SET
                start_offset_minutes = ?,
                post_override = ?,
                toestel_override = ?,
                locked = ?,
                move_before_planning_id = ?,
                move_after_planning_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE planning_run_id = ?
              AND planning_id = ?
            """,
            (
                new_start_offset,
                new_post_override,
                new_toestel_override,
                new_locked,
                new_move_before,
                new_move_after,
                run_id,
                planning_id,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO planning_overrides (
                planning_run_id,
                planning_id,
                start_offset_minutes,
                post_override,
                toestel_override,
                locked,
                move_before_planning_id,
                move_after_planning_id,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                run_id,
                planning_id,
                int(start_offset_minutes or 0),
                _normalize_nullable_text(post_override),
                _normalize_nullable_text(toestel_override),
                int(locked or 0),
                _normalize_move_value(move_before_planning_id, None),
                _normalize_move_value(move_after_planning_id, None),
            ),
        )

    conn.commit()


def shift_task_minutes(conn, planning_id: str, delta_minutes: int, planning_run_id=None):
    existing = get_override_for_planning_id(conn, planning_id, planning_run_id)

    current_offset = int(existing["start_offset_minutes"] or 0) if existing else 0
    current_post = existing["post_override"] if existing else None
    current_toestel = existing["toestel_override"] if existing else None
    current_locked = int(existing["locked"] or 0) if existing else 0
    current_before = existing["move_before_planning_id"] if existing else None
    current_after = existing["move_after_planning_id"] if existing else None

    upsert_planning_override(
        conn,
        planning_id=planning_id,
        planning_run_id=planning_run_id,
        start_offset_minutes=current_offset + int(delta_minutes),
        post_override=current_post,
        toestel_override=current_toestel,
        locked=current_locked,
        move_before_planning_id=current_before,
        move_after_planning_id=current_after,
    )


def set_task_lock(conn, planning_id: str, locked: bool, planning_run_id=None):
    existing = get_override_for_planning_id(conn, planning_id, planning_run_id)

    upsert_planning_override(
        conn,
        planning_id=planning_id,
        planning_run_id=planning_run_id,
        start_offset_minutes=int(existing["start_offset_minutes"] or 0) if existing else 0,
        post_override=existing["post_override"] if existing else None,
        toestel_override=existing["toestel_override"] if existing else None,
        locked=1 if locked else 0,
        move_before_planning_id=existing["move_before_planning_id"] if existing else None,
        move_after_planning_id=existing["move_after_planning_id"] if existing else None,
    )


def set_task_post_override(conn, planning_id: str, post_override: str | None, planning_run_id=None):
    existing = get_override_for_planning_id(conn, planning_id, planning_run_id)

    cleaned_post = str(post_override).strip() if post_override is not None else None
    if cleaned_post == "":
        cleaned_post = None

    upsert_planning_override(
        conn,
        planning_id=planning_id,
        planning_run_id=planning_run_id,
        start_offset_minutes=int(existing["start_offset_minutes"] or 0) if existing else 0,
        post_override=cleaned_post,
        toestel_override=existing["toestel_override"] if existing else None,
        locked=int(existing["locked"] or 0) if existing else 0,
        move_before_planning_id=existing["move_before_planning_id"] if existing else None,
        move_after_planning_id=existing["move_after_planning_id"] if existing else None,
    )


def set_task_toestel_override(conn, planning_id: str, toestel_override: str | None, planning_run_id=None):
    existing = get_override_for_planning_id(conn, planning_id, planning_run_id)

    cleaned_toestel = str(toestel_override).strip() if toestel_override is not None else None
    if cleaned_toestel == "":
        cleaned_toestel = None

    upsert_planning_override(
        conn,
        planning_id=planning_id,
        planning_run_id=planning_run_id,
        start_offset_minutes=int(existing["start_offset_minutes"] or 0) if existing else 0,
        post_override=existing["post_override"] if existing else None,
        toestel_override=cleaned_toestel,
        locked=int(existing["locked"] or 0) if existing else 0,
        move_before_planning_id=existing["move_before_planning_id"] if existing else None,
        move_after_planning_id=existing["move_after_planning_id"] if existing else None,
    )


def set_task_move_before(conn, planning_id: str, target_planning_id: str, planning_run_id=None):
    existing = get_override_for_planning_id(conn, planning_id, planning_run_id)

    upsert_planning_override(
        conn,
        planning_id=planning_id,
        planning_run_id=planning_run_id,
        start_offset_minutes=int(existing["start_offset_minutes"] or 0) if existing else 0,
        post_override=existing["post_override"] if existing else None,
        toestel_override=existing["toestel_override"] if existing else None,
        locked=int(existing["locked"] or 0) if existing else 0,
        move_before_planning_id=target_planning_id,
        move_after_planning_id="__CLEAR__",
    )


def set_task_move_after(conn, planning_id: str, target_planning_id: str, planning_run_id=None):
    existing = get_override_for_planning_id(conn, planning_id, planning_run_id)

    upsert_planning_override(
        conn,
        planning_id=planning_id,
        planning_run_id=planning_run_id,
        start_offset_minutes=int(existing["start_offset_minutes"] or 0) if existing else 0,
        post_override=existing["post_override"] if existing else None,
        toestel_override=existing["toestel_override"] if existing else None,
        locked=int(existing["locked"] or 0) if existing else 0,
        move_before_planning_id="__CLEAR__",
        move_after_planning_id=target_planning_id,
    )


def clear_overrides_for_workday(conn, werkdag_iso: str, planning_run_id=None) -> int:
    """
    Verwijdert alle overrides voor taken van één werkdag binnen de actieve planning run.
    Geeft aantal verwijderde overrides terug.
    """
    run_id = _get_run_id(conn, planning_run_id)
    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM planning_overrides
        WHERE planning_run_id = ?
          AND planning_id IN (
              SELECT DISTINCT planning_id
              FROM planning
              WHERE werkdag_iso = ?
          )
        """,
        (run_id, werkdag_iso),
    )

    deleted = cur.rowcount
    conn.commit()
    return deleted


def clear_task_override(conn, planning_id: str, planning_run_id=None):
    init_planning_overrides_table(conn)
    run_id = _get_run_id(conn, planning_run_id)

    conn.execute(
        """
        DELETE FROM planning_overrides
        WHERE planning_run_id = ?
          AND planning_id = ?
        """,
        (run_id, planning_id),
    )
    conn.commit()


def clear_all_planning_overrides(conn, planning_run_id=None):
    init_planning_overrides_table(conn)
    run_id = _get_run_id(conn, planning_run_id)

    conn.execute(
        "DELETE FROM planning_overrides WHERE planning_run_id = ?",
        (run_id,),
    )
    conn.commit()


def _apply_reorder_within_group(group: pd.DataFrame) -> pd.DataFrame:
    if group.empty:
        return group

    group = group.copy().reset_index(drop=True)

    candidate_rows = group[
        (~group["Taak"].astype(str).str.lower().str.contains("pauze", na=False))
        & (~group["Locked"].fillna(False))
    ].copy()

    if candidate_rows.empty:
        return group

    planning_ids_in_group = set(group["Planning ID"].astype(str).tolist())

    for _, move_row in candidate_rows.iterrows():
        source_id = str(move_row["Planning ID"])
        move_before = move_row.get("move_before_planning_id")
        move_after = move_row.get("move_after_planning_id")

        if pd.notna(move_before) and str(move_before).strip():
            target_id = str(move_before).strip()
            if target_id != source_id and target_id in planning_ids_in_group:
                target_locked = bool(
                    group.loc[group["Planning ID"] == target_id, "Locked"].iloc[0]
                )
                if not target_locked:
                    row_to_move = group[group["Planning ID"] == source_id].copy()
                    group = group[group["Planning ID"] != source_id].reset_index(drop=True)
                    target_idx = group.index[group["Planning ID"] == target_id].tolist()
                    if target_idx:
                        insert_at = target_idx[0]
                        top = group.iloc[:insert_at]
                        bottom = group.iloc[insert_at:]
                        group = pd.concat([top, row_to_move, bottom], ignore_index=True)

        elif pd.notna(move_after) and str(move_after).strip():
            target_id = str(move_after).strip()
            if target_id != source_id and target_id in planning_ids_in_group:
                target_locked = bool(
                    group.loc[group["Planning ID"] == target_id, "Locked"].iloc[0]
                )
                if not target_locked:
                    row_to_move = group[group["Planning ID"] == source_id].copy()
                    group = group[group["Planning ID"] != source_id].reset_index(drop=True)
                    target_idx = group.index[group["Planning ID"] == target_id].tolist()
                    if target_idx:
                        insert_at = target_idx[0] + 1
                        top = group.iloc[:insert_at]
                        bottom = group.iloc[insert_at:]
                        group = pd.concat([top, row_to_move, bottom], ignore_index=True)

    return group.reset_index(drop=True)


def apply_planning_overrides(conn, planning_df: pd.DataFrame, planning_run_id=None) -> pd.DataFrame:
    if planning_df is None or planning_df.empty:
        return planning_df

    if "Planning ID" not in planning_df.columns:
        return planning_df

    overrides_df = get_planning_overrides_df(conn, planning_run_id)
    result = planning_df.copy()

    if overrides_df.empty:
        result["Manueel aangepast"] = False
        result["Locked"] = False
        result["Start offset minuten"] = 0
        result["Domino offset minuten"] = 0
        return result

    result = result.merge(
        overrides_df,
        how="left",
        left_on="Planning ID",
        right_on="planning_id",
    )

    result["start_offset_minutes"] = (
        pd.to_numeric(result["start_offset_minutes"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    result["locked"] = (
        pd.to_numeric(result["locked"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    result["Start"] = pd.to_datetime(result["Start"], errors="coerce")
    result["Einde"] = pd.to_datetime(result["Einde"], errors="coerce")

    has_post_override = result["post_override"].notna() & (
        result["post_override"].astype(str).str.strip() != ""
    )
    if has_post_override.any():
        result.loc[has_post_override, "Post"] = result.loc[
            has_post_override, "post_override"
        ]

    has_toestel_override = result["toestel_override"].notna() & (
        result["toestel_override"].astype(str).str.strip() != ""
    )
    if has_toestel_override.any():
        result.loc[has_toestel_override, "Toestel"] = result.loc[
            has_toestel_override, "toestel_override"
        ]

    result["Locked"] = result["locked"] == 1
    result["Start offset minuten"] = result["start_offset_minutes"]
    result["Domino offset minuten"] = 0

    result = result.sort_values(
        ["Werkdag_iso", "Post", "Start", "Einde", "Taak"]
    ).reset_index(drop=True)

    reordered_groups = []
    for _, group in result.groupby(["Werkdag_iso", "Post"], sort=False):
        reordered_groups.append(_apply_reorder_within_group(group))

    result = pd.concat(reordered_groups, ignore_index=True)

    grouped_rows = []

    for _, group in result.groupby(["Werkdag_iso", "Post"], sort=False):
        group = group.copy().reset_index(drop=True)

        has_reorder_in_group = (
            (
                group["move_before_planning_id"].notna()
                & (group["move_before_planning_id"].astype(str).str.strip() != "")
            )
            | (
                group["move_after_planning_id"].notna()
                & (group["move_after_planning_id"].astype(str).str.strip() != "")
            )
        ).any()

        if has_reorder_in_group:
            cursor = group["Start"].min()

            for i in range(len(group)):
                is_locked = bool(group.at[i, "Locked"])

                if is_locked:
                    cursor = max(cursor, group.at[i, "Einde"])
                    continue

                old_start = group.at[i, "Start"]
                old_end = group.at[i, "Einde"]
                duration = old_end - old_start

                group.at[i, "Start"] = cursor
                group.at[i, "Einde"] = cursor + duration
                cursor = group.at[i, "Einde"]

        domino_running = 0
        manual_shift_seen = False

        for i in range(len(group)):
            own_offset = int(group.at[i, "start_offset_minutes"] or 0)
            is_locked = bool(group.at[i, "Locked"])

            if is_locked:
                group.at[i, "Domino offset minuten"] = 0
                domino_running = 0
                manual_shift_seen = False
                continue

            if own_offset != 0:
                group.at[i, "Start"] = group.at[i, "Start"] + timedelta(minutes=own_offset)
                group.at[i, "Einde"] = group.at[i, "Einde"] + timedelta(minutes=own_offset)
                group.at[i, "Domino offset minuten"] = own_offset
                domino_running = own_offset
                manual_shift_seen = True
                continue

            if manual_shift_seen and domino_running != 0:
                group.at[i, "Start"] = group.at[i, "Start"] + timedelta(minutes=domino_running)
                group.at[i, "Einde"] = group.at[i, "Einde"] + timedelta(minutes=domino_running)
                group.at[i, "Domino offset minuten"] = domino_running

        grouped_rows.append(group)

    result = pd.concat(grouped_rows, ignore_index=True)

    final_has_post_override = result["post_override"].notna() & (
        result["post_override"].astype(str).str.strip() != ""
    )
    final_has_toestel_override = result["toestel_override"].notna() & (
        result["toestel_override"].astype(str).str.strip() != ""
    )
    final_has_reorder = (
        (
            result["move_before_planning_id"].notna()
            & (result["move_before_planning_id"].astype(str).str.strip() != "")
        )
        | (
            result["move_after_planning_id"].notna()
            & (result["move_after_planning_id"].astype(str).str.strip() != "")
        )
    )

    result["Manueel aangepast"] = (
        (result["start_offset_minutes"] != 0)
        | (result["Domino offset minuten"] != 0)
        | final_has_post_override
        | final_has_toestel_override
        | final_has_reorder
    )

    drop_cols = [
        "planning_run_id",
        "planning_id",
        "start_offset_minutes",
        "post_override",
        "toestel_override",
        "locked",
        "move_before_planning_id",
        "move_after_planning_id",
        "updated_at",
    ]

    result = result.drop(columns=[c for c in drop_cols if c in result.columns])

    return result