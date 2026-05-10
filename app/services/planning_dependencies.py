import re
from datetime import datetime
from typing import Any


TASK_SEQUENCE_RE = re.compile(r"^(.+_\d+)_(\d+)$")


def parse_task_sequence_code(value: str | None) -> tuple[str, int] | None:
    if not value:
        return None

    match = TASK_SEQUENCE_RE.match(value.strip())
    if not match:
        return None

    group = match.group(1)
    step = int(match.group(2))

    return group, step


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    return None


def apply_dependency_warnings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_group_step: dict[tuple[str, int], dict[str, Any]] = {}

    for row in rows:
        task_code = row.get("Planning ID") or row.get("Taak")
        parsed = parse_task_sequence_code(task_code)

        if not parsed:
            continue

        group, step = parsed
        by_group_step[(group, step)] = row

    for row in rows:
        row["Dependency status"] = "ok"
        row["Dependency warning"] = None
        row["Dependency previous task"] = None
        row["Dependency previous end"] = None

        task_code = row.get("Planning ID") or row.get("Taak")
        parsed = parse_task_sequence_code(task_code)
        print("DEPENDENCY DEBUG", {
            "task_code": task_code,
            "parsed": parsed,
            "start": row.get("Start"),
            "end": row.get("Einde"),
            "post": row.get("Post"),
        })

        if not parsed:
            continue

        group, step = parsed

        if step <= 1:
            continue

        previous_row = by_group_step.get((group, step - 1))
        print("PREVIOUS DEBUG", {
            "task_code": task_code,
            "looking_for": f"{group}_{step - 1}",
            "found_previous": bool(previous_row),
            "previous_end": previous_row.get("Einde") if previous_row else None,
        })

        if not previous_row:
            row["Dependency status"] = "warning"
            row["Dependency warning"] = (
                f"{task_code} heeft geen vorige taak {group}_{step - 1} gevonden."
            )
            row["Dependency previous task"] = f"{group}_{step - 1}"
            continue

        previous_end = parse_datetime(previous_row.get("Einde"))
        current_start = parse_datetime(row.get("Start"))

        row["Dependency previous task"] = (
            previous_row.get("Taak") or previous_row.get("Planning ID")
        )
        row["Dependency previous end"] = previous_row.get("Einde")

        if not previous_end or not current_start:
            row["Dependency status"] = "warning"
            row["Dependency warning"] = (
                f"Kan afhankelijkheid voor {task_code} niet controleren door ontbrekende tijden."
            )
            continue

        if current_start < previous_end:
            previous_label = previous_row.get("Taak") or previous_row.get("Planning ID")
            previous_post = previous_row.get("Post") or "onbekende post"
            previous_time = previous_end.strftime("%H:%M")

            row["Dependency status"] = "blocked"
            row["Dependency warning"] = (
                f"{task_code} mag pas starten na {previous_label} "
                f"op post {previous_post}, klaar om {previous_time}."
            )

    return rows