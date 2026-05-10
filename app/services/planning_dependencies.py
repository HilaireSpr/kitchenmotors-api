import re
from datetime import datetime
from typing import Any


TASK_SEQUENCE_RE = re.compile(r"^(.+)_(\d+)$")


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


def get_task_code(row: dict[str, Any]) -> str | None:
    taak = row.get("Taak")

    if isinstance(taak, str) and taak.strip():
        return taak.split(" - ")[0].strip()

    return None


def apply_dependency_warnings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_group_step: dict[tuple[str, int], dict[str, Any]] = {}
    row_index_by_task_code: dict[str, int] = {}

    for index, row in enumerate(rows):
        task_code = get_task_code(row)

        if task_code:
            row_index_by_task_code[task_code] = index

        parsed = parse_task_sequence_code(task_code)

        if not parsed:
            continue

        group, step = parsed
        by_group_step[(group, step)] = row

    for index, row in enumerate(rows):
        row["Dependency status"] = "ok"
        row["Dependency warning"] = None
        row["Dependency previous task"] = None
        row["Dependency previous end"] = None

        task_code = get_task_code(row)
        parsed = parse_task_sequence_code(task_code)

        if not parsed:
            continue

        group, step = parsed

        if step <= 1:
            continue

        previous_task_code = f"{group}_{step - 1}"
        previous_row = by_group_step.get((group, step - 1))

        if not previous_row:
            row["Dependency status"] = "warning"
            row["Dependency warning"] = (
                f"{task_code} heeft geen vorige taak {previous_task_code} gevonden."
            )
            row["Dependency previous task"] = previous_task_code
            continue

        previous_label = get_task_code(previous_row) or previous_task_code
        previous_post = previous_row.get("Post") or "onbekende post"

        row["Dependency previous task"] = previous_label
        row["Dependency previous end"] = previous_row.get("Einde")

        previous_index = row_index_by_task_code.get(previous_label)

        if previous_index is not None and index < previous_index:
            row["Dependency status"] = "blocked"
            row["Dependency warning"] = (
                f"{task_code} staat vóór {previous_label}, "
                f"maar {previous_label} moet eerst gebeuren op post {previous_post}."
            )
            continue

        previous_end = parse_datetime(previous_row.get("Einde"))
        current_start = parse_datetime(row.get("Start"))

        if not previous_end or not current_start:
            row["Dependency status"] = "warning"
            row["Dependency warning"] = (
                f"Kan afhankelijkheid voor {task_code} niet controleren door ontbrekende tijden."
            )
            continue

        if current_start < previous_end:
            previous_time = previous_end.strftime("%H:%M")

            row["Dependency status"] = "blocked"
            row["Dependency warning"] = (
                f"{task_code} mag pas starten na {previous_label} "
                f"op post {previous_post}, klaar om {previous_time}."
            )

    return rows