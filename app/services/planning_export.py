from io import BytesIO

import pandas as pd


EXPORT_COLUMNS = [
    "Planning ID",
    "Werkdag_iso",
    "Werkdag",
    "Start",
    "Einde",
    "Post",
    "Recept",
    "Taak",
    "Toestel",
    "Actieve tijd",
    "Passieve tijd",
    "Totale duur",
    "Voorkeur offset",
    "Min offset",
    "Max offset",
    "Gekozen offset",
    "Planner reden",
    "Conflict",
    "Conflict reden",
    "Pakket ID",
    "Pakket code",
    "Pakket volgorde",
    "Pakket status",
    "Planning fase",
]


def export_planning_run_to_excel(
    conn,
    planning_run_id: int,
    werkdag: str,
) -> BytesIO:
    rows = conn.execute(
        """
        SELECT
            planning_id,
            werkdag_iso,
            recept,
            taak,
            post,
            toestel,
            start,
            einde,
            actieve_tijd,
            passieve_tijd,
            totale_duur,
            locked,
            manueel_aangepast,
            start_offset_minuten,
            row_json
        FROM planning_saved
        WHERE planning_run_id = ?
        AND werkdag_iso = ?
        ORDER BY werkdag_iso, start, post, taak
        """,
        (planning_run_id, werkdag),
    ).fetchall()

    data = []

    for row in rows:
        import json

        if row["row_json"]:
            data.append(json.loads(row["row_json"]))
        else:
            data.append(
                {
                    "Planning ID": row["planning_id"],
                    "Werkdag_iso": row["werkdag_iso"],
                    "Recept": row["recept"],
                    "Taak": row["taak"],
                    "Post": row["post"],
                    "Toestel": row["toestel"],
                    "Start": row["start"],
                    "Einde": row["einde"],
                    "Actieve tijd": row["actieve_tijd"],
                    "Passieve tijd": row["passieve_tijd"],
                    "Totale duur": row["totale_duur"],
                    "Locked": bool(row["locked"]),
                    "Manueel aangepast": bool(row["manueel_aangepast"]),
                    "Start offset minuten": row["start_offset_minuten"],
                }
            )

    planner_df = pd.DataFrame(data)

    for col in EXPORT_COLUMNS:
        if col not in planner_df.columns:
            planner_df[col] = None

    planner_df = planner_df[EXPORT_COLUMNS]

    mens_df = planner_df.copy()

    mens_df["Mens datum"] = ""
    mens_df["Mens start"] = ""
    mens_df["Mens post"] = ""
    mens_df["Mens opmerking"] = ""

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        planner_df.to_excel(writer, sheet_name="Planner", index=False)
        mens_df.to_excel(writer, sheet_name="Menselijke planning", index=False)

        info_df = pd.DataFrame(
            [
                {
                    "Uitleg": "Pas in tabblad 'Menselijke planning' alleen Mens datum, Mens start, Mens post en Mens opmerking aan.",
                },
                {
                    "Uitleg": "Planning ID niet wijzigen. Die gebruiken we later om planner en menselijke planning te vergelijken.",
                },
            ]
        )
        info_df.to_excel(writer, sheet_name="Info", index=False)

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "A2"

            for column_cells in ws.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter

                for cell in column_cells:
                    value = cell.value
                    if value is not None:
                        max_length = max(max_length, len(str(value)))

                ws.column_dimensions[column_letter].width = min(max(max_length + 2, 10), 45)

    output.seek(0)
    return output

def export_visible_day_rows_to_excel(
    rows: list[dict],
    werkdag: str,
) -> BytesIO:
    visible_rows = [
        row for row in rows
        if str(row.get("Werkdag_iso") or "") == werkdag
    ]

    planner_df = pd.DataFrame(visible_rows)

    for col in EXPORT_COLUMNS:
        if col not in planner_df.columns:
            planner_df[col] = None

    planner_df = planner_df[EXPORT_COLUMNS]

    mens_df = planner_df.copy()
    mens_df["Mens datum"] = ""
    mens_df["Mens start"] = ""
    mens_df["Mens post"] = ""
    mens_df["Mens opmerking"] = ""

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        planner_df.to_excel(writer, sheet_name="Planner", index=False)
        mens_df.to_excel(writer, sheet_name="Menselijke planning", index=False)

        info_df = pd.DataFrame(
            [
                {
                    "Uitleg": "Deze export bevat exact de planningrijen die vanuit de frontend werden doorgestuurd.",
                },
                {
                    "Uitleg": "Pas alleen Mens datum, Mens start, Mens post en Mens opmerking aan.",
                },
            ]
        )
        info_df.to_excel(writer, sheet_name="Info", index=False)

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "A2"

            for column_cells in ws.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter

                for cell in column_cells:
                    value = cell.value
                    if value is not None:
                        max_length = max(max_length, len(str(value)))

                ws.column_dimensions[column_letter].width = min(max(max_length + 2, 10), 45)

    output.seek(0)
    return output