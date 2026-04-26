from io import BytesIO
from datetime import datetime, timedelta

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas


def format_minutes_to_hhmm(minutes: int) -> str:
    minutes = int(minutes or 0)
    uren = minutes // 60
    resten = minutes % 60
    return f"{uren:02d}:{resten:02d}"


def hex_to_rgb_tuple(hex_color: str):
    try:
        hex_color = str(hex_color or "").strip().lstrip("#")
        if len(hex_color) == 3:
            hex_color = "".join([c * 2 for c in hex_color])

        if len(hex_color) == 6:
            r = int(hex_color[0:2], 16) / 255
            g = int(hex_color[2:4], 16) / 255
            b = int(hex_color[4:6], 16) / 255
            return (r, g, b)
    except Exception:
        pass

    return (0.12, 0.47, 0.71)


def draw_wrapped_text(
    c,
    text,
    x,
    y,
    max_width,
    line_height=8,
    font_name="Helvetica",
    font_size=7,
    max_lines=2,
):
    c.setFont(font_name, font_size)
    words = str(text or "").split()
    lines = []
    current = ""

    for word in words:
        candidate = f"{current} {word}".strip()
        if stringWidth(candidate, font_name, font_size) <= max_width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
            if len(lines) >= max_lines:
                break

    if current and len(lines) < max_lines:
        lines.append(current)

    for i, line in enumerate(lines[:max_lines]):
        c.drawString(x, y - i * line_height, line)


def _is_pause_task(taak: str) -> bool:
    return "pauze" in str(taak or "").lower()


def _draw_page_header(c, page_width, page_height, margin, gekozen_dag_label, gekozen_post):
    top_y = page_height - margin

    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 16)
    titel = f"KitchenMotor - Dagplanning {gekozen_dag_label}"
    c.drawString(margin, top_y, titel)

    c.setFont("Helvetica", 9)
    subtitel = (
        f"Post: {gekozen_post}    |    Gegenereerd op "
        f"{datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )
    c.drawString(margin, top_y - 7 * mm, subtitel)

    return top_y


def build_visual_dagplanning_pdf(
    df,
    gekozen_dag_label: str,
    gekozen_post: str = "Alle posten",
    kleurmap=None,
) -> bytes:
    buffer = BytesIO()
    page_width, page_height = landscape(A4)
    c = canvas.Canvas(buffer, pagesize=(page_width, page_height))

    margin = 10 * mm

    top_y = _draw_page_header(
        c=c,
        page_width=page_width,
        page_height=page_height,
        margin=margin,
        gekozen_dag_label=gekozen_dag_label,
        gekozen_post=gekozen_post,
    )

    if df.empty:
        c.setFont("Helvetica", 11)
        c.drawString(margin, top_y - 18 * mm, "Geen taken gevonden voor deze selectie.")
        c.save()
        pdf = buffer.getvalue()
        buffer.close()
        return pdf

    temp = df.copy().sort_values(["Post", "Start", "Einde", "Taak"]).reset_index(drop=True)

    temp["Start"] = temp["Start"].astype("datetime64[ns]")
    temp["Einde"] = temp["Einde"].astype("datetime64[ns]")

    min_start = temp["Start"].min().to_pydatetime()
    max_end = temp["Einde"].max().to_pydatetime()

    # Rond de tijdsas mooi af op hele uren
    axis_start = min_start.replace(minute=0, second=0, microsecond=0)
    if axis_start > min_start:
        axis_start = axis_start - timedelta(hours=1)

    axis_end = max_end.replace(minute=0, second=0, microsecond=0)
    if axis_end < max_end:
        axis_end = axis_end + timedelta(hours=1)

    total_minutes = max(60, int((axis_end - axis_start).total_seconds() / 60))

    # Compactere layout
    label_x = margin
    label_width = 42 * mm
    timeinfo_width = 22 * mm
    timeline_x = label_x + label_width + timeinfo_width + 4 * mm
    timeline_width = page_width - timeline_x - margin

    start_y = top_y - 18 * mm
    axis_y = start_y
    row_height = 10 * mm
    block_height = 6.5 * mm

    def x_from_dt(dt):
        minutes = int((dt - axis_start).total_seconds() / 60)
        return timeline_x + (minutes / total_minutes) * timeline_width

    def draw_axis(current_axis_y):
        c.setStrokeColor(colors.HexColor("#C7D0D9"))
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 7)

        c.line(timeline_x, current_axis_y, timeline_x + timeline_width, current_axis_y)

        uur_cursor = axis_start
        while uur_cursor <= axis_end:
            x = x_from_dt(uur_cursor)
            c.line(x, current_axis_y - 1.5 * mm, x, current_axis_y + 1.5 * mm)
            c.drawCentredString(x, current_axis_y + 3 * mm, uur_cursor.strftime("%H:%M"))
            uur_cursor += timedelta(hours=1)

    draw_axis(axis_y)

    current_y = start_y - 8 * mm

    for _, row in temp.iterrows():
        if current_y < 18 * mm:
            c.showPage()
            top_y = _draw_page_header(
                c=c,
                page_width=page_width,
                page_height=page_height,
                margin=margin,
                gekozen_dag_label=gekozen_dag_label,
                gekozen_post=gekozen_post,
            )
            start_y = top_y - 18 * mm
            axis_y = start_y
            draw_axis(axis_y)
            current_y = start_y - 8 * mm

        post = str(row.get("Post", "") or "")
        taak = str(row.get("Taak", "") or "")
        recept = str(row.get("Recept", "") or "")
        toestel = str(row.get("Toestel", "") or "")
        start = row["Start"].to_pydatetime() if hasattr(row["Start"], "to_pydatetime") else row["Start"]
        einde = row["Einde"].to_pydatetime() if hasattr(row["Einde"], "to_pydatetime") else row["Einde"]

        x1 = x_from_dt(start)
        x2 = x_from_dt(einde)
        width = max(12, x2 - x1)

        is_pause = _is_pause_task(taak)

        if is_pause:
            block_color = colors.HexColor("#C8C8C8")
        else:
            rgb = hex_to_rgb_tuple(kleurmap.get(post, "#1f77b4") if kleurmap else "#1f77b4")
            block_color = colors.Color(*rgb)

        # Linkerkolommen
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 7)
        c.drawString(label_x, current_y + 5, post[:26])

        c.setFont("Helvetica", 6.5)
        c.drawString(label_x, current_y + 1, taak[:34])

        c.setFont("Helvetica", 6.5)
        c.drawString(label_x + label_width, current_y + 3, f"{start.strftime('%H:%M')}")
        c.drawRightString(
            label_x + label_width + timeinfo_width - 2 * mm,
            current_y + 3,
            f"{einde.strftime('%H:%M')}",
        )

        # Rasterlijn
        c.setStrokeColor(colors.HexColor("#EEEEEE"))
        c.setLineWidth(0.4)
        c.line(label_x, current_y - 1.5 * mm, page_width - margin, current_y - 1.5 * mm)

        # Blok
        c.setFillColor(block_color)
        c.setStrokeColor(block_color)
        c.roundRect(x1, current_y, width, block_height, 1.5 * mm, fill=1, stroke=0)

        # Tekst in blok: compacter
        c.setFillColor(colors.white)

        if is_pause:
            tekst = "Pauze"
        else:
            onderdelen = [taak]
            if recept and recept.strip() and recept.strip() != "-":
                onderdelen.append(recept)
            if toestel and toestel.strip().lower() not in {"", "geen", "-", "n.v.t.", "nvt"}:
                onderdelen.append(f"⚙ {toestel}")
            tekst = " | ".join(onderdelen)

        draw_wrapped_text(
            c,
            tekst,
            x1 + 1.5 * mm,
            current_y + 4.8,
            max(width - 3 * mm, 12),
            line_height=6.5,
            font_name="Helvetica-Bold" if not is_pause else "Helvetica",
            font_size=6.5,
            max_lines=1 if width < 45 else 2,
        )

        current_y -= row_height

    c.save()
    pdf = buffer.getvalue()
    buffer.close()
    return pdf