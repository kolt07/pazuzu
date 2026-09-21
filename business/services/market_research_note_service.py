# -*- coding: utf-8 -*-
"""Текстово-числові висновки дослідження ринку та PDF для відправки в Telegram."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

KIND_LABELS = {
    "land": "земля",
    "real_estate": "нерухомість",
    "mixed": "земля з будівлею",
    "other": "інше",
}
METRIC_LABELS = {
    "price_uah": "ціна",
    "price_per_m2_uah": "ціна за м²",
    "price_per_sotka_uah": "ціна за сотку",
}


def _num(v: Any) -> Optional[float]:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n


def _fmt_int(n: float) -> str:
    return f"{int(round(n)):,}".replace(",", "\u00a0")


def _fmt_uah(n: float) -> str:
    if abs(n) >= 1_000_000:
        s = f"{n / 1_000_000:.1f}".replace(".", ",")
        return f"{s}\u00a0млн грн"
    return f"{_fmt_int(n)}\u00a0грн"


def _fmt_unit_price(n: float, metric: str) -> str:
    if metric == "price_uah":
        return _fmt_uah(n)
    if metric == "price_per_sotka_uah":
        return f"{_fmt_int(n)}\u00a0грн/с"
    return f"{_fmt_int(n)}\u00a0грн/м²"


def _join_uk(parts: List[str]) -> str:
    parts = [p for p in parts if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return ", ".join(parts[:-1]) + " і " + parts[-1]


def _counter_phrase(mapping: Dict[str, Any], *, unit: str = "оголошень", limit: int = 8) -> str:
    items = [(k, v) for k, v in (mapping or {}).items() if str(k).strip() and v]
    items.sort(key=lambda x: int(x[1] or 0), reverse=True)
    items = items[:limit]
    if not items:
        return ""
    bits = [f"{name} — {int(n)} {unit}" for name, n in items]
    return _join_uk(bits)


def _price_sentence(st: Dict[str, Any], metric: str) -> Optional[str]:
    n = int(st.get("n") or 0)
    if n <= 0:
        return None
    label = METRIC_LABELS.get(metric, "ціна")
    mn, mx = _num(st.get("min")), _num(st.get("max"))
    if n < 5 or st.get("median") is None:
        if mn is None or mx is None:
            return f"Є {n} пропозицій із відомою {label}, але цього замало, щоб говорити про типову величину."
        return (
            f"Є {n} пропозицій із відомою {label}: від {_fmt_unit_price(mn, metric)} "
            f"до {_fmt_unit_price(mx, metric)}. Для типової оцінки потрібно щонайменше 5 цін."
        )
    med = _num(st.get("median"))
    p25, p75 = _num(st.get("p25")), _num(st.get("p75"))
    outliers = int(st.get("iqr_outliers") or 0)
    parts = [
        f"Типова {label} (половина пропозицій дешевша) — {_fmt_unit_price(med, metric)}."
    ]
    if p25 is not None and p75 is not None:
        parts.append(
            f"У більшості — від {_fmt_unit_price(p25, metric)} до {_fmt_unit_price(p75, metric)}."
        )
    if mn is not None and mx is not None:
        parts.append(f"Крайні значення: {_fmt_unit_price(mn, metric)} … {_fmt_unit_price(mx, metric)}.")
    if outliers:
        parts.append(f"Окремо вибиваються {outliers} пропозиції.")
    return " ".join(parts)


def conclusions_from_report(report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Секції висновків без жаргону (n, p25, IQR, медіана як термін у заголовках таблиць)."""
    report = report if isinstance(report, dict) else {}
    p = report.get("passport") or {}
    n = int(p.get("n") or 0)
    sources = p.get("sources") or {}
    src_bits = []
    for key, label in (("olx", "OLX"), ("prozorro", "ProZorro")):
        if sources.get(key):
            src_bits.append(f"{label} — {int(sources[key])}")
    deals = []
    for d in p.get("deal_types") or []:
        deals.append("оренда" if d == "rent" else "продаж")
    depth = p.get("depth_days")
    depth_txt = "без обмеження за датою" if depth is None else f"за останні {int(depth)} дн."
    dates = ""
    if p.get("date_from") or p.get("date_to"):
        dates = f" Дати оновлення у вибірці: {p.get('date_from') or '—'} — {p.get('date_to') or '—'}."

    overview = [
        f"У вибірці {n} оголошень" + (f" ({_join_uk(src_bits)})" if src_bits else "") + ".",
        f"Угода: {_join_uk(deals) or 'продаж'}. Період пошуку: {depth_txt}.{dates}",
    ]
    filt = str(p.get("filter_summary") or "").strip()
    if filt:
        overview.append(f"Умови відбору: {filt}.")

    sections: List[Dict[str, Any]] = [{"title": "Коротко", "paragraphs": overview}]

    comp = report.get("composition") or {}
    who = []
    types = _counter_phrase(comp.get("property_type") or {}, unit="шт.")
    if types:
        who.append(f"За типом: {types}.")
    regions = _counter_phrase(comp.get("region") or {}, unit="шт.")
    if regions:
        who.append(f"За областями: {regions}.")
    cities = _counter_phrase(comp.get("city_top10") or {}, unit="шт.", limit=10)
    if cities:
        who.append(f"Населені пункти з найбільшою кількістю: {cities}.")
    if who:
        sections.append({"title": "Що ввійшло у вибірку", "paragraphs": who})

    price_paras: List[str] = []
    for block in report.get("price_distribution") or []:
        deal = "оренди" if block.get("deal_type") == "rent" else "продажу"
        price_paras.append(f"По {deal} — {int(block.get('n') or 0)} оголошень.")
        by_kind = block.get("by_kind") or {}
        for kind, metrics in by_kind.items():
            klabel = KIND_LABELS.get(kind, kind)
            for metric in ("price_uah", "price_per_m2_uah", "price_per_sotka_uah"):
                sent = _price_sentence(metrics.get(metric) or {}, metric)
                if sent:
                    price_paras.append(f"{klabel.capitalize()}: {sent}")
    if price_paras:
        sections.append({"title": "Ціни", "paragraphs": price_paras})

    slices = report.get("slices") or {}
    slice_paras: List[str] = []
    region_rows = slices.get("region_median_price_uah") or []
    if region_rows:
        bits = []
        for row in region_rows[:8]:
            med = _num(row.get("median"))
            if not row.get("name") or med is None:
                continue
            bits.append(f"{row['name']} — {_fmt_uah(med)} ({int(row.get('n') or 0)} шт.)")
        if bits:
            slice_paras.append("Типова ціна за областями: " + _join_uk(bits) + ".")
    buckets = slices.get("area_buckets") or []
    if buckets:
        bits = []
        for row in buckets:
            med = _num(row.get("median"))
            if not row.get("bucket"):
                continue
            extra = f", типова ціна {_fmt_uah(med)}" if med is not None else ""
            bits.append(f"{row['bucket']} — {int(row.get('n') or 0)} шт.{extra}")
        if bits:
            slice_paras.append("За площею: " + _join_uk(bits) + ".")
    tags = slices.get("tags_top") or []
    if tags:
        bits = []
        for row in tags[:8]:
            med = _num(row.get("median"))
            if not row.get("tag"):
                continue
            extra = f", типова ціна {_fmt_uah(med)}" if med is not None else ""
            bits.append(f"{row['tag']} — {int(row.get('n') or 0)} шт.{extra}")
        if bits:
            slice_paras.append("Часті ознаки в оголошеннях: " + _join_uk(bits) + ".")
    if slice_paras:
        sections.append({"title": "Де дорожче і що частіше", "paragraphs": slice_paras})

    cmp_paras: List[str] = []
    for row in report.get("market_comparison") or []:
        city = row.get("city") or ""
        region = row.get("region") or ""
        place = city + (f" ({region})" if region else "")
        sample = _num(row.get("sample_median"))
        market = _num(row.get("market_median"))
        delta = _num(row.get("delta_pct"))
        if not place or sample is None or market is None:
            continue
        vs = "на рівні ринку"
        if delta is not None:
            if delta > 3:
                vs = f"приблизно на {abs(delta):.0f}% вище за ринок"
            elif delta < -3:
                vs = f"приблизно на {abs(delta):.0f}% нижче за ринок"
        level = "по місту" if row.get("scope") == "city" else "по області"
        cmp_paras.append(
            f"{place}: у вибірці типова ціна за одиницю площі {_fmt_int(sample)} грн, "
            f"{level} на ринку — {_fmt_int(market)} грн ({vs}; {int(row.get('sample_n') or 0)} пропозицій)."
        )
    if cmp_paras:
        sections.append({"title": "Порівняння з ринком", "paragraphs": cmp_paras})

    lims = [str(x).strip() for x in (report.get("limitations") or []) if str(x).strip()]
    if lims:
        sections.append({"title": "На що зважати", "paragraphs": lims})

    return {"sections": sections}


def _cyrillic_font_path() -> Optional[Path]:
    here = Path(__file__).resolve()
    candidates = [
        here.parents[1] / "assets" / "fonts" / "DejaVuSans.ttf",
        Path(r"C:\Windows\Fonts\arial.ttf"),
        Path(r"C:\Windows\Fonts\calibri.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
        Path("/usr/share/fonts/truetype/freefont/FreeSans.ttf"),
    ]
    for p in candidates:
        if p.is_file():
            return p
    return None


def build_research_note_pdf(report: Optional[Dict[str, Any]], *, title: str = "Довідка з дослідження ринку") -> bytes:
    from fpdf import FPDF

    font_path = _cyrillic_font_path()
    if not font_path:
        raise RuntimeError("Немає шрифту з кирилицею для PDF (Arial / DejaVu).")

    conclusions = conclusions_from_report(report)
    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()
    pdf.set_left_margin(16)
    pdf.set_right_margin(16)
    pdf.add_font("NoteSans", "", str(font_path))
    pdf.set_font("NoteSans", size=16)
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(pdf.epw, 8, title)
    pdf.ln(2)
    pdf.set_font("NoteSans", size=11)
    for section in conclusions.get("sections") or []:
        pdf.ln(3)
        pdf.set_font("NoteSans", size=13)
        pdf.set_x(pdf.l_margin)
        pdf.multi_cell(pdf.epw, 7, str(section.get("title") or ""))
        pdf.set_font("NoteSans", size=11)
        for para in section.get("paragraphs") or []:
            text = str(para).replace("\u00a0", " ")
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(pdf.epw, 6, text)
            pdf.ln(1)
    buf = BytesIO()
    raw = pdf.output()
    if isinstance(raw, (bytes, bytearray)):
        buf.write(bytes(raw))
    else:
        pdf.output(buf)
    return buf.getvalue()
