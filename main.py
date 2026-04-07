import os
import csv
import io
import re
import time
import math
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, date
from collections import defaultdict

# =========================
# CONFIG
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
EREDES_CSV_URL = os.getenv("EREDES_CSV_URL", "").strip()

PERDAS = float(os.getenv("PERDAS", "0.15"))
FADEQ = float(os.getenv("FADEQ", "1.02"))
AC = float(os.getenv("AC", "0.0055"))
GGS = float(os.getenv("GGS", "0.0100"))
TAR_VAZIO = float(os.getenv("TAR_VAZIO", "0.0158"))
TAR_FV = float(os.getenv("TAR_FV", "0.0835"))

MIN_DAY_COVERAGE = float(os.getenv("MIN_DAY_COVERAGE", "0.80"))  # 80%
EXPECTED_INTERVALS_PER_DAY = 96
MIN_INTERVALS_VALID_DAY = math.ceil(EXPECTED_INTERVALS_PER_DAY * MIN_DAY_COVERAGE)

OMIE_URL_TEMPLATE = "https://www.omie.es/sites/default/files/dados/AGNO_{year}/MES_{month}/TXT/marginalpdbcpt_{yyyymmdd}.1"

# =========================
# HELPERS
# =========================
def fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()

    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue

    return raw.decode("utf-8", errors="replace")


def normalize_google_sheets_csv_url(url: str) -> str:
    if not url:
        return url

    if "docs.google.com/spreadsheets" in url and "/export?" not in url:
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        if m:
            sheet_id = m.group(1)
            gid_match = re.search(r"[#&?]gid=([0-9]+)", url)
            gid = gid_match.group(1) if gid_match else "0"
            return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    return url


def parse_float_pt(value: str) -> float:
    if value is None:
        return 0.0
    s = str(value).strip().replace("\xa0", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def format_eur(v: float, digits: int = 3) -> str:
    s = f"{v:.{digits}f}".rstrip("0").rstrip(".")
    return s


def is_vazio(dt: datetime) -> bool:
    h = dt.hour
    return h >= 22 or h < 8


def add_cache_buster(url: str) -> str:
    if not url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}t={int(time.time())}"


# =========================
# OMIE
# =========================
def build_omie_url(target_date: date) -> str:
    yyyymmdd = target_date.strftime("%Y%m%d")
    return OMIE_URL_TEMPLATE.format(
        year=target_date.strftime("%Y"),
        month=target_date.strftime("%m"),
        yyyymmdd=yyyymmdd,
    )


def parse_omie_prices(text: str) -> dict[int, float]:
    prices = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("*"):
            continue

        parts = re.split(r"[;\t]+", line)
        if len(parts) < 6:
            continue

        try:
            hour = int(parts[2])
            price = float(parts[5].replace(",", "."))
            if 1 <= hour <= 24:
                prices[hour - 1] = price  # 0..23
        except Exception:
            continue

    if len(prices) < 24:
        raise ValueError("Não foi possível ler as 24 horas do ficheiro OMIE.")

    return prices


def load_omie_day(target_date: date) -> dict[int, float]:
    url = build_omie_url(target_date)
    txt = fetch_text(url)
    return parse_omie_prices(txt)


def omie_mwh_to_g9_kwh(omie_eur_mwh: float, vazio: bool) -> float:
    base_kwh = omie_eur_mwh / 1000.0
    tar = TAR_VAZIO if vazio else TAR_FV
    return (base_kwh * FADEQ * (1 + PERDAS)) + AC + GGS + tar


def calc_today_g9_prices(omie_prices: dict[int, float]) -> tuple[float, float, float]:
    vazio_vals = []
    fv_vals = []

    for hour, omie_price in omie_prices.items():
        price_kwh = omie_mwh_to_g9_kwh(omie_price, vazio=(hour >= 22 or hour < 8))
        if hour >= 22 or hour < 8:
            vazio_vals.append(price_kwh)
        else:
            fv_vals.append(price_kwh)

    avg_vazio = sum(vazio_vals) / len(vazio_vals) if vazio_vals else 0.0
    avg_fv = sum(fv_vals) / len(fv_vals) if fv_vals else 0.0
    omie_avg = sum(omie_prices.values()) / len(omie_prices) if omie_prices else 0.0
    return omie_avg, avg_vazio, avg_fv


# =========================
# E-REDES
# =========================
def detect_csv_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","


def find_header_row(lines: list[str]) -> int:
    for i, line in enumerate(lines[:20]):
        low = line.lower()
        if "data" in low and "hora" in low:
            return i
    return 0


def parse_eredes_datetime(data_str: str, hora_str: str) -> datetime:
    data_str = data_str.strip()
    hora_str = hora_str.strip()

    for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(f"{data_str} {hora_str}", fmt)
        except Exception:
            pass

    raise ValueError(f"Data/hora inválida: {data_str} {hora_str}")


def load_eredes_15m_data() -> list[dict]:
    url = normalize_google_sheets_csv_url(EREDES_CSV_URL)
    if not url:
        print("DEBUG E-REDES: variável EREDES_CSV_URL vazia.")
        return []

    url = add_cache_buster(url)
    csv_text = fetch_text(url)

    lines = csv_text.splitlines()
    if not lines:
        return []

    header_idx = find_header_row(lines)
    content = "\n".join(lines[header_idx:])
    delimiter = detect_csv_delimiter(content[:2000])

    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    rows = []

    for r in reader:
        norm = {str(k).strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in r.items() if k}

        data_str = (
            norm.get("data")
            or norm.get("date")
            or norm.get("dia")
            or ""
        )
        hora_str = (
            norm.get("hora")
            or norm.get("hour")
            or ""
        )
        consumo_str = (
            norm.get("consumo registado (kw)")
            or norm.get("consumo registado (kwh)")
            or norm.get("consumo")
            or norm.get("energia")
            or ""
        )
        estado = norm.get("estado", "")

        if not data_str or not hora_str:
            continue

        if estado and estado.lower() not in ("medido", "real", "válido", "valido", ""):
            pass

        try:
            dt = parse_eredes_datetime(data_str, hora_str)
        except Exception:
            continue

        consumo = parse_float_pt(consumo_str)
        rows.append({
            "datetime": dt,
            "consumo_kwh": consumo,
            "estado": estado,
        })

    rows.sort(key=lambda x: x["datetime"])

    dedup = {}
    for row in rows:
        dedup[row["datetime"]] = row

    out = list(sorted(dedup.values(), key=lambda x: x["datetime"]))
    return out


# =========================
# CÁLCULOS
# =========================
def count_intervals_by_day(rows: list[dict]) -> dict[date, int]:
    counts = defaultdict(int)
    for row in rows:
        counts[row["datetime"].date()] += 1
    return dict(counts)


def find_latest_valid_consumption_day(rows: list[dict]) -> date | None:
    if not rows:
        return None

    counts = count_intervals_by_day(rows)
    valid_days = [d for d, c in counts.items() if c >= MIN_INTERVALS_VALID_DAY]

    if not valid_days:
        return None

    return max(valid_days)


def calc_day_consumption_and_cost(rows: list[dict], target_day: date) -> dict:
    day_rows = [r for r in rows if r["datetime"].date() == target_day]
    if not day_rows:
        return {
            "day": target_day,
            "count": 0,
            "vazio_kwh": 0.0,
            "fv_kwh": 0.0,
            "total_kwh": 0.0,
            "vazio_cost": 0.0,
            "fv_cost": 0.0,
            "total_cost": 0.0,
            "avg_price": 0.0,
        }

    try:
        omie_prices = load_omie_day(target_day)
    except Exception:
        omie_prices = {}

    vazio_kwh = fv_kwh = 0.0
    vazio_cost = fv_cost = 0.0

    for row in day_rows:
        dt = row["datetime"]
        kwh = row["consumo_kwh"]
        vazio = is_vazio(dt)

        if vazio:
            vazio_kwh += kwh
        else:
            fv_kwh += kwh

        if omie_prices:
            omie_hour = omie_prices.get(dt.hour)
            if omie_hour is not None:
                unit_price = omie_mwh_to_g9_kwh(omie_hour, vazio=vazio)
                cost = kwh * unit_price
                if vazio:
                    vazio_cost += cost
                else:
                    fv_cost += cost

    total_kwh = vazio_kwh + fv_kwh
    total_cost = vazio_cost + fv_cost
    avg_price = (total_cost / total_kwh) if total_kwh > 0 else 0.0

    return {
        "day": target_day,
        "count": len(day_rows),
        "vazio_kwh": vazio_kwh,
        "fv_kwh": fv_kwh,
        "total_kwh": total_kwh,
        "vazio_cost": vazio_cost,
        "fv_cost": fv_cost,
        "total_cost": total_cost,
        "avg_price": avg_price,
    }


def calc_month_accumulated(rows: list[dict], today_ref: date) -> dict:
    month_rows = [
        r for r in rows
        if r["datetime"].year == today_ref.year and r["datetime"].month == today_ref.month
    ]

    vazio_kwh = fv_kwh = 0.0
    vazio_cost = fv_cost = 0.0

    omie_cache = {}

    for row in month_rows:
        dt = row["datetime"]
        kwh = row["consumo_kwh"]
        vazio = is_vazio(dt)

        if vazio:
            vazio_kwh += kwh
        else:
            fv_kwh += kwh

        day = dt.date()
        if day not in omie_cache:
            try:
                omie_cache[day] = load_omie_day(day)
            except Exception:
                omie_cache[day] = None

        day_prices = omie_cache[day]
        if day_prices:
            omie_hour = day_prices.get(dt.hour)
            if omie_hour is not None:
                unit_price = omie_mwh_to_g9_kwh(omie_hour, vazio=vazio)
                cost = kwh * unit_price
                if vazio:
                    vazio_cost += cost
                else:
                    fv_cost += cost

    total_kwh = vazio_kwh + fv_kwh
    total_cost = vazio_cost + fv_cost
    avg_price = (total_cost / total_kwh) if total_kwh > 0 else 0.0

    last_dt = max((r["datetime"] for r in month_rows), default=None)

    return {
        "vazio_kwh": vazio_kwh,
        "fv_kwh": fv_kwh,
        "total_kwh": total_kwh,
        "vazio_cost": vazio_cost,
        "fv_cost": fv_cost,
        "total_cost": total_cost,
        "avg_price": avg_price,
        "last_dt": last_dt,
    }


# =========================
# TELEGRAM
# =========================
def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram não configurado. Mensagem gerada:")
        print(text)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        print("Telegram OK:", body)


def build_message(today_ref: date, omie_avg: float, g9_vazio: float, g9_fv: float,
                  day_stats: dict | None, month_stats: dict) -> str:
    lines = []

    lines.append(f"📅 G9 - {today_ref.strftime('%d/%m/%Y')}")
    lines.append("")
    lines.append(f"OMIE: {format_eur(omie_avg, 1)} €/MWh")
    lines.append("")
    lines.append("⚡ G9 Indexado")
    lines.append(f"• Vazio: {format_eur(g9_vazio, 3)} €/kWh")
    lines.append(f"• Fora vazio: {format_eur(g9_fv, 3)} €/kWh")

    if day_stats:
        label_day = day_stats["day"].strftime("%d/%m/%Y")
        lines.append("")
        lines.append(f"📊 Consumos ({label_day})")
        lines.append(f"• Vazio: {format_eur(day_stats['vazio_kwh'], 1)} kWh")
        lines.append(f"• Fora vazio: {format_eur(day_stats['fv_kwh'], 1)} kWh")
        lines.append(f"• Total: {format_eur(day_stats['total_kwh'], 1)} kWh")

        lines.append("")
        lines.append("💰 Custos reais")
        lines.append(f"• Vazio: {format_eur(day_stats['vazio_cost'], 2)} €")
        lines.append(f"• Fora vazio: {format_eur(day_stats['fv_cost'], 2)} €")
        lines.append(f"• Total: {format_eur(day_stats['total_cost'], 2)} €")
        lines.append(f"• Preço médio real: {format_eur(day_stats['avg_price'], 3)} €/kWh")

    if month_stats["last_dt"]:
        lu = month_stats["last_dt"].strftime("%d/%m/%Y às %H:%M")
    else:
        lu = "sem dados"

    lines.append("")
    lines.append(f"📆 Acumulado do mês (última atualização: {lu})")
    lines.append(f"• Vazio: {format_eur(month_stats['vazio_kwh'], 1)} kWh")
    lines.append(f"• Fora vazio: {format_eur(month_stats['fv_kwh'], 1)} kWh")
    lines.append(f"• Total: {format_eur(month_stats['total_kwh'], 1)} kWh")

    lines.append("")
    lines.append(f"💸 Acumulado real (última atualização: {lu})")
    lines.append(f"• Vazio: {format_eur(month_stats['vazio_cost'], 2)} €")
    lines.append(f"• Fora vazio: {format_eur(month_stats['fv_cost'], 2)} €")
    lines.append(f"• Total: {format_eur(month_stats['total_cost'], 2)} €")
    lines.append(f"• Preço médio real: {format_eur(month_stats['avg_price'], 3)} €/kWh")

    return "\n".join(lines)


# =========================
# MAIN
# =========================
def main():
    today_ref = datetime.now().date()

    # OMIE do dia da mensagem
    omie_today = load_omie_day(today_ref)
    omie_avg, g9_vazio, g9_fv = calc_today_g9_prices(omie_today)

    # Leituras E-REDES
    rows = load_eredes_15m_data()

    # Escolher o último dia "bom" (>=80% preenchido)
    valid_day = find_latest_valid_consumption_day(rows)
    day_stats = calc_day_consumption_and_cost(rows, valid_day) if valid_day else None

    # Acumulado do mês até à última leitura disponível
    month_stats = calc_month_accumulated(rows, today_ref)

    msg = build_message(today_ref, omie_avg, g9_vazio, g9_fv, day_stats, month_stats)
    print(msg)
    send_telegram_message(msg)


if __name__ == "__main__":
    main()
