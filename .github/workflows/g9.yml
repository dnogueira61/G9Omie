import os
import csv
import io
import re
import time
import math
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, date
from collections import defaultdict

# =========================================================
# HELPERS CONFIG
# =========================================================
def getenv_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value != "" else default


def getenv_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    if value == "":
        return default
    return float(value)


# =========================================================
# CONFIG
# =========================================================
TELEGRAM_BOT_TOKEN = getenv_str("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = getenv_str("TELEGRAM_CHAT_ID", "")
EREDES_CSV_URL = getenv_str("EREDES_CSV_URL", "")

PERDAS = getenv_float("PERDAS", 0.15)
FADEQ = getenv_float("FADEQ", 1.02)
AC = getenv_float("AC", 0.0055)
GGS = getenv_float("GGS", 0.0100)
TAR_VAZIO = getenv_float("TAR_VAZIO", 0.0158)
TAR_FV = getenv_float("TAR_FV", 0.0835)

EXPECTED_INTERVALS_PER_DAY = 96
MIN_PREVIOUS_DAY_RATIO = 0.80
MIN_FALLBACK_DAY_RATIO = 0.70

MIN_PREVIOUS_DAY_INTERVALS = math.ceil(EXPECTED_INTERVALS_PER_DAY * MIN_PREVIOUS_DAY_RATIO)   # 77
MIN_FALLBACK_DAY_INTERVALS = math.floor(EXPECTED_INTERVALS_PER_DAY * MIN_FALLBACK_DAY_RATIO) + 1  # 68

OMIE_DIRECT_URL_TEMPLATE = "https://www.omie.es/sites/default/files/dados/AGNO_{year}/MES_{month}/TXT/{filename}"
OMIE_DOWNLOAD_URL_TEMPLATE_PT = "https://www.omie.es/pt/file-download?parents=marginalpdbcpt&filename={filename}"
OMIE_DOWNLOAD_URL_TEMPLATE_EN = "https://www.omie.es/en/file-download?parents=marginalpdbcpt&filename={filename}"


# =========================================================
# HELPERS
# =========================================================
def fetch_bytes(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def decode_bytes(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def fetch_text(url: str) -> str:
    return decode_bytes(fetch_bytes(url))


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


def add_cache_buster(url: str) -> str:
    if not url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}t={int(time.time())}"


def parse_float_pt(value) -> float:
    if value is None:
        return 0.0

    s = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not s:
        return 0.0

    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def format_num(value: float, digits: int) -> str:
    return f"{value:.{digits}f}"


def format_kwh(value: float) -> str:
    return f"{value:.1f}"


def format_eur(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def is_vazio(dt: datetime) -> bool:
    return dt.hour >= 22 or dt.hour < 8


def safe_max_dt(rows: list[dict]):
    return max((r["datetime"] for r in rows), default=None)


# =========================================================
# OMIE
# =========================================================
def omie_filename(target_date: date) -> str:
    return f"marginalpdbcpt_{target_date.strftime('%Y%m%d')}.1"


def build_omie_candidate_urls(target_date: date) -> list[str]:
    filename = omie_filename(target_date)
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")

    return [
        OMIE_DIRECT_URL_TEMPLATE.format(year=year, month=month, filename=filename),
        OMIE_DOWNLOAD_URL_TEMPLATE_PT.format(filename=urllib.parse.quote(filename)),
        OMIE_DOWNLOAD_URL_TEMPLATE_EN.format(filename=urllib.parse.quote(filename)),
    ]


def looks_like_html(text: str) -> bool:
    head = text[:500].lower()
    return ("<html" in head) or ("<!doctype html" in head) or ("<body" in head)


def parse_omie_prices(text: str) -> dict[int, float]:
    hour_buckets = defaultdict(list)

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.upper().startswith("MARGINALPDBCPT"):
            continue
        if line.startswith("*"):
            continue

        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 5:
            continue

        period_raw = parts[3]
        price_raw = parts[4].replace(",", ".")

        if not re.fullmatch(r"\d{1,3}", period_raw):
            continue
        if not re.fullmatch(r"-?\d+(?:\.\d+)?", price_raw):
            continue

        period = int(period_raw)
        price = float(price_raw)

        if 1 <= period <= 96:
            hour = (period - 1) // 4
            hour_buckets[hour].append(price)
        elif 1 <= period <= 24:
            hour = period - 1
            hour_buckets[hour].append(price)

    prices = {}
    for hour in range(24):
        vals = hour_buckets.get(hour, [])
        if not vals:
            continue
        prices[hour] = sum(vals) / len(vals)

    if len(prices) != 24:
        snippet = text[:1500].replace("\n", "\\n")
        raise ValueError(
            f"Não foi possível construir as 24 horas OMIE. "
            f"Horas construídas: {len(prices)}. Início do conteúdo: {snippet}"
        )

    return prices


def load_omie_day(target_date: date) -> dict[int, float]:
    urls = build_omie_candidate_urls(target_date)
    last_error = None

    for url in urls:
        try:
            print(f"OMIE: a tentar {url}")
            raw = fetch_bytes(url)
            text = decode_bytes(raw)

            if looks_like_html(text):
                raise ValueError("Resposta OMIE parece HTML em vez de TXT.")

            return parse_omie_prices(text)

        except Exception as e:
            last_error = e
            print(f"AVISO: tentativa OMIE falhou para {target_date} | url={url} | erro={e}")

    raise RuntimeError(f"Falha ao carregar OMIE de {target_date}: {last_error}")


def load_omie_day_with_fallback(reference_day: date, max_back_days: int = 5) -> tuple[dict[int, float], date]:
    errors = []

    for back in range(0, max_back_days + 1):
        d = reference_day - timedelta(days=back)
        try:
            prices = load_omie_day(d)
            if back > 0:
                print(f"OMIE fallback usado: {d} (em vez de {reference_day})")
            return prices, d
        except Exception as e:
            errors.append(f"{d}: {e}")

    raise RuntimeError("Falha total OMIE. Tentativas: " + " | ".join(errors))


def omie_mwh_to_g9_kwh(omie_eur_mwh: float, vazio: bool) -> float:
    base_kwh = omie_eur_mwh / 1000.0
    tar = TAR_VAZIO if vazio else TAR_FV
    return (base_kwh * FADEQ * (1 + PERDAS)) + AC + GGS + tar


def calc_g9_prices_from_omie(omie_prices: dict[int, float]) -> tuple[float, float, float]:
    vazio_vals = []
    fv_vals = []

    for hour, omie_price in omie_prices.items():
        price_kwh = omie_mwh_to_g9_kwh(omie_price, vazio=(hour >= 22 or hour < 8))
        if hour >= 22 or hour < 8:
            vazio_vals.append(price_kwh)
        else:
            fv_vals.append(price_kwh)

    omie_avg = sum(omie_prices.values()) / len(omie_prices) if omie_prices else 0.0
    avg_vazio = sum(vazio_vals) / len(vazio_vals) if vazio_vals else 0.0
    avg_fv = sum(fv_vals) / len(fv_vals) if fv_vals else 0.0

    return omie_avg, avg_vazio, avg_fv


# =========================================================
# E-REDES
# =========================================================
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

    formats = (
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M",
    )

    for fmt in formats:
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

    print(f"DEBUG E-REDES: URL presente? {'sim' if bool(url) else 'não'}")
    url = add_cache_buster(url)

    try:
        csv_text = fetch_text(url)
    except Exception as e:
        print(f"DEBUG E-REDES: erro ao descarregar CSV: {e}")
        return []

    print(f"DEBUG E-REDES: tamanho do CSV descarregado = {len(csv_text)} caracteres")

    lines = csv_text.splitlines()
    print(f"DEBUG E-REDES: número de linhas brutas = {len(lines)}")

    if not lines:
        print("DEBUG E-REDES: CSV sem linhas.")
        return []

    print("DEBUG E-REDES: primeiras 5 linhas do ficheiro:")
    for i, line in enumerate(lines[:5], start=1):
        print(f"  [{i}] {line}")

    header_idx = find_header_row(lines)
    print(f"DEBUG E-REDES: header_idx = {header_idx}")

    content = "\n".join(lines[header_idx:])
    delimiter = detect_csv_delimiter(content[:2000])
    print(f"DEBUG E-REDES: delimitador detetado = '{delimiter}'")

    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)

    rows = []
    skipped_no_datetime = 0
    skipped_bad_datetime = 0

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
            skipped_no_datetime += 1
            continue

        try:
            dt = parse_eredes_datetime(data_str, hora_str)
        except Exception:
            skipped_bad_datetime += 1
            continue

        consumo = parse_float_pt(consumo_str)

        rows.append({
            "datetime": dt,
            "consumo_kwh": consumo,
            "estado": estado,
        })

    print(f"DEBUG E-REDES: linhas válidas antes de dedup = {len(rows)}")
    print(f"DEBUG E-REDES: ignoradas sem data/hora = {skipped_no_datetime}")
    print(f"DEBUG E-REDES: ignoradas por data/hora inválida = {skipped_bad_datetime}")

    rows.sort(key=lambda x: x["datetime"])

    dedup = {}
    for row in rows:
        dedup[row["datetime"]] = row

    final_rows = list(sorted(dedup.values(), key=lambda x: x["datetime"]))
    print(f"DEBUG E-REDES: linhas válidas finais = {len(final_rows)}")

    if final_rows:
        print(f"DEBUG E-REDES: primeira leitura = {final_rows[0]['datetime']} | {final_rows[0]['consumo_kwh']}")
        print(f"DEBUG E-REDES: última leitura = {final_rows[-1]['datetime']} | {final_rows[-1]['consumo_kwh']}")
    else:
        print("DEBUG E-REDES: nenhuma leitura válida encontrada.")

    return final_rows


def count_intervals_by_day(rows: list[dict]) -> dict[date, int]:
    counts = defaultdict(int)
    for row in rows:
        counts[row["datetime"].date()] += 1
    return dict(counts)


def choose_consumption_day(rows: list[dict], today_ref: date) -> tuple[date | None, int, str | None]:
    if not rows:
        return None, 0, None

    counts = count_intervals_by_day(rows)
    previous_day = today_ref - timedelta(days=1)
    previous_count = counts.get(previous_day, 0)

    if previous_count >= MIN_PREVIOUS_DAY_INTERVALS:
        return previous_day, previous_count, "previous_day_80"

    eligible_days = [
        (d, c) for d, c in counts.items()
        if c >= MIN_FALLBACK_DAY_INTERVALS
    ]
    if not eligible_days:
        return None, 0, None

    eligible_days.sort(key=lambda x: x[0], reverse=True)
    chosen_day, chosen_count = eligible_days[0]
    return chosen_day, chosen_count, "fallback_70"


# =========================================================
# CÁLCULOS
# =========================================================
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
        omie_prices, omie_used_day = load_omie_day_with_fallback(target_day, max_back_days=3)
        print(f"OMIE usado para custos do dia {target_day}: {omie_used_day}")
    except Exception as e:
        print(f"AVISO: falha ao carregar OMIE de {target_day} para custos: {e}")
        omie_prices = None

    vazio_kwh = 0.0
    fv_kwh = 0.0
    vazio_cost = 0.0
    fv_cost = 0.0

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

    vazio_kwh = 0.0
    fv_kwh = 0.0
    vazio_cost = 0.0
    fv_cost = 0.0
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
                prices, used_day = load_omie_day_with_fallback(day, max_back_days=3)
                omie_cache[day] = prices
                if used_day != day:
                    print(f"OMIE fallback no acumulado mensal para {day}: usado {used_day}")
            except Exception as e:
                print(f"AVISO: falha ao carregar OMIE de {day} no acumulado mensal: {e}")
                omie_cache[day] = None

        prices = omie_cache[day]
        if prices:
            omie_hour = prices.get(dt.hour)
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
    last_dt = safe_max_dt(month_rows)

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


# =========================================================
# TELEGRAM
# =========================================================
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


def build_message(
    today_ref: date,
    omie_reference_day_requested: date,
    omie_reference_day_used: date,
    day_stats: dict | None,
    month_stats: dict,
    omie_avg: float,
    g9_vazio: float,
    g9_fv: float,
) -> str:
    lines = []

    lines.append(f"📅 G9 - {today_ref.strftime('%d/%m/%Y')}")
    lines.append("")

    if omie_reference_day_requested == omie_reference_day_used:
        lines.append(f"OMIE ({omie_reference_day_used.strftime('%d/%m/%Y')}): {format_num(omie_avg, 1)} €/MWh")
    else:
        lines.append(f"OMIE ({omie_reference_day_used.strftime('%d/%m/%Y')} - fallback): {format_num(omie_avg, 1)} €/MWh")

    lines.append("")
    lines.append("⚡ G9 Indexado")
    lines.append(f"• Vazio: {format_num(g9_vazio, 3)} €/kWh")
    lines.append(f"• Fora vazio: {format_num(g9_fv, 3)} €/kWh")

    if day_stats:
        label_day = day_stats["day"].strftime("%d/%m/%Y")
        total_kwh = day_stats["total_kwh"]

        pct_vazio = (day_stats["vazio_kwh"] / total_kwh * 100) if total_kwh > 0 else 0.0
        pct_fv = (day_stats["fv_kwh"] / total_kwh * 100) if total_kwh > 0 else 0.0

        lines.append("")
        lines.append(f"📊 Consumos ({label_day})")
        lines.append(f"• Vazio: {format_kwh(day_stats['vazio_kwh'])} kWh ({format_num(pct_vazio, 1)}%)")
        lines.append(f"• Fora vazio: {format_kwh(day_stats['fv_kwh'])} kWh ({format_num(pct_fv, 1)}%)")
        lines.append(f"• Total: {format_kwh(day_stats['total_kwh'])} kWh")

        lines.append("")
        lines.append("💰 Custos reais")
        lines.append(f"• Vazio: {format_eur(day_stats['vazio_cost'], 2)} €")
        lines.append(f"• Fora vazio: {format_eur(day_stats['fv_cost'], 2)} €")
        lines.append(f"• Total: {format_eur(day_stats['total_cost'], 2)} €")
        lines.append(f"• Preço médio real: {format_num(day_stats['avg_price'], 3)} €/kWh")

    if month_stats["last_dt"]:
        lu = month_stats["last_dt"].strftime("%d/%m/%Y às %H:%M")
    else:
        lu = "sem dados"

    lines.append("")
    lines.append(f"📆 Acumulado do mês (última atualização: {lu})")
    lines.append(f"• Vazio: {format_kwh(month_stats['vazio_kwh'])} kWh")
    lines.append(f"• Fora vazio: {format_kwh(month_stats['fv_kwh'])} kWh")
    lines.append(f"• Total: {format_kwh(month_stats['total_kwh'])} kWh")

    lines.append("")
    lines.append(f"💸 Acumulado real (última atualização: {lu})")
    lines.append(f"• Vazio: {format_eur(month_stats['vazio_cost'], 2)} €")
    lines.append(f"• Fora vazio: {format_eur(month_stats['fv_cost'], 2)} €")
    lines.append(f"• Total: {format_eur(month_stats['total_cost'], 2)} €")
    lines.append(f"• Preço médio real: {format_num(month_stats['avg_price'], 3)} €/kWh")

    return "\n".join(lines)


# =========================================================
# MAIN
# =========================================================
def main():
    today_ref = datetime.now().date()

    omie_reference_day_requested = today_ref - timedelta(days=1)
    omie_prices, omie_reference_day_used = load_omie_day_with_fallback(
        omie_reference_day_requested,
        max_back_days=5
    )
    omie_avg, g9_vazio, g9_fv = calc_g9_prices_from_omie(omie_prices)

    rows = load_eredes_15m_data()

    chosen_day, chosen_count, chosen_mode = choose_consumption_day(rows, today_ref)

    if chosen_day:
        print(
            f"Dia escolhido para consumos: {chosen_day} | "
            f"intervalos={chosen_count} | modo={chosen_mode}"
        )
        day_stats = calc_day_consumption_and_cost(rows, chosen_day)
    else:
        print("Nenhum dia elegível encontrado para a secção de consumos.")
        day_stats = None

    month_stats = calc_month_accumulated(rows, today_ref)

    msg = build_message(
        today_ref=today_ref,
        omie_reference_day_requested=omie_reference_day_requested,
        omie_reference_day_used=omie_reference_day_used,
        day_stats=day_stats,
        month_stats=month_stats,
        omie_avg=omie_avg,
        g9_vazio=g9_vazio,
        g9_fv=g9_fv,
    )

    print(msg)
    send_telegram_message(msg)


if __name__ == "__main__":
    main()
