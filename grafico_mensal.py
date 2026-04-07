import os
import csv
import io
import re
import time
import calendar
import urllib.request
from datetime import datetime
from collections import defaultdict

import matplotlib.pyplot as plt

EREDES_CSV_URL = os.getenv("EREDES_CSV_URL", "").strip()


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


def is_vazio(dt: datetime) -> bool:
    return dt.hour >= 22 or dt.hour < 8


def load_eredes_15m_data() -> list[dict]:
    url = normalize_google_sheets_csv_url(EREDES_CSV_URL)
    if not url:
        raise RuntimeError("EREDES_CSV_URL está vazia.")

    url = add_cache_buster(url)
    csv_text = fetch_text(url)

    lines = csv_text.splitlines()
    if not lines:
        raise RuntimeError("CSV E-REDES sem linhas.")

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

        if not data_str or not hora_str:
            continue

        try:
            dt = parse_eredes_datetime(data_str, hora_str)
        except Exception:
            continue

        consumo = parse_float_pt(consumo_str)

        rows.append({
            "datetime": dt,
            "consumo_kwh": consumo,
        })

    rows.sort(key=lambda x: x["datetime"])

    dedup = {}
    for row in rows:
        dedup[row["datetime"]] = row

    final_rows = list(sorted(dedup.values(), key=lambda x: x["datetime"]))
    if not final_rows:
        raise RuntimeError("Nenhuma leitura válida encontrada no CSV E-REDES.")

    return final_rows


def aggregate_daily(rows: list[dict]):
    latest_dt = max(r["datetime"] for r in rows)
    year = latest_dt.year
    month = latest_dt.month

    days_in_month = calendar.monthrange(year, month)[1]

    vazio_by_day = defaultdict(float)
    fv_by_day = defaultdict(float)

    for row in rows:
        dt = row["datetime"]
        if dt.year != year or dt.month != month:
            continue

        d = dt.day
        kwh = row["consumo_kwh"]

        if is_vazio(dt):
            vazio_by_day[d] += kwh
        else:
            fv_by_day[d] += kwh

    days = list(range(1, days_in_month + 1))
    vazio = [vazio_by_day[d] for d in days]
    fora_vazio = [fv_by_day[d] for d in days]
    total = [v + f for v, f in zip(vazio, fora_vazio)]

    acumulado = []
    s = 0.0
    for val in total:
        s += val
        acumulado.append(s)

    return {
        "year": year,
        "month": month,
        "days": days,
        "vazio": vazio,
        "fora_vazio": fora_vazio,
        "total": total,
        "acumulado": acumulado,
        "latest_dt": latest_dt,
    }


def build_chart(data: dict, output_path: str = "grafico_mensal.png"):
    year = data["year"]
    month = data["month"]
    month_name = calendar.month_name[month]

    days = data["days"]
    vazio = data["vazio"]
    fora_vazio = data["fora_vazio"]
    acumulado = data["acumulado"]
    latest_dt = data["latest_dt"]

    fig, ax1 = plt.subplots(figsize=(14, 7))

    ax1.bar(days, vazio, label="Vazio")
    ax1.bar(days, fora_vazio, bottom=vazio, label="Fora vazio")

    ax1.set_xlabel("Dia do mês")
    ax1.set_ylabel("Consumo diário (kWh)")
    ax1.set_xticks(days)

    ax2 = ax1.twinx()
    ax2.plot(days, acumulado, marker="o", label="Acumulado total")
    ax2.set_ylabel("Acumulado (kWh)")

    fig.suptitle(
        f"Consumo mensal - {month_name} {year}\n"
        f"Última atualização: {latest_dt.strftime('%d/%m/%Y %H:%M')}"
    )

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, loc="upper left")

    fig.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main():
    rows = load_eredes_15m_data()
    data = aggregate_daily(rows)
    build_chart(data, "grafico_mensal.png")
    print("Gráfico criado: grafico_mensal.png")


if __name__ == "__main__":
    main()
