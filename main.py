import os
import re
import csv
import json
import urllib.request
import urllib.parse
import urllib.error
from io import StringIO
from html import unescape
from datetime import datetime, timedelta, time, date
from zoneinfo import ZoneInfo


OMIE_URL = "https://www.omie.es/pt/spot-hoy"
TELEGRAM_API = "https://api.telegram.org"
TZ = ZoneInfo("Europe/Lisbon")


def get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def normalize_google_sheets_csv_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url

    if "gviz/tq" in url or "export?format=csv" in url:
        return url

    if "docs.google.com/spreadsheets" not in url:
        return url

    gid_match = re.search(r"[#?&]gid=(\d+)", url)
    gid = gid_match.group(1) if gid_match else "0"

    sheet_match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not sheet_match:
        return url

    sheet_id = sheet_match.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def fetch_text(url: str) -> str:
    url = normalize_google_sheets_csv_url(url)
    print(f"DEBUG fetch_text URL final: {url}")

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,text/plain,*/*;q=0.8",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8-sig", errors="replace")
            print(f"DEBUG fetch_text first 300 chars: {body[:300]!r}")
            return body
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        print(f"DEBUG fetch_text HTTPError URL: {url}")
        print(f"DEBUG fetch_text HTTPError body: {error_body}")
        raise


def extract_omie_mwh(html: str) -> float:
    text = unescape(html)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    patterns = [
        r"Preço medio Portugal\s*([0-9]+,[0-9]+)\s*€/MWh",
        r"Preco medio Portugal\s*([0-9]+,[0-9]+)\s*€/MWh",
        r"Preço médio Portugal\s*([0-9]+,[0-9]+)\s*€/MWh",
        r"Preco médio Portugal\s*([0-9]+,[0-9]+)\s*€/MWh",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1).replace(",", "."))

    raise RuntimeError(f"Não foi possível extrair o OMIE do HTML. Trecho: {text[:500]}")


def calculate_prices(omie_mwh: float) -> dict:
    perdas = get_env_float("PERDAS", 0.15)
    fadeq = get_env_float("FADEQ", 1.02)
    ac = get_env_float("AC", 0.0055)
    ggs = get_env_float("GGS", 0.0100)
    tar_vazio = get_env_float("TAR_VAZIO", 0.0158)
    tar_fv = get_env_float("TAR_FV", 0.0835)

    omie_kwh = omie_mwh / 1000.0
    base = (omie_kwh * fadeq * (1.0 + perdas)) + ac + ggs

    preco_vazio = base + tar_vazio
    preco_fv = base + tar_fv

    return {
        "OMIE_MWh": round(omie_mwh, 1),
        "PRECO_VAZIO": round(preco_vazio, 3),
        "PRECO_FV": round(preco_fv, 3),
    }


def parse_date_multi(value: str):
    value = (value or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def parse_time_multi(value: str):
    value = (value or "").strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            pass
    return None


def to_float(value: str) -> float:
    value = (value or "").strip().replace("\xa0", "").replace(" ", "")
    if value == "":
        return 0.0

    if "," in value and "." in value:
        value = value.replace(".", "").replace(",", ".")
    elif "," in value:
        value = value.replace(",", ".")

    try:
        return float(value)
    except ValueError:
        return 0.0


def normalize_header_name(name: str) -> str:
    name = (name or "").strip().lstrip("\ufeff")
    name = re.sub(r"\s+", " ", name)
    return name.lower()


def detect_csv_delimiter(sample_text: str) -> str:
    first_lines = "\n".join(sample_text.splitlines()[:10])
    semicolons = first_lines.count(";")
    commas = first_lines.count(",")
    return ";" if semicolons > commas else ","


def find_header_index(lines: list[str]) -> int | None:
    for i, line in enumerate(lines):
        line_norm = normalize_header_name(line)
        if "data" in line_norm and "hora" in line_norm and "consumo" in line_norm:
            return i
    return None


def pick_column(fieldnames: list[str], candidates: list[str]) -> str | None:
    normalized_map = {normalize_header_name(f): f for f in fieldnames if f}

    for candidate in candidates:
        candidate_norm = normalize_header_name(candidate)
        if candidate_norm in normalized_map:
            return normalized_map[candidate_norm]

    for norm_name, original_name in normalized_map.items():
        for candidate in candidates:
            if normalize_header_name(candidate) in norm_name:
                return original_name

    return None


def is_vazio(dt_local: datetime) -> bool:
    t = dt_local.time()
    return t >= time(22, 0) or t < time(8, 0)


def load_eredes_15m_data() -> list[dict]:
    url = os.getenv("EREDES_CSV_URL")

    if not url:
        print("DEBUG E-REDES: variável EREDES_CSV_URL vazia.")
        return []

    # 🔥 anti-cache
    import time
    url = f"{url}&t={int(time.time())}"

    print("DEBUG E-REDES URL:", url[:80])


    header_index = find_header_index(lines)
    if header_index is None:
        print("DEBUG E-REDES: cabeçalho Data/Hora/Consumo não encontrado.")
        print("DEBUG primeiras 20 linhas:")
        for line in lines[:20]:
            print(repr(line))
        return []

    cleaned_csv = "\n".join(lines[header_index:])
    delimiter = detect_csv_delimiter(cleaned_csv)

    print(f"DEBUG E-REDES: delimitador detetado = {delimiter!r}")
    print(f"DEBUG E-REDES: header index = {header_index}")
    print(f"DEBUG E-REDES: linha cabeçalho = {lines[header_index]!r}")

    reader = csv.DictReader(StringIO(cleaned_csv), delimiter=delimiter)

    if not reader.fieldnames:
        print("DEBUG E-REDES: sem fieldnames.")
        return []

    print("DEBUG E-REDES: fieldnames =", reader.fieldnames)

    col_data = pick_column(reader.fieldnames, ["Data"])
    col_hora = pick_column(reader.fieldnames, ["Hora"])
    col_consumo = pick_column(reader.fieldnames, ["Consumo"])
    col_estado = pick_column(reader.fieldnames, ["Estado"])

    print("DEBUG E-REDES: colunas escolhidas =", {
        "data": col_data,
        "hora": col_hora,
        "consumo": col_consumo,
        "estado": col_estado,
    })

    if not col_data or not col_hora or not col_consumo:
        print("DEBUG E-REDES: faltam colunas obrigatórias.")
        return []

    rows = []
    for idx, row in enumerate(reader, start=1):
        d = parse_date_multi(row.get(col_data, ""))
        h = parse_time_multi(row.get(col_hora, ""))
        consumo = to_float(row.get(col_consumo, "0"))
        estado = (row.get(col_estado, "") or "").strip() if col_estado else ""

        if not d or not h:
            continue

        dt_local = datetime.combine(d, h).replace(tzinfo=TZ)

        rows.append({
            "date": d,
            "time": h,
            "datetime": dt_local,
            "consumo": consumo,
            "estado": estado,
            "is_vazio": is_vazio(dt_local),
        })

        if idx <= 5:
            print("DEBUG E-REDES sample raw row:", row)
            print("DEBUG E-REDES parsed row:", rows[-1])

    rows.sort(key=lambda x: x["datetime"])
    return rows


def omie_file_url_for_date(target_date: date) -> str:
    ymd = target_date.strftime("%Y%m%d")
    return (
        f"https://www.omie.es/es/file-download?parents%5B0%5D=marginalpdbcpt"
        f"&filename=marginalpdbcpt_{ymd}.1"
    )


def parse_omie_decimal(value: str) -> float:
    value = (value or "").strip()
    if not value:
        return 0.0
    return float(value.replace(".", "").replace(",", "."))


def load_omie_day_prices(target_date: date) -> dict[int, float]:
    url = omie_file_url_for_date(target_date)
    print(f"DEBUG OMIE URL: {url}")
    raw = fetch_text(url)

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    print("DEBUG OMIE primeiras linhas:")
    for line in lines[:10]:
        print(repr(line))

    prices_mwh: dict[int, float] = {}

    for line in lines:
        # Exemplo real:
        # 2026;04;01;1;9.17;9.17;
        parts = [p.strip() for p in line.split(";") if p.strip()]

        if len(parts) >= 5:
            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                hour_num = int(parts[3])  # 1 a 24
                price_mwh = float(parts[4])  # PT

                # Garantir que é o dia certo
                if date(year, month, day) != target_date:
                    continue

                if 1 <= hour_num <= 24:
                    prices_mwh[hour_num - 1] = price_mwh

            except Exception:
                continue

    if len(prices_mwh) < 24:
        raise RuntimeError(
            f"Não foi possível ler 24 preços OMIE para {target_date}. Obtidos: {len(prices_mwh)}"
        )

    # Conversão para €/kWh + tarifas
    perdas = get_env_float("PERDAS", 0.15)
    fadeq = get_env_float("FADEQ", 1.02)
    ac = get_env_float("AC", 0.0055)
    ggs = get_env_float("GGS", 0.0100)
    tar_vazio = get_env_float("TAR_VAZIO", 0.0158)
    tar_fv = get_env_float("TAR_FV", 0.0835)

    prices_kwh: dict[int, float] = {}

    for hour_start, omie_mwh in sorted(prices_mwh.items()):
        omie_kwh = omie_mwh / 1000.0
        base = (omie_kwh * fadeq * (1.0 + perdas)) + ac + ggs

        if 22 <= hour_start or hour_start < 8:
            final_price = base + tar_vazio
        else:
            final_price = base + tar_fv

        prices_kwh[hour_start] = final_price

    print("DEBUG OMIE preços finais €/kWh:", {k: round(v, 5) for k, v in prices_kwh.items()})
    return prices_kwh


def calculate_real_consumption_costs(rows: list[dict]) -> dict:
    hoje = datetime.now(TZ).date()

    base_empty = {
        "tem_dados_ontem": False,
        "data_ontem": (hoje - timedelta(days=1)).strftime("%d/%m/%Y"),
        "ultima_atualizacao": None,
        "ultima_hora": None,
        "consumo_ontem_vazio": 0.0,
        "consumo_ontem_fv": 0.0,
        "consumo_ontem_total": 0.0,
        "custo_ontem_vazio": 0.0,
        "custo_ontem_fv": 0.0,
        "custo_ontem_total": 0.0,
        "acumulado_vazio": 0.0,
        "acumulado_fv": 0.0,
        "acumulado_total": 0.0,
        "custo_mes_vazio": 0.0,
        "custo_mes_fv": 0.0,
        "custo_mes_total": 0.0,
        "preco_medio_ontem": 0.0,
        "preco_medio_mes": 0.0,
    }

    if not rows:
        return base_empty

    # 🔥 NOVO: última data + hora
    ultimo_datetime = max(r["datetime"] for r in rows)
    ultima_data = ultimo_datetime.date()
    ultima_hora = ultimo_datetime.strftime("%H:%M")

    primeiro_dia_mes = ultima_data.replace(day=1)
    ontem = hoje - timedelta(days=1)

    unique_dates = sorted({r["date"] for r in rows if primeiro_dia_mes <= r["date"] <= ultima_data})
    omie_by_date: dict[date, dict[int, float]] = {}

    for d in unique_dates:
        omie_by_date[d] = load_omie_day_prices(d)

    rows_mes = [r for r in rows if primeiro_dia_mes <= r["date"] <= ultima_data]
    rows_ontem = [r for r in rows if r["date"] == ontem]

    def enrich_cost(row: dict) -> float:
        hour_start = row["datetime"].hour
        price = omie_by_date[row["date"]][hour_start]
        return row["consumo"] * price

    consumo_ontem_vazio = sum(r["consumo"] for r in rows_ontem if r["is_vazio"])
    consumo_ontem_fv = sum(r["consumo"] for r in rows_ontem if not r["is_vazio"])
    consumo_ontem_total = consumo_ontem_vazio + consumo_ontem_fv

    custo_ontem_vazio = sum(enrich_cost(r) for r in rows_ontem if r["is_vazio"])
    custo_ontem_fv = sum(enrich_cost(r) for r in rows_ontem if not r["is_vazio"])
    custo_ontem_total = custo_ontem_vazio + custo_ontem_fv

    acumulado_vazio = sum(r["consumo"] for r in rows_mes if r["is_vazio"])
    acumulado_fv = sum(r["consumo"] for r in rows_mes if not r["is_vazio"])
    acumulado_total = acumulado_vazio + acumulado_fv

    custo_mes_vazio = sum(enrich_cost(r) for r in rows_mes if r["is_vazio"])
    custo_mes_fv = sum(enrich_cost(r) for r in rows_mes if not r["is_vazio"])
    custo_mes_total = custo_mes_vazio + custo_mes_fv

    preco_medio_ontem = (custo_ontem_total / consumo_ontem_total) if consumo_ontem_total > 0 else 0.0
    preco_medio_mes = (custo_mes_total / acumulado_total) if acumulado_total > 0 else 0.0

    return {
        "tem_dados_ontem": len(rows_ontem) > 0,
        "data_ontem": ontem.strftime("%d/%m/%Y"),
        "ultima_atualizacao": ultima_data.strftime("%d/%m/%Y"),
        "ultima_hora": ultima_hora,
        "consumo_ontem_vazio": round(consumo_ontem_vazio, 1),
        "consumo_ontem_fv": round(consumo_ontem_fv, 1),
        "consumo_ontem_total": round(consumo_ontem_total, 1),
        "custo_ontem_vazio": round(custo_ontem_vazio, 2),
        "custo_ontem_fv": round(custo_ontem_fv, 2),
        "custo_ontem_total": round(custo_ontem_total, 2),
        "acumulado_vazio": round(acumulado_vazio, 1),
        "acumulado_fv": round(acumulado_fv, 1),
        "acumulado_total": round(acumulado_total, 1),
        "custo_mes_vazio": round(custo_mes_vazio, 2),
        "custo_mes_fv": round(custo_mes_fv, 2),
        "custo_mes_total": round(custo_mes_total, 2),
        "preco_medio_ontem": round(preco_medio_ontem, 3),
        "preco_medio_mes": round(preco_medio_mes, 3),
    }


def build_message(prices: dict, consumos: dict | None) -> str:
    hoje_pt = datetime.now(TZ).strftime("%d/%m/%Y")

    parts = [
        f"📅 G9 - {hoje_pt}",
        "",
        f"OMIE: {prices['OMIE_MWh']} €/MWh",
        "",
        "⚡ G9 Indexado",
        f"• Vazio: {prices['PRECO_VAZIO']} €/kWh",
        f"• Fora vazio: {prices['PRECO_FV']} €/kWh",
    ]

    if consumos and consumos.get("tem_dados_ontem"):
        parts.extend([
            "",
            f"📊 Consumos de ontem ({consumos['data_ontem']})",
            f"• Vazio: {consumos['consumo_ontem_vazio']} kWh",
            f"• Fora vazio: {consumos['consumo_ontem_fv']} kWh",
            f"• Total: {consumos['consumo_ontem_total']} kWh",
            "",
            "💰 Custos reais de ontem",
            f"• Vazio: {consumos['custo_ontem_vazio']} €",
            f"• Fora vazio: {consumos['custo_ontem_fv']} €",
            f"• Total: {consumos['custo_ontem_total']} €",
            f"• Preço médio real: {consumos['preco_medio_ontem']} €/kWh",
        ])

    # 🔥 AQUI está a magia
    ultima = "sem dados"
    if consumos and consumos.get("ultima_atualizacao"):
        ultima = consumos["ultima_atualizacao"]
        if consumos.get("ultima_hora"):
            ultima = f"{ultima} às {consumos['ultima_hora']}"

    parts.extend([
        "",
        f"📆 Acumulado do mês (última atualização: {ultima})",
        f"• Vazio: {consumos['acumulado_vazio'] if consumos else 0} kWh",
        f"• Fora vazio: {consumos['acumulado_fv'] if consumos else 0} kWh",
        f"• Total: {consumos['acumulado_total'] if consumos else 0} kWh",
        "",
        f"💶 Acumulado real (última atualização: {ultima})",
        f"• Vazio: {consumos['custo_mes_vazio'] if consumos else 0} €",
        f"• Fora vazio: {consumos['custo_mes_fv'] if consumos else 0} €",
        f"• Total: {consumos['custo_mes_total'] if consumos else 0} €",
        f"• Preço médio real: {consumos['preco_medio_mes'] if consumos else 0} €/kWh",
    ])

    return "\n".join(parts)


def send_telegram(message: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        raise RuntimeError("Faltam TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID.")

    url = f"{TELEGRAM_API}/bot{token}/sendMessage"

    data = {
        "chat_id": str(chat_id).strip(),
        "text": message,
    }

    encoded = urllib.parse.urlencode(data).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            response_text = resp.read().decode("utf-8", errors="replace")
            print("Telegram response:", response_text)

            response_json = json.loads(response_text)
            if not response_json.get("ok"):
                raise RuntimeError(f"Erro Telegram: {response_text}")

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        print("Telegram error body:", error_body)
        raise RuntimeError(f"Erro Telegram HTTP {e.code}: {error_body}")


def main() -> None:
    now_pt = datetime.now(TZ)

    if os.getenv("FORCE_RUN", "").lower() != "1" and now_pt.hour != 8:
        print(f"Skip - não são 08h em Portugal. Hora atual: {now_pt.strftime('%H:%M:%S')}")
        return

    html = fetch_text(OMIE_URL)
    omie_mwh = extract_omie_mwh(html)
    prices = calculate_prices(omie_mwh)

    rows = load_eredes_15m_data()

    print("=== DEBUG E-REDES ===")
    print("Nº de linhas lidas:", len(rows))
    if rows:
        print("Primeiro datetime:", rows[0]["datetime"])
        print("Último datetime:", rows[-1]["datetime"])
    print("=== FIM DEBUG E-REDES ===")

    consumos = calculate_real_consumption_costs(rows)

    message = build_message(prices, consumos)

    print("=== CHAT ID ===", os.getenv("TELEGRAM_CHAT_ID"))
    print("=== MESSAGE ===")
    print(message)
    print("=== END MESSAGE ===")

    send_telegram(message)


if __name__ == "__main__":
    main()
