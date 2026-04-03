import os
import re
import csv
import json
import urllib.request
import urllib.parse
from io import StringIO
from html import unescape
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

OMIE_URL = "https://www.omie.es/pt/spot-hoy"
TELEGRAM_API = "https://api.telegram.org"


def get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,text/csv,*/*;q=0.7",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


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


def parse_date_pt(value: str):
    value = (value or "").strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def to_int(value: str) -> int:
    value = (value or "").strip().replace(".", "").replace(",", ".")
    if value == "":
        return 0
    return int(float(value))


def load_eredes_data() -> list[dict]:
    url = os.getenv("EREDES_CSV_URL")
    if not url:
        return []

    csv_text = fetch_text(url)
    reader = csv.DictReader(StringIO(csv_text))
    rows = []

    for row in reader:
        data_leitura = parse_date_pt(row.get("Data da Leitura", ""))
        if not data_leitura:
            continue

        vazio = to_int(row.get("Vazio", "0"))
        ponta = to_int(row.get("Ponta", "0"))
        cheias = to_int(row.get("Cheias", "0"))

        rows.append({
            "date": data_leitura,
            "vazio": vazio,
            "ponta": ponta,
            "cheias": cheias,
            "fv": ponta + cheias,
        })

    rows.sort(key=lambda x: x["date"])
    return rows


def calculate_consumption_costs(rows: list[dict], preco_vazio: float, preco_fv: float) -> dict | None:
    if len(rows) < 2:
        return None

    tz = ZoneInfo("Europe/Lisbon")
    hoje = datetime.now(tz).date()
    ontem = hoje - timedelta(days=1)
    primeiro_dia_mes = ontem.replace(day=1)

    by_date = {r["date"]: r for r in rows}

    if ontem not in by_date or (ontem - timedelta(days=1)) not in by_date:
        return None

    # Consumo de ontem = leitura de ontem - leitura do dia anterior
    r_ontem = by_date[ontem]
    r_ant = by_date[ontem - timedelta(days=1)]

    consumo_ontem_vazio = r_ontem["vazio"] - r_ant["vazio"]
    consumo_ontem_fv = r_ontem["fv"] - r_ant["fv"]
    consumo_ontem_total = consumo_ontem_vazio + consumo_ontem_fv

    custo_ontem_vazio = consumo_ontem_vazio * preco_vazio
    custo_ontem_fv = consumo_ontem_fv * preco_fv
    custo_ontem_total = custo_ontem_vazio + custo_ontem_fv

    # Acumulado do mês = diferença entre ontem e o dia anterior ao 1º dia do mês
    acumulado_vazio = 0
    acumulado_fv = 0

    if primeiro_dia_mes in by_date and (primeiro_dia_mes - timedelta(days=1)) in by_date:
        r_inicio = by_date[primeiro_dia_mes - timedelta(days=1)]
        acumulado_vazio = r_ontem["vazio"] - r_inicio["vazio"]
        acumulado_fv = r_ontem["fv"] - r_inicio["fv"]
    else:
        # fallback: soma diferenças diárias disponíveis desde o 1º dia do mês
        datas_mes = [d for d in sorted(by_date.keys()) if primeiro_dia_mes <= d <= ontem]
        for d in datas_mes:
            d_ant = d - timedelta(days=1)
            if d_ant in by_date:
                acumulado_vazio += by_date[d]["vazio"] - by_date[d_ant]["vazio"]
                acumulado_fv += by_date[d]["fv"] - by_date[d_ant]["fv"]

    acumulado_total = acumulado_vazio + acumulado_fv

    custo_mes_vazio = acumulado_vazio * preco_vazio
    custo_mes_fv = acumulado_fv * preco_fv
    custo_mes_total = custo_mes_vazio + custo_mes_fv

    return {
        "data_ontem": ontem.strftime("%d/%m/%Y"),
        "consumo_ontem_vazio": round(consumo_ontem_vazio, 2),
        "consumo_ontem_fv": round(consumo_ontem_fv, 2),
        "consumo_ontem_total": round(consumo_ontem_total, 2),
        "custo_ontem_vazio": round(custo_ontem_vazio, 2),
        "custo_ontem_fv": round(custo_ontem_fv, 2),
        "custo_ontem_total": round(custo_ontem_total, 2),
        "acumulado_vazio": round(acumulado_vazio, 2),
        "acumulado_fv": round(acumulado_fv, 2),
        "acumulado_total": round(acumulado_total, 2),
        "custo_mes_vazio": round(custo_mes_vazio, 2),
        "custo_mes_fv": round(custo_mes_fv, 2),
        "custo_mes_total": round(custo_mes_total, 2),
    }


def build_message(prices: dict, consumos: dict | None) -> str:
    hoje_pt = datetime.now(ZoneInfo("Europe/Lisbon")).strftime("%d/%m/%Y")

    parts = [
        f"📅 G9 - {hoje_pt}",
        "",
        f"OMIE: {prices['OMIE_MWh']} €/MWh",
        "",
        "⚡ G9 estimado",
        f"• Vazio: {prices['PRECO_VAZIO']} €/kWh",
        f"• Fora vazio: {prices['PRECO_FV']} €/kWh",
    ]

    if consumos:
        parts.extend([
            "",
            f"📊 Consumos de ontem ({consumos['data_ontem']})",
            f"• Vazio: {consumos['consumo_ontem_vazio']} kWh",
            f"• Fora vazio: {consumos['consumo_ontem_fv']} kWh",
            f"• Total: {consumos['consumo_ontem_total']} kWh",
            "",
            "💰 Custos de ontem",
            f"• Vazio: {consumos['custo_ontem_vazio']} €",
            f"• Fora vazio: {consumos['custo_ontem_fv']} €",
            f"• Total: {consumos['custo_ontem_total']} €",
            "",
            "📆 Acumulado do mês",
            f"• Vazio: {consumos['acumulado_vazio']} kWh",
            f"• Fora vazio: {consumos['acumulado_fv']} kWh",
            f"• Total: {consumos['acumulado_total']} kWh",
            "",
            "💶 Acumulado estimado",
            f"• Vazio: {consumos['custo_mes_vazio']} €",
            f"• Fora vazio: {consumos['custo_mes_fv']} €",
            f"• Total: {consumos['custo_mes_total']} €",
        ])

    return "\n".join(parts)


def send_telegram(message: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        raise RuntimeError("Faltam TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID.")

    payload = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": message,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{TELEGRAM_API}/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            response_text = resp.read().decode("utf-8", errors="replace")
            response_json = json.loads(response_text)
            if not response_json.get("ok"):
                raise RuntimeError(f"Erro Telegram: {response_text}")
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Erro Telegram HTTP {e.code}: {error_body}")


def main() -> None:
    now_pt = datetime.now(ZoneInfo("Europe/Lisbon"))

    if now_pt.hour != 8:
        print(f"Skip - não são 08h em Portugal. Hora atual: {now_pt.strftime('%H:%M:%S')}")
        return

    html = fetch_text(OMIE_URL)
    omie_mwh = extract_omie_mwh(html)
    prices = calculate_prices(omie_mwh)

    rows = load_eredes_data()
    consumos = calculate_consumption_costs(rows, prices["PRECO_VAZIO"], prices["PRECO_FV"])

    message = build_message(prices, consumos)
    print(message)
    send_telegram(message)


if __name__ == "__main__":
    main()
