import os
import re
import json
import urllib.request
import urllib.parse
from html import unescape
from datetime import datetime
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def extract_omie_mwh(html: str) -> float:
    text = unescape(html)

    # remover tags HTML
    text = re.sub(r"<[^>]+>", " ", text)

    # normalizar espaços
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


def calculate(omie_mwh: float) -> dict:
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


def build_message(data: dict) -> str:
    hoje_pt = datetime.now(ZoneInfo("Europe/Lisbon")).strftime("%d/%m/%Y")

    return (
        f"📅 G9 - DATA {hoje_pt}\n\n"
        f"OMIE: {data['OMIE_MWh']} €/MWh\n\n"
        "⚡ G9 estimado\n"
        f"• Vazio: {data['PRECO_VAZIO']} €/kWh\n"
        f"• Fora vazio: {data['PRECO_FV']} €/kWh"
    )


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

    # só envia às 08:00 em Portugal
    if now_pt.hour != 8:
        print(f"Skip - não são 08h em Portugal. Hora atual: {now_pt.strftime('%H:%M:%S')}")
        return

    html = fetch_text(OMIE_URL)
    omie_mwh = extract_omie_mwh(html)
    data = calculate(omie_mwh)
    message = build_message(data)

    print(message)
    send_telegram(message)


if __name__ == "__main__":
    main()
