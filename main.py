import os
import re
import json
import urllib.request
import urllib.parse
from html import unescape

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
    patterns = [
        r"Portugal\s*-\s*Simples[\s\S]*?([0-9]+,[0-9]+)\s*€",
        r"Portugal\s*-\s*Simple[s]?[\s\S]*?([0-9]+,[0-9]+)\s*€",
        r"Portugal[\s\S]{0,500}?([0-9]+,[0-9]+)\s*€",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1).replace(",", "."))
    raise RuntimeError("Não foi possível extrair o OMIE do HTML.")


def calculate(omie_mwh: float) -> dict:
    perdas = get_env_float("PERDAS", 0.15)
    fadeq = get_env_float("FADEQ", 1.02)
    ac = get_env_float("AC", 0.0055)
    ggs = get_env_float("GGS", 0.0100)
    tar_vazio = get_env_float("TAR_VAZIO", 0.0158)
    tar_fv = get_env_float("TAR_FV", 0.0835)
    perc_vazio = get_env_float("PERC_VAZIO", 0.65)
    perc_fv = get_env_float("PERC_FV", 0.35)
    preco_fixo_ref = get_env_float("PRECO_FIXO_REF", 0.134)

    omie_kwh = omie_mwh / 1000.0
    base = (omie_kwh * fadeq * (1.0 + perdas)) + ac + ggs
    preco_vazio = base + tar_vazio
    preco_fv = base + tar_fv
    preco_final = (preco_vazio * perc_vazio) + (preco_fv * perc_fv)
    tar_media = (tar_vazio * perc_vazio) + (tar_fv * perc_fv)
    breakeven_num = ((preco_fixo_ref - ac - ggs - tar_media) / (fadeq * (1.0 + perdas))) * 1000.0

    if breakeven_num <= 0:
        breakeven_txt = "≤ 0 €/MWh"
    else:
        breakeven_txt = f"{breakeven_num:.1f} €/MWh"

    if preco_final < preco_fixo_ref:
        decisao = "✅ Indexado abaixo do preço de referência"
    elif abs(preco_final - preco_fixo_ref) < 1e-9:
        decisao = "➖ Empate com o preço de referência"
    else:
        decisao = "⚠️ Indexado acima do preço de referência"

    return {
        "OMIE_MWh": round(omie_mwh, 1),
        "PRECO_VAZIO": round(preco_vazio, 3),
        "PRECO_FV": round(preco_fv, 3),
        "PRECO_FINAL": round(preco_final, 3),
        "PRECO_FIXO_REF": round(preco_fixo_ref, 3),
        "BREAKEVEN_TXT": breakeven_txt,
        "DECISAO": decisao,
    }


def build_message(data: dict) -> str:
    return (
        "📅 Atualização diária G9\n\n"
        f"OMIE: {data['OMIE_MWh']} €/MWh\n\n"
        "⚡ G9 estimado\n"
        f"• Vazio: {data['PRECO_VAZIO']} €/kWh\n"
        f"• Fora vazio: {data['PRECO_FV']} €/kWh\n"
        f"• Médio: {data['PRECO_FINAL']} €/kWh\n\n"
        "🎯 Break-even\n"
        f"• {data['BREAKEVEN_TXT']}\n\n"
        "📌 Referência fixa\n"
        f"• {data['PRECO_FIXO_REF']} €/kWh\n\n"
        f"{data['DECISAO']}"
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
    with urllib.request.urlopen(req, timeout=30) as resp:
        response_text = resp.read().decode("utf-8", errors="replace")
        response_json = json.loads(response_text)
        if not response_json.get("ok"):
            raise RuntimeError(f"Erro Telegram: {response_text}")


def main() -> None:
    html = fetch_text(OMIE_URL)
    omie_mwh = extract_omie_mwh(html)
    data = calculate(omie_mwh)
    message = build_message(data)
    print(message)
    send_telegram(message)


if __name__ == "__main__":
    main()
