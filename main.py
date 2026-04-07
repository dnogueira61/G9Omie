import os
import io
import csv
import math
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Lisbon")

MASTER_FILE = "master.csv"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
EREDES_CSV_URL = os.getenv("EREDES_CSV_URL", "").strip()
FORCE_RUN = os.getenv("FORCE_RUN", "0").strip() == "1"

PERDAS = float(os.getenv("PERDAS", "0.15") or "0.15")
FADEQ = float(os.getenv("FADEQ", "1.02") or "1.02")
AC = float(os.getenv("AC", "0.0055") or "0.0055")
GGS = float(os.getenv("GGS", "0.0100") or "0.0100")
TAR_VAZIO = float(os.getenv("TAR_VAZIO", "0.0158") or "0.0158")
TAR_FV = float(os.getenv("TAR_FV", "0.0835") or "0.0835")


def now_local() -> datetime:
    return datetime.now(TZ)


def fmt_date(d: datetime) -> str:
    return d.strftime("%d/%m/%Y")


def fmt_dt(d: datetime) -> str:
    return d.strftime("%d/%m/%Y às %H:%M")


def is_vazio(dt: datetime) -> bool:
    h = dt.hour
    return h >= 22 or h < 8


def periodo_label(dt: datetime) -> str:
    return "Vazio" if is_vazio(dt) else "Fora vazio"


def fetch_text(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.text


def fetch_bytes(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content


def normalize_google_sheets_url(url: str) -> str:
    if "docs.google.com/spreadsheets" in url and "/export?" not in url:
        if "/edit" in url:
            url = url.split("/edit")[0]
        if "gid=" not in url:
            return f"{url}/export?format=csv"
        gid = url.split("gid=")[-1].split("&")[0]
        return f"{url}/export?format=csv&gid={gid}"
    return url


def parse_float_pt(value) -> float:
    if pd.isna(value):
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def detect_csv_delimiter(text: str) -> str:
    sample = "\n".join(text.splitlines()[:10])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","


def find_header_row(lines: list[str]) -> int:
    for i, line in enumerate(lines[:30]):
        low = line.lower()
        if "data" in low and "hora" in low:
            return i
    return 0


def load_eredes_15m_data() -> list[dict]:
    url = normalize_google_sheets_url(EREDES_CSV_URL)
    if not url:
        print("EREDES_CSV_URL vazio.")
        return []

    if "docs.google.com" in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}t={int(now_local().timestamp())}"

    text = fetch_text(url)
    lines = text.splitlines()
    if not lines:
        return []

    delimiter = detect_csv_delimiter(text)
    header_row = find_header_row(lines)
    cleaned = "\n".join(lines[header_row:])

    df = pd.read_csv(io.StringIO(cleaned), sep=delimiter, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    col_data = next((c for c in df.columns if c.lower() == "data"), None)
    col_hora = next((c for c in df.columns if c.lower() == "hora"), None)
    col_consumo = next(
        (
            c
            for c in df.columns
            if "consumo registado" in c.lower()
            or c.lower() == "consumo"
            or "consumo medido" in c.lower()
        ),
        None,
    )
    col_estado = next((c for c in df.columns if c.lower() == "estado"), None)

    if not col_data or not col_hora or not col_consumo:
        raise RuntimeError(f"Não encontrei colunas esperadas no CSV E-REDES. Colunas: {list(df.columns)}")

    rows = []
    for _, r in df.iterrows():
        data_raw = str(r.get(col_data, "")).strip()
        hora_raw = str(r.get(col_hora, "")).strip()
        consumo_raw = r.get(col_consumo, "")
        estado_raw = str(r.get(col_estado, "")).strip() if col_estado else ""

        if not data_raw or not hora_raw:
            continue

        dt = None
        for fmt in ("%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(f"{data_raw} {hora_raw}", fmt).replace(tzinfo=TZ)
                break
            except Exception:
                pass

        if dt is None:
            continue

        consumo_kwh = parse_float_pt(consumo_raw)
        periodo = periodo_label(dt)

        rows.append(
            {
                "timestamp": dt.isoformat(),
                "date": dt.strftime("%Y-%m-%d"),
                "time": dt.strftime("%H:%M"),
                "periodo": periodo,
                "consumo_kwh": consumo_kwh,
                "estado": estado_raw,
            }
        )

    rows.sort(key=lambda x: x["timestamp"])
    return rows


def omie_url_for_date(d: datetime) -> str:
    ymd = d.strftime("%Y%m%d")
    return f"https://www.omie.es/sites/default/files/dados/AGNO_{d:%Y}/MES_{d:%m}/TXT/marginalpdbcpt_{ymd}.1"


def parse_omie_file_for_day(target_day: datetime) -> dict[int, float]:
    url = omie_url_for_date(target_day)
    text = fetch_text(url)

    prices_eur_mwh = {}
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 6:
            continue

        try:
            # formato OMIE típico: data;mercado;hora/período;...;preço
            raw_date = parts[0]
            raw_period = parts[2]
            raw_price = parts[-1].replace(",", ".")

            day = None
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
                try:
                    day = datetime.strptime(raw_date, fmt).date()
                    break
                except Exception:
                    pass

            if day != target_day.date():
                continue

            period = int(raw_period)
            price = float(raw_price)
            prices_eur_mwh[period] = price
        except Exception:
            continue

    if len(prices_eur_mwh) not in (24, 96):
        raise RuntimeError(f"OMIE: número de períodos inesperado para {target_day.date()}: {len(prices_eur_mwh)}")

    if len(prices_eur_mwh) == 24:
        expanded = {}
        for h in range(24):
            price = prices_eur_mwh.get(h + 1, 0.0)
            for q in range(4):
                expanded[h * 4 + q + 1] = price
        return expanded

    return prices_eur_mwh


def g9_price_from_omie_eur_mwh(omie_eur_mwh: float, vazio: bool) -> float:
    omie_eur_kwh = omie_eur_mwh / 1000.0
    tarifa = TAR_VAZIO if vazio else TAR_FV
    return (omie_eur_kwh * FADEQ * (1 + PERDAS)) + AC + GGS + tarifa


def apply_omie_prices(rows: list[dict]) -> list[dict]:
    by_day: dict[str, dict[int, float]] = {}

    enriched = []
    for row in rows:
        dt = datetime.fromisoformat(row["timestamp"])
        day_key = dt.strftime("%Y-%m-%d")

        if day_key not in by_day:
            by_day[day_key] = parse_omie_file_for_day(dt)

        period_15m = dt.hour * 4 + (dt.minute // 15) + 1
        omie_price = by_day[day_key].get(period_15m, 0.0)

        vazio = row["periodo"] == "Vazio"
        g9_final = g9_price_from_omie_eur_mwh(omie_price, vazio)
        custo = row["consumo_kwh"] * g9_final

        new_row = row.copy()
        new_row["omie_eur_mwh"] = omie_price
        new_row["g9_eur_kwh"] = g9_final
        new_row["custo_eur"] = custo
        enriched.append(new_row)

    return enriched


def load_master_df() -> pd.DataFrame:
    if not os.path.exists(MASTER_FILE):
        return pd.DataFrame(
            columns=[
                "timestamp",
                "date",
                "time",
                "periodo",
                "consumo_kwh",
                "estado",
                "omie_eur_mwh",
                "g9_eur_kwh",
                "custo_eur",
            ]
        )

    df = pd.read_csv(MASTER_FILE)
    if df.empty:
        return df

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).copy()
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        df["timestamp"] = df["timestamp"].str.replace(r"(\+..)(..)$", r"\1:\2", regex=True)

    for c in ("consumo_kwh", "omie_eur_mwh", "g9_eur_kwh", "custo_eur"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df


def update_master(rows: list[dict]) -> pd.DataFrame:
    incoming = pd.DataFrame(rows)
    master = load_master_df()

    combined = pd.concat([master, incoming], ignore_index=True)

    if combined.empty:
        return combined

    combined["timestamp"] = pd.to_datetime(combined["timestamp"], errors="coerce")
    combined = combined.dropna(subset=["timestamp"]).copy()

    for c in ("consumo_kwh", "omie_eur_mwh", "g9_eur_kwh", "custo_eur"):
        if c in combined.columns:
            combined[c] = pd.to_numeric(combined[c], errors="coerce").fillna(0.0)

    # Mantém apenas um registo por timestamp: o último
    combined = combined.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")

    combined["date"] = combined["timestamp"].dt.strftime("%Y-%m-%d")
    combined["time"] = combined["timestamp"].dt.strftime("%H:%M")
    combined["periodo"] = combined["timestamp"].apply(periodo_label)

    combined = combined.sort_values("timestamp").reset_index(drop=True)
    combined.to_csv(MASTER_FILE, index=False)

    return combined


def pick_best_day(df: pd.DataFrame) -> tuple[pd.Timestamp | None, int]:
    if df.empty:
        return None, 0

    temp = df.copy()
    temp["day"] = temp["timestamp"].dt.floor("D")
    counts = temp.groupby("day").size().sort_index()

    yesterday = (now_local() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = pd.Timestamp(yesterday)

    if yesterday in counts.index and counts.loc[yesterday] >= math.ceil(96 * 0.80):
        return yesterday, int(counts.loc[yesterday])

    valid = counts[counts > math.floor(96 * 0.70)]
    if not valid.empty:
        day = valid.index.max()
        return day, int(valid.loc[day])

    return None, 0


def summarize_day(df: pd.DataFrame, day: pd.Timestamp) -> dict:
    sub = df[df["timestamp"].dt.floor("D") == day].copy()

    vazio = sub[sub["periodo"] == "Vazio"]
    fv = sub[sub["periodo"] == "Fora vazio"]

    kv = float(vazio["consumo_kwh"].sum())
    kf = float(fv["consumo_kwh"].sum())
    kt = kv + kf

    cv = float(vazio["custo_eur"].sum())
    cf = float(fv["custo_eur"].sum())
    ct = cv + cf

    pv = (kv / kt * 100) if kt > 0 else 0.0
    pf = (kf / kt * 100) if kt > 0 else 0.0
    pm = (ct / kt) if kt > 0 else 0.0

    return {
        "day": day,
        "kwh_vazio": kv,
        "kwh_fv": kf,
        "kwh_total": kt,
        "pct_vazio": pv,
        "pct_fv": pf,
        "cost_vazio": cv,
        "cost_fv": cf,
        "cost_total": ct,
        "avg_price": pm,
    }


def summarize_month(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "kwh_vazio": 0.0,
            "kwh_fv": 0.0,
            "kwh_total": 0.0,
            "cost_vazio": 0.0,
            "cost_fv": 0.0,
            "cost_total": 0.0,
            "avg_price": 0.0,
            "last_update": None,
        }

    latest = df["timestamp"].max()
    month_df = df[
        (df["timestamp"].dt.year == latest.year) &
        (df["timestamp"].dt.month == latest.month)
    ].copy()

    vazio = month_df[month_df["periodo"] == "Vazio"]
    fv = month_df[month_df["periodo"] == "Fora vazio"]

    kv = float(vazio["consumo_kwh"].sum())
    kf = float(fv["consumo_kwh"].sum())
    kt = kv + kf

    cv = float(vazio["custo_eur"].sum())
    cf = float(fv["custo_eur"].sum())
    ct = cv + cf

    pm = (ct / kt) if kt > 0 else 0.0

    return {
        "kwh_vazio": kv,
        "kwh_fv": kf,
        "kwh_total": kt,
        "cost_vazio": cv,
        "cost_fv": cf,
        "cost_total": ct,
        "avg_price": pm,
        "last_update": latest,
    }


def get_today_g9_prices() -> tuple[float, float, float]:
    today = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    omie_today = parse_omie_file_for_day(today)

    vazio_prices = []
    fv_prices = []

    for period, omie_eur_mwh in sorted(omie_today.items()):
        idx0 = period - 1
        hour = idx0 // 4
        minute = (idx0 % 4) * 15
        dt = today.replace(hour=hour, minute=minute)

        final_price = g9_price_from_omie_eur_mwh(omie_eur_mwh, is_vazio(dt))
        if is_vazio(dt):
            vazio_prices.append(final_price)
        else:
            fv_prices.append(final_price)

    avg_vazio = sum(vazio_prices) / len(vazio_prices) if vazio_prices else 0.0
    avg_fv = sum(fv_prices) / len(fv_prices) if fv_prices else 0.0

    yesterday = today - timedelta(days=1)
    omie_yesterday = parse_omie_file_for_day(yesterday)
    omie_avg_yesterday = sum(omie_yesterday.values()) / len(omie_yesterday) if omie_yesterday else 0.0

    return omie_avg_yesterday, avg_vazio, avg_fv


def melhor_janela_para_consumos(preco_vazio: float, preco_fv: float) -> str:
    if preco_vazio <= 0 or preco_fv <= 0:
        return "💡 Melhor janela para consumos: sem dados válidos para sugestão."

    diff_pct = ((preco_fv - preco_vazio) / preco_fv) * 100 if preco_fv > 0 else 0.0

    if preco_vazio < preco_fv:
        return f"💡 Melhor janela para consumos: vazio (≈ {diff_pct:.0f}% mais barato do que fora vazio)."

    if preco_vazio > preco_fv:
        return "💡 Melhor janela para consumos: fora vazio, hoje está mais barato do que o habitual."

    return "💡 Melhor janela para consumos: preços muito próximos entre períodos."


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram não configurado. Mensagem:")
        print(text)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    r = requests.post(
        url,
        data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        },
        timeout=60,
    )
    r.raise_for_status()


def build_message(day_summary: dict, month_summary: dict, omie_yesterday_avg: float, g9_vazio: float, g9_fv: float) -> str:
    ref_date = now_local()
    day = pd.Timestamp(day_summary["day"]).to_pydatetime().replace(tzinfo=TZ)
    last_upd = month_summary["last_update"]
    last_upd_str = fmt_dt(last_upd.to_pydatetime().replace(tzinfo=TZ)) if last_upd is not None else "-"

    sugestao = melhor_janela_para_consumos(g9_vazio, g9_fv)

    msg = (
        f"📅 G9 - {fmt_date(ref_date)}\n\n"
        f"OMIE ({fmt_date(ref_date - timedelta(days=1))}): {omie_yesterday_avg:.1f} €/MWh\n\n"
        f"⚡ G9 Indexado\n"
        f"• Vazio: {g9_vazio:.3f} €/kWh\n"
        f"• Fora vazio: {g9_fv:.3f} €/kWh\n"
        f"{sugestao}\n\n"
        f"📊 Consumos ({fmt_date(day)})\n"
        f"• Vazio: {day_summary['kwh_vazio']:.1f} kWh ({day_summary['pct_vazio']:.1f}%)\n"
        f"• Fora vazio: {day_summary['kwh_fv']:.1f} kWh ({day_summary['pct_fv']:.1f}%)\n"
        f"• Total: {day_summary['kwh_total']:.1f} kWh\n\n"
        f"💰 Custos reais\n"
        f"• Vazio: {day_summary['cost_vazio']:.2f} €\n"
        f"• Fora vazio: {day_summary['cost_fv']:.2f} €\n"
        f"• Total: {day_summary['cost_total']:.2f} €\n"
        f"• Preço médio real: {day_summary['avg_price']:.3f} €/kWh\n\n"
        f"📅 Acumulado do mês (última atualização: {last_upd_str})\n"
        f"• Vazio: {month_summary['kwh_vazio']:.1f} kWh\n"
        f"• Fora vazio: {month_summary['kwh_fv']:.1f} kWh\n"
        f"• Total: {month_summary['kwh_total']:.1f} kWh\n\n"
        f"💸 Acumulado real (última atualização: {last_upd_str})\n"
        f"• Vazio: {month_summary['cost_vazio']:.2f} €\n"
        f"• Fora vazio: {month_summary['cost_fv']:.2f} €\n"
        f"• Total: {month_summary['cost_total']:.2f} €\n"
        f"• Preço médio real: {month_summary['avg_price']:.3f} €/kWh"
    )
    return msg


def main():
    print("A carregar dados E-REDES...")
    rows = load_eredes_15m_data()
    if not rows:
        raise RuntimeError("Sem dados E-REDES.")

    print(f"Linhas E-REDES lidas: {len(rows)}")

    print("A aplicar preços OMIE/G9...")
    enriched = apply_omie_prices(rows)

    print("A atualizar master.csv com deduplicação por timestamp...")
    master = update_master(enriched)

    if master.empty:
        raise RuntimeError("master.csv ficou vazio.")

    master["timestamp"] = pd.to_datetime(master["timestamp"], errors="coerce")
    master = master.dropna(subset=["timestamp"]).copy()

    best_day, intervals = pick_best_day(master)
    if best_day is None:
        if not FORCE_RUN:
            raise RuntimeError("Não encontrei um dia válido com dados suficientes.")
        best_day = master["timestamp"].dt.floor("D").max()

    print(f"Dia escolhido: {best_day.date()} | intervalos: {intervals}")

    day_summary = summarize_day(master, best_day)
    month_summary = summarize_month(master)

    print("A obter preços G9 médios do dia...")
    omie_yesterday_avg, g9_vazio, g9_fv = get_today_g9_prices()

    msg = build_message(day_summary, month_summary, omie_yesterday_avg, g9_vazio, g9_fv)

    print("A enviar mensagem Telegram...")
    send_telegram_message(msg)
    print("Concluído.")


if __name__ == "__main__":
    main()
