import os
import io
import csv
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

TZ = ZoneInfo("Europe/Lisbon")
MASTER_FILE = "eredes_master.xlsx"

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
    return dt.hour >= 22 or dt.hour < 8


def periodo_label(dt: datetime) -> str:
    return "Vazio" if is_vazio(dt) else "Fora vazio"


def normalize_google_sheets_url(url: str) -> str:
    if "docs.google.com/spreadsheets" not in url:
        return url

    base = url.split("?")[0]
    if "/edit" in base:
        base = base.split("/edit")[0]

    if "gid=" in url:
        gid = url.split("gid=")[-1].split("&")[0].strip()
        if gid:
            return f"{base}/export?format=csv&gid={gid}"

    return f"{base}/export?format=csv"


def fetch_text(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.text


def parse_float_safe(v) -> float:
    if pd.isna(v):
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def detect_delimiter(text: str) -> str:
    sample = "\n".join(text.splitlines()[:10])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        return dialect.delimiter
    except Exception:
        return ";" if sample.count(";") >= sample.count(",") else ","


def find_header_row(lines: list[str]) -> int:
    for i, line in enumerate(lines[:40]):
        low = line.lower()
        if "data" in low and "hora" in low and "consumo" in low:
            return i
    return 0


def normalize_eredes_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    col_data = next((c for c in df.columns if c.strip().lower() == "data"), None)
    col_hora = next((c for c in df.columns if c.strip().lower() == "hora"), None)
    col_consumo = next(
        (
            c for c in df.columns
            if "consumo registado" in c.strip().lower()
            or c.strip().lower() == "consumo"
            or c.strip().lower() == "consumo registado (kw)"
        ),
        None,
    )
    col_estado = next((c for c in df.columns if c.strip().lower() == "estado"), None)

    if not col_data or not col_hora or not col_consumo:
        raise RuntimeError(f"Não encontrei as colunas esperadas. Colunas: {list(df.columns)}")

    rows = []
    for _, r in df.iterrows():
        data_raw = str(r.get(col_data, "")).strip()
        hora_raw = str(r.get(col_hora, "")).strip()
        consumo_raw = r.get(col_consumo, "")
        estado_raw = str(r.get(col_estado, "")).strip() if col_estado else ""

        if not data_raw or not hora_raw:
            continue

        dt = None
        for fmt in ("%Y/%m/%d %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(f"{data_raw} {hora_raw}", fmt).replace(tzinfo=TZ)
                break
            except Exception:
                pass

        if dt is None:
            continue

        rows.append(
            {
                "timestamp": dt,
                "date": dt.strftime("%Y-%m-%d"),
                "time": dt.strftime("%H:%M"),
                "periodo": periodo_label(dt),
                "consumo_kwh": parse_float_safe(consumo_raw) / 4.0,
                "estado": estado_raw,
            }
        )

    return pd.DataFrame(rows)


def load_eredes_15m_data() -> list[dict]:
    url = normalize_google_sheets_url(EREDES_CSV_URL)
    if not url:
        raise RuntimeError("EREDES_CSV_URL vazio.")

    if "docs.google.com" in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}t={int(now_local().timestamp())}"

    text = fetch_text(url)
    lines = text.splitlines()
    if not lines:
        return []

    delimiter = detect_delimiter(text)
    header_row = find_header_row(lines)
    cleaned = "\n".join(lines[header_row:])

    df = pd.read_csv(io.StringIO(cleaned), sep=delimiter, dtype=str)
    norm = normalize_eredes_dataframe(df)

    if norm.empty:
        return []

    norm["timestamp"] = norm["timestamp"].apply(lambda x: x.isoformat())
    rows = norm.to_dict(orient="records")
    rows.sort(key=lambda x: x["timestamp"])
    return rows


def omie_candidate_urls_for_date(d: datetime) -> list[str]:
    ymd = d.strftime("%Y%m%d")
    filename = f"marginalpdbcpt_{ymd}.1"
    return [
        f"https://www.omie.es/pt/file-download?filename={filename}&parents=marginalpdbcpt",
        f"https://www.omie.es/en/file-download?filename={filename}&parents=marginalpdbcpt",
        f"https://www.omie.es/sites/default/files/dados/AGNO_{d:%Y}/MES_{d:%m}/TXT/{filename}",
    ]


def parse_omie_file_for_day(target_day: datetime) -> dict[int, float]:
    last_error = None
    text = None

    for url in omie_candidate_urls_for_date(target_day):
        try:
            text = fetch_text(url)
            if text and len(text.strip()) > 20:
                print(f"OMIE OK: {url}")
                break
        except Exception as e:
            last_error = e
            print(f"OMIE falhou: {url} -> {e}")

    if not text:
        raise RuntimeError(
            f"Não foi possível obter OMIE para {target_day.strftime('%Y-%m-%d')}. Último erro: {last_error}"
        )

    prices: dict[int, float] = {}
    preview = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        if len(preview) < 15:
            preview.append(line)

        parts = [p.strip() for p in line.split(";")]

        if not parts or parts[0].upper() == "MARGINALPDBCPT":
            continue

        if len(parts) >= 6:
            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                period = int(parts[3])
                price = float(parts[4].replace(",", "."))

                if datetime(year, month, day).date() != target_day.date():
                    continue

                prices[period] = price
            except Exception:
                pass

    print(f"OMIE períodos extraídos para {target_day.strftime('%Y-%m-%d')}: {len(prices)}")

    if len(prices) == 24:
        expanded = {}
        for h in range(24):
            p = prices.get(h + 1, 0.0)
            for q in range(4):
                expanded[h * 4 + q + 1] = p
        return expanded

    if len(prices) == 96:
        return prices

    raise RuntimeError(
        f"OMIE com número de períodos inesperado em {target_day.date()}: {len(prices)}\n"
        f"Primeiras linhas do ficheiro:\n" + "\n".join(preview)
    )


def g9_price_from_omie_eur_mwh(omie_eur_mwh: float, vazio: bool) -> float:
    omie_eur_kwh = omie_eur_mwh / 1000.0
    tarifa = TAR_VAZIO if vazio else TAR_FV
    return (omie_eur_kwh * FADEQ * (1 + PERDAS)) + AC + GGS + tarifa


def apply_omie_prices(rows: list[dict]) -> list[dict]:
    omie_cache: dict[str, dict[int, float]] = {}
    enriched = []

    for row in rows:
        dt = datetime.fromisoformat(row["timestamp"])
        day_key = dt.strftime("%Y-%m-%d")

        if day_key not in omie_cache:
            omie_cache[day_key] = parse_omie_file_for_day(dt)

        period_15m = dt.hour * 4 + (dt.minute // 15) + 1
        omie_eur_mwh = omie_cache[day_key].get(period_15m, 0.0)

        final_price = g9_price_from_omie_eur_mwh(omie_eur_mwh, row["periodo"] == "Vazio")
        custo = row["consumo_kwh"] * final_price

        new_row = row.copy()
        new_row["omie_eur_mwh"] = omie_eur_mwh
        new_row["g9_eur_kwh"] = final_price
        new_row["custo_eur"] = custo
        enriched.append(new_row)

    return enriched


def load_master_df() -> pd.DataFrame:
    if not os.path.exists(MASTER_FILE):
        return pd.DataFrame(
            columns=[
                "timestamp", "date", "time", "periodo", "consumo_kwh", "estado",
                "omie_eur_mwh", "g9_eur_kwh", "custo_eur",
            ]
        )

    raw = pd.read_excel(MASTER_FILE, engine="openpyxl")
    if raw.empty:
        return raw

    raw.columns = [str(c).strip() for c in raw.columns]

    if "timestamp" in raw.columns:
        df = raw.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).copy()
    else:
        df = normalize_eredes_dataframe(raw)

    for c in ["consumo_kwh", "omie_eur_mwh", "g9_eur_kwh", "custo_eur"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    if "estado" not in df.columns:
        df["estado"] = ""

    if "date" not in df.columns:
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    if "time" not in df.columns:
        df["time"] = df["timestamp"].dt.strftime("%H:%M")

    if "periodo" not in df.columns:
        df["periodo"] = df["timestamp"].apply(periodo_label)

    try:
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    except Exception:
        pass

    return df[
        [
            "timestamp", "date", "time", "periodo", "consumo_kwh", "estado",
            "omie_eur_mwh", "g9_eur_kwh", "custo_eur",
        ]
    ].copy()


def update_master(rows: list[dict]) -> pd.DataFrame:
    incoming = pd.DataFrame(rows)
    master = load_master_df()

    combined = pd.concat([master, incoming], ignore_index=True)
    if combined.empty:
        return combined

    combined["timestamp"] = pd.to_datetime(combined["timestamp"], errors="coerce", utc=True)
    combined = combined.dropna(subset=["timestamp"]).copy()

    for c in ["consumo_kwh", "omie_eur_mwh", "g9_eur_kwh", "custo_eur"]:
        if c not in combined.columns:
            combined[c] = 0.0
        combined[c] = pd.to_numeric(combined[c], errors="coerce").fillna(0.0)

    if "estado" not in combined.columns:
        combined["estado"] = ""

    combined = combined.sort_values("timestamp")
    combined = combined.drop_duplicates(subset=["timestamp"], keep="last")

    combined["timestamp"] = combined["timestamp"].dt.tz_convert("Europe/Lisbon")
    combined["date"] = combined["timestamp"].dt.strftime("%Y-%m-%d")
    combined["time"] = combined["timestamp"].dt.strftime("%H:%M")
    combined["periodo"] = combined["timestamp"].apply(periodo_label)
    combined["timestamp"] = combined["timestamp"].dt.tz_localize(None)

    combined = combined[
        [
            "timestamp", "date", "time", "periodo", "consumo_kwh", "estado",
            "omie_eur_mwh", "g9_eur_kwh", "custo_eur",
        ]
    ].sort_values("timestamp").reset_index(drop=True)

    combined.to_excel(MASTER_FILE, index=False, engine="openpyxl")
    return combined


def pick_best_day(df: pd.DataFrame) -> tuple[pd.Timestamp | None, int]:
    if df.empty:
        return None, 0

    temp = df.copy()
    temp["day"] = temp["timestamp"].dt.floor("D")
    counts = temp.groupby("day").size().sort_index()

    yesterday = pd.Timestamp(
        (now_local() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)
    )

    if yesterday in counts.index and counts.loc[yesterday] >= math.ceil(96 * 0.80):
        return yesterday, int(counts.loc[yesterday])

    valid = counts[counts > math.floor(96 * 0.70)]
    if not valid.empty:
        best = valid.index.max()
        return best, int(valid.loc[best])

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

    return {
        "day": day,
        "kwh_vazio": kv,
        "kwh_fv": kf,
        "kwh_total": kt,
        "pct_vazio": (kv / kt * 100) if kt > 0 else 0.0,
        "pct_fv": (kf / kt * 100) if kt > 0 else 0.0,
        "cost_vazio": cv,
        "cost_fv": cf,
        "cost_total": ct,
        "avg_price": (ct / kt) if kt > 0 else 0.0,
    }


def summarize_month(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "kwh_vazio": 0.0, "kwh_fv": 0.0, "kwh_total": 0.0,
            "cost_vazio": 0.0, "cost_fv": 0.0, "cost_total": 0.0,
            "avg_price": 0.0, "last_update": None,
        }

    latest = df["timestamp"].max()
    month_df = df[
        (df["timestamp"].dt.year == latest.year)
        & (df["timestamp"].dt.month == latest.month)
    ].copy()

    vazio = month_df[month_df["periodo"] == "Vazio"]
    fv = month_df[month_df["periodo"] == "Fora vazio"]

    kv = float(vazio["consumo_kwh"].sum())
    kf = float(fv["consumo_kwh"].sum())
    kt = kv + kf

    cv = float(vazio["custo_eur"].sum())
    cf = float(fv["custo_eur"].sum())
    ct = cv + cf

    return {
        "kwh_vazio": kv,
        "kwh_fv": kf,
        "kwh_total": kt,
        "cost_vazio": cv,
        "cost_fv": cf,
        "cost_total": ct,
        "avg_price": (ct / kt) if kt > 0 else 0.0,
        "last_update": latest,
    }


def build_best_windows(slots: list[dict]) -> str:
    if not slots:
        return "sem dados"

    prices = [s["price"] for s in slots]
    threshold_idx = max(1, round(len(prices) * 0.35)) - 1
    threshold = sorted(prices)[threshold_idx]

    selected = [s for s in slots if s["price"] <= threshold]
    if not selected:
        selected = sorted(slots, key=lambda x: x["price"])[:24]

    hours = sorted(set(s["dt"].hour for s in selected))
    if not hours:
        return "sem dados"

    groups = []
    start = hours[0]
    prev = hours[0]

    for h in hours[1:]:
        if h == prev + 1:
            prev = h
        else:
            groups.append((start, prev + 1))
            start = h
            prev = h

    groups.append((start, prev + 1))

    def fmt_range(a: int, b: int) -> str:
        end = 24 if b >= 24 else b
        return f"{a:02d}-{end:02d}"

    return " e ".join(fmt_range(a, b) for a, b in groups)


def get_today_g9_prices_and_windows() -> tuple[float, float, float, str]:
    today = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    omie_today = parse_omie_file_for_day(today)

    vazio_prices = []
    fv_prices = []
    slots = []

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

        slots.append({"dt": dt, "price": final_price})

    omie_yesterday = parse_omie_file_for_day(today - timedelta(days=1))
    omie_yesterday_avg = sum(omie_yesterday.values()) / len(omie_yesterday) if omie_yesterday else 0.0

    avg_vazio = sum(vazio_prices) / len(vazio_prices) if vazio_prices else 0.0
    avg_fv = sum(fv_prices) / len(fv_prices) if fv_prices else 0.0

    janela = build_best_windows(slots)
    return omie_yesterday_avg, avg_vazio, avg_fv, janela


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN em falta no ambiente.")
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID em falta no ambiente.")

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    r = requests.post(
        url,
        data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=60,
    )
    print("Telegram status:", r.status_code)
    print("Telegram response:", r.text)
    r.raise_for_status()


def build_message(day_summary: dict, month_summary: dict, omie_yesterday_avg: float, g9_vazio: float, g9_fv: float, janela: str) -> str:
    ref_date = now_local()
    day = pd.Timestamp(day_summary["day"]).to_pydatetime()
    last_upd = month_summary["last_update"]
    last_upd_str = fmt_dt(last_upd.to_pydatetime()) if last_upd is not None else "-"

    return (
        f"📅 G9 - {fmt_date(ref_date)}\n\n"
        f"OMIE ({fmt_date(ref_date - timedelta(days=1))}): {omie_yesterday_avg:.1f} €/MWh\n\n"
        f"⚡ G9 Indexado\n"
        f"• Vazio: {g9_vazio:.3f} €/kWh\n"
        f"• Fora vazio: {g9_fv:.3f} €/kWh\n"
        f"💡 Melhor janela para consumos: {janela}\n\n"
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


def main():
    print("A ler E-REDES...")
    rows = load_eredes_15m_data()
    if not rows:
        raise RuntimeError("Sem dados E-REDES.")

    print(f"Linhas lidas: {len(rows)}")

    print("A aplicar preços OMIE/G9...")
    enriched = apply_omie_prices(rows)

    print(f"A atualizar {MASTER_FILE}...")
    master = update_master(enriched)

    master["timestamp"] = pd.to_datetime(master["timestamp"], errors="coerce")
    master = master.dropna(subset=["timestamp"]).copy()

    best_day, intervals = pick_best_day(master)
    if best_day is None:
        if not FORCE_RUN:
            raise RuntimeError("Não encontrei dia válido com dados suficientes.")
        best_day = master["timestamp"].dt.floor("D").max()

    print(f"Dia escolhido: {best_day.date()} ({intervals} intervalos)")

    day_summary = summarize_day(master, best_day)
    month_summary = summarize_month(master)

    print("A calcular preços médios e janela ideal...")
    omie_yesterday_avg, g9_vazio, g9_fv, janela = get_today_g9_prices_and_windows()

    msg = build_message(day_summary, month_summary, omie_yesterday_avg, g9_vazio, g9_fv, janela)

    print("A enviar Telegram...")
    send_telegram_message(msg)
    print("Concluído.")


if __name__ == "__main__":
    main()
