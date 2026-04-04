"""
配当プラス - 株価データ自動更新スクリプト v3.1
全銘柄を逐次取得する方式（レート制限回避重視）

設計方針：
  - yf.Ticker() で1銘柄ずつ取得（一括ダウンロードは使わない）
  - バッチ間にスリープを入れ、Yahoo Finance のレート制限を回避
  - 6時間タイムアウトを前提に、16,000銘柄の完走を目指す

スリープ戦略：
  - 10銘柄ごとに3秒
  - 100銘柄ごとに15秒 + 進捗ログ
  - 500銘柄ごとに60秒の長め休憩
  - レート制限検知時は120秒待機

想定所要時間：
  16,000銘柄 × 約1.2秒/銘柄 = 約3.5〜4.5時間
"""

import yfinance as yf
import pandas as pd
import json
import time
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === 設定 ===
BATCH_SIZE = 10
SLEEP_BETWEEN_BATCHES = 3
SLEEP_EVERY_100 = 15
SLEEP_EVERY_500 = 60
SLEEP_ON_ERROR = 10
SLEEP_RATE_LIMITED = 120
MAX_RETRIES = 1

PAYMENT_OFFSET_JP = 3
PAYMENT_OFFSET_US = 1

# パス設定
REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
JP_CSV = DATA_DIR / "jp_stocks.csv"
US_CSV = DATA_DIR / "us_stocks.csv"
OVERRIDES_FILE = DATA_DIR / "overrides.json"
JP_MASTER = DATA_DIR / "jp_ticker_master.csv"
US_MASTER = DATA_DIR / "us_ticker_master.csv"

# グローバル
JP_TICKERS: dict[str, tuple[str, str]] = {}
US_TICKERS: dict[str, str] = {}
rate_limit_count = 0


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# === 銘柄マスタ読み込み ===

def load_jp_master() -> dict[str, tuple[str, str]]:
    result = {}
    if not JP_MASTER.exists():
        log(f"⚠ {JP_MASTER} が見つかりません")
        return result
    df = pd.read_csv(JP_MASTER, dtype=str).fillna("")
    for _, row in df.iterrows():
        ticker = str(row["ticker"]).strip()
        if not ticker:
            continue
        name = str(row["name"]).strip()
        sector = str(row["sector"]).strip()
        result[f"{ticker}.T"] = (name, sector)
    return result


def load_us_master() -> dict[str, str]:
    result = {}
    if not US_MASTER.exists():
        log(f"⚠ {US_MASTER} が見つかりません")
        return result
    df = pd.read_csv(US_MASTER, dtype=str).fillna("")
    for _, row in df.iterrows():
        ticker = str(row["ticker"]).strip()
        if not ticker:
            continue
        sector = str(row["sector"]).strip()
        result[ticker] = sector
    return result


def load_overrides() -> dict:
    if OVERRIDES_FILE.exists():
        try:
            data = json.loads(OVERRIDES_FILE.read_text(encoding="utf-8"))
            log(f"overrides.json: {len(data)}銘柄の補正データを読み込み")
            return data
        except Exception as e:
            log(f"⚠ overrides.json 読み込みエラー: {e}")
    return {}


# === 配当関連 ===

def estimate_payment_month(ex_date: datetime, market: str) -> str:
    offset = PAYMENT_OFFSET_JP if market == "JP" else PAYMENT_OFFSET_US
    payment_date = ex_date + relativedelta(months=offset)
    return payment_date.strftime("%Y-%m")


def get_dividend_details(ticker_obj, market: str) -> str:
    try:
        dividends = ticker_obj.dividends
        if dividends is None or dividends.empty:
            return ""

        one_year_ago = pd.Timestamp.now(tz="UTC") - pd.DateOffset(years=1)
        if dividends.index.tz is None:
            dividends.index = dividends.index.tz_localize("UTC")
        recent = dividends[dividends.index >= one_year_ago]
        if recent.empty:
            return ""

        next_payment = None
        try:
            info = ticker_obj.info or {}
            div_date = info.get("dividendDate")
            if div_date and isinstance(div_date, (int, float)):
                next_payment = datetime.fromtimestamp(div_date).strftime("%Y-%m")
        except Exception:
            pass

        parts = []
        items = list(recent.items())
        for idx, (date, amount) in enumerate(items):
            ex_date_str = date.strftime("%Y-%m-%d")
            is_latest = (idx == len(items) - 1)
            if is_latest and next_payment:
                pay_month = next_payment
            else:
                pay_month = estimate_payment_month(date.to_pydatetime(), market)
            parts.append(f"ex:{ex_date_str}|pay:{pay_month}:{round(amount, 4)}")

        return ", ".join(parts)
    except Exception:
        return ""


# === 個別銘柄取得 ===

def fetch_single_ticker(ticker_symbol: str, market: str) -> dict | None:
    global rate_limit_count

    for attempt in range(MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            if not info or "regularMarketPrice" not in info:
                if attempt < MAX_RETRIES:
                    time.sleep(SLEEP_ON_ERROR)
                    continue
                return None

            price = info.get("regularMarketPrice", 0) or 0
            dividend_yield = info.get("dividendYield", 0) or 0
            dividend_rate = info.get("dividendRate", 0) or 0
            name = info.get("shortName", "") or info.get("longName", "") or ""

            # 配当詳細は配当がある銘柄のみ取得（時間節約）
            div_details = ""
            if dividend_rate > 0:
                div_details = get_dividend_details(ticker, market)

            return {
                "price": round(price, 2),
                "yield_x100": round(dividend_yield * 10000),
                "annual_div": round(dividend_rate, 4),
                "yf_name": name,
                "div_details": div_details,
            }

        except Exception as e:
            err_str = str(e)
            if "Too Many Requests" in err_str or "Rate" in err_str:
                rate_limit_count += 1
                log(f"  ⚠ レート制限検知（{rate_limit_count}回目）: {ticker_symbol}")
                log(f"    → {SLEEP_RATE_LIMITED}秒待機...")
                time.sleep(SLEEP_RATE_LIMITED)
                continue
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_ON_ERROR)
            else:
                return None

    return None


# === 市場ごとの逐次取得 ===

def fetch_market(symbols: list[str], market: str) -> list[dict]:
    results = []
    total = len(symbols)
    success = 0
    skipped = 0
    start_time = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]

        for symbol in batch:
            data = fetch_single_ticker(symbol, market)
            if data:
                data["ticker"] = symbol
                results.append(data)
                success += 1
            else:
                results.append({
                    "ticker": symbol,
                    "price": 0,
                    "yield_x100": 0,
                    "annual_div": 0,
                    "yf_name": "",
                    "div_details": "",
                })
                skipped += 1

        fetched_so_far = i + len(batch)

        # バッチ間スリープ
        if i + BATCH_SIZE < total:
            time.sleep(SLEEP_BETWEEN_BATCHES)

        # 100銘柄ごと：進捗ログ + 追加スリープ
        if fetched_so_far % 100 < BATCH_SIZE:
            elapsed = time.time() - start_time
            rate = fetched_so_far / elapsed if elapsed > 0 else 0
            remaining = (total - fetched_so_far) / rate if rate > 0 else 0
            log(f"  進捗: {fetched_so_far}/{total} "
                f"(成功:{success} スキップ:{skipped}) "
                f"残り約{int(remaining / 60)}分")
            if fetched_so_far < total:
                time.sleep(SLEEP_EVERY_100)

        # 500銘柄ごと：長め休憩
        if fetched_so_far % 500 < BATCH_SIZE and fetched_so_far > 0 and fetched_so_far < total:
            log(f"  ... 500銘柄到達 → {SLEEP_EVERY_500}秒の長め休憩 ...")
            time.sleep(SLEEP_EVERY_500)

    elapsed_total = time.time() - start_time
    log(f"  完了: {success}/{total}銘柄取得 "
        f"({int(elapsed_total / 60)}分{int(elapsed_total % 60)}秒)")

    return results


# === データ統合・書き出し ===

def build_final_data(raw_data: list[dict], market: str) -> list[dict]:
    for d in raw_data:
        symbol = d["ticker"]
        if market == "JP":
            entry = JP_TICKERS.get(symbol)
            d["name"] = entry[0] if entry else d.get("yf_name", "")
            d["sector"] = entry[1] if entry else ""
        else:
            d["name"] = d.get("yf_name", "")
            d["sector"] = US_TICKERS.get(symbol, "")
    return raw_data


def apply_overrides(data: list[dict], overrides: dict) -> list[dict]:
    for d in data:
        ticker = d["ticker"]
        if ticker in overrides:
            for key, value in overrides[ticker].items():
                if key.startswith("_"):
                    continue
                d[key] = value
                log(f"  📝 {ticker}: {key} を手動値で上書き")
    return data


def load_existing_csv(path: Path) -> pd.DataFrame | None:
    if path.exists():
        try:
            return pd.read_csv(path, dtype=str)
        except Exception:
            return None
    return None


def merge_with_existing(new_data: list[dict], existing_df: pd.DataFrame | None,
                        ticker_col: str) -> list[dict]:
    if existing_df is None:
        return new_data
    if ticker_col not in existing_df.columns:
        return new_data

    for d in new_data:
        if d["price"] > 0:
            continue
        lookup = d["ticker"].replace(".T", "") if ".T" in d["ticker"] else d["ticker"]
        match = existing_df[existing_df[ticker_col] == lookup]
        if not match.empty:
            row = match.iloc[0]
            cols = list(existing_df.columns)
            try:
                d["price"] = float(row[cols[2]]) if len(cols) > 2 else 0
                if d["yield_x100"] == 0 and len(cols) > 3:
                    d["yield_x100"] = int(float(row[cols[3]])) if row[cols[3]] else 0
                if d["annual_div"] == 0 and len(cols) > 4:
                    d["annual_div"] = float(row[cols[4]]) if row[cols[4]] else 0
                if not d.get("div_details") and len(cols) > 6:
                    d["div_details"] = str(row[cols[6]]) if pd.notna(row[cols[6]]) else ""
            except (ValueError, IndexError):
                pass

    return new_data


def write_jp_csv(data: list[dict], path: Path):
    rows = []
    for d in sorted(data, key=lambda x: x["ticker"]):
        code = d["ticker"].replace(".T", "")
        details = d.get("div_details", "")
        if "," in str(details):
            details = f'"{details}"'
        rows.append(
            f'{code},{d["name"]},{d["price"]},{d["yield_x100"]},'
            f'{d["annual_div"]},{d["sector"]},{details}'
        )
    header = "銘柄コード,企業名,価格,利回り(%),年間配当,セクター,配当内訳"
    content = header + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    log(f"✓ {path.name}: {len(rows)}銘柄を書き出し")


def write_us_csv(data: list[dict], path: Path):
    rows = []
    for d in sorted(data, key=lambda x: x["ticker"]):
        details = d.get("div_details", "")
        if "," in str(details):
            details = f'"{details}"'
        rows.append(
            f'{d["ticker"]},{d["name"]},{d["price"]},{d["yield_x100"]},'
            f'{d["annual_div"]},{d["sector"]},{details}'
        )
    header = "Ticker,Company,Price,Yield(%),AnnualDiv,Sector,DivDetails"
    content = header + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    log(f"✓ {path.name}: {len(rows)}銘柄を書き出し")


# === メイン ===

def main():
    log("=" * 60)
    log("配当プラス 株価データ自動更新 v3.1")
    log("（逐次取得方式・レート制限回避重視）")
    log("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    global JP_TICKERS, US_TICKERS
    JP_TICKERS = load_jp_master()
    US_TICKERS = load_us_master()
    total = len(JP_TICKERS) + len(US_TICKERS)
    log(f"銘柄マスタ: 日本株 {len(JP_TICKERS)} + 米国株 {len(US_TICKERS)} = {total}銘柄")

    estimated_minutes = int(total * 1.2 / 60)
    log(f"推定所要時間: 約{estimated_minutes}分（{estimated_minutes // 60}時間{estimated_minutes % 60}分）")

    if not JP_TICKERS and not US_TICKERS:
        log("✗ 銘柄マスタが空です")
        sys.exit(1)

    overrides = load_overrides()
    existing_jp = load_existing_csv(JP_CSV)
    existing_us = load_existing_csv(US_CSV)

    # --- 日本株 ---
    log("\n" + "=" * 60)
    log(f"=== 日本株 ({len(JP_TICKERS)}銘柄) ===")
    log("=" * 60)
    jp_raw = fetch_market(list(JP_TICKERS.keys()), market="JP")
    jp_data = build_final_data(jp_raw, market="JP")
    jp_data = apply_overrides(jp_data, overrides)
    jp_data = merge_with_existing(jp_data, existing_jp, "銘柄コード")
    write_jp_csv(jp_data, JP_CSV)

    log(f"\n... 市場切替: {SLEEP_EVERY_500}秒待機 ...\n")
    time.sleep(SLEEP_EVERY_500)

    # --- 米国株 ---
    log("=" * 60)
    log(f"=== 米国株 ({len(US_TICKERS)}銘柄) ===")
    log("=" * 60)
    us_raw = fetch_market(list(US_TICKERS.keys()), market="US")
    us_data = build_final_data(us_raw, market="US")
    us_data = apply_overrides(us_data, overrides)
    us_data = merge_with_existing(us_data, existing_us, "Ticker")
    write_us_csv(us_data, US_CSV)

    # --- サマリー ---
    jp_ok = sum(1 for d in jp_data if d["price"] > 0)
    us_ok = sum(1 for d in us_data if d["price"] > 0)
    jp_div = sum(1 for d in jp_data if d["yield_x100"] > 0)
    us_div = sum(1 for d in us_data if d["yield_x100"] > 0)

    log("\n" + "=" * 60)
    log("最終結果:")
    log(f"  日本株: {jp_ok}/{len(jp_data)}銘柄の株価取得（うち配当あり {jp_div}）")
    log(f"  米国株: {us_ok}/{len(us_data)}銘柄の株価取得（うち配当あり {us_div}）")
    if rate_limit_count > 0:
        log(f"  ⚠ レート制限: {rate_limit_count}回検知")
    log("=" * 60)


if __name__ == "__main__":
    main()
