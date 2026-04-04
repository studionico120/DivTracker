"""
配当プラス - 株価データ自動更新スクリプト v3.0
GitHub Actions から週次で自動実行される。

v3.0 変更点（v2.1からの改善）：
  - yf.download() による一括株価取得で大幅高速化
    （4,000銘柄でも数分で完了）
  - 配当詳細の個別取得は「配当がある銘柄だけ」に絞り込み
  - 並列取得（ThreadPoolExecutor）で配当取得も高速化

処理フロー：
  Phase 1: yf.download() で全銘柄の株価を一括取得（数十秒）
  Phase 2: yf.Ticker().info で配当利回り・配当額を取得（主要銘柄のみ）
  Phase 3: yf.Ticker().dividends で配当履歴を取得（配当銘柄のみ）
"""

import yfinance as yf
import pandas as pd
import json
import time
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# === 設定 ===
# 配当詳細を個別取得する際のバッチ設定
DIV_BATCH_SIZE = 20           # 並列取得の同時実行数
DIV_SLEEP_BETWEEN = 2         # バッチ間のスリープ（秒）
MAX_RETRIES = 1               # 失敗時のリトライ（回数を減らして高速化）

# 支払い月の推定オフセット
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

# グローバル変数（起動時にCSVから読み込む）
JP_TICKERS: dict[str, tuple[str, str]] = {}
US_TICKERS: dict[str, str] = {}


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# === 銘柄マスタの読み込み ===

def load_jp_master() -> dict[str, tuple[str, str]]:
    """日本株マスタCSV → { "8058.T": ("三菱商事", "商社") }"""
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
    """米国株マスタCSV → { "AAPL": "Technology" }"""
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
    """手動補正データを読み込む"""
    if OVERRIDES_FILE.exists():
        try:
            data = json.loads(OVERRIDES_FILE.read_text(encoding="utf-8"))
            log(f"overrides.json: {len(data)}銘柄の補正データを読み込み")
            return data
        except Exception as e:
            log(f"⚠ overrides.json 読み込みエラー: {e}")
    return {}


# === Phase 1: 一括株価取得 ===

def bulk_download_prices(symbols: list[str]) -> dict[str, float]:
    """
    yf.download() で全銘柄の終値を一括取得する。
    数千銘柄でも1回のAPIコールで済むため圧倒的に速い。
    
    戻り値: { "8058.T": 2500.0, "AAPL": 195.5, ... }
    """
    if not symbols:
        return {}

    log(f"一括株価取得: {len(symbols)}銘柄...")

    try:
        # 直近5日分をダウンロード（最新の終値を取るため）
        df = yf.download(
            symbols,
            period="5d",
            progress=False,
            threads=True,
        )

        if df.empty:
            log("⚠ 株価データが空でした")
            return {}

        prices = {}

        if len(symbols) == 1:
            # 1銘柄の場合、DataFrameの構造が異なる
            last_price = df["Close"].dropna().iloc[-1] if not df["Close"].dropna().empty else 0
            prices[symbols[0]] = round(float(last_price), 2)
        else:
            # 複数銘柄の場合
            close = df["Close"]
            for symbol in symbols:
                if symbol in close.columns:
                    series = close[symbol].dropna()
                    if not series.empty:
                        prices[symbol] = round(float(series.iloc[-1]), 2)

        log(f"  ✓ {len(prices)}/{len(symbols)}銘柄の株価を取得")
        return prices

    except Exception as e:
        log(f"  ✗ 一括取得エラー: {e}")
        return {}


# === Phase 2: 配当情報の個別取得 ===

def estimate_payment_month(ex_date: datetime, market: str) -> str:
    offset = PAYMENT_OFFSET_JP if market == "JP" else PAYMENT_OFFSET_US
    payment_date = ex_date + relativedelta(months=offset)
    return payment_date.strftime("%Y-%m")


def fetch_dividend_info(symbol: str, market: str) -> dict | None:
    """
    1銘柄の配当情報（利回り、年間配当額、配当履歴）を取得する。
    株価は Phase 1 で取得済みなので、ここでは配当関連のみ。
    """
    for attempt in range(MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}

            dividend_yield = info.get("dividendYield", 0) or 0
            dividend_rate = info.get("dividendRate", 0) or 0
            short_name = info.get("shortName", "") or info.get("longName", "") or ""

            # 配当がない銘柄は最小限の情報だけ返す
            if dividend_yield == 0 and dividend_rate == 0:
                return {
                    "yield_x100": 0,
                    "annual_div": 0,
                    "yf_name": short_name,
                    "div_details": "",
                }

            # 次回支払い日の取得を試みる
            next_payment = None
            div_date = info.get("dividendDate")
            if div_date and isinstance(div_date, (int, float)):
                try:
                    next_payment = datetime.fromtimestamp(div_date).strftime("%Y-%m")
                except Exception:
                    pass

            # 配当履歴の取得
            div_details = get_dividend_details(ticker, market, next_payment)

            return {
                "yield_x100": round(dividend_yield * 10000),
                "annual_div": round(dividend_rate, 4),
                "yf_name": short_name,
                "div_details": div_details,
            }

        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(3)
            else:
                return None

    return None


def get_dividend_details(ticker_obj, market: str, next_payment: str | None) -> str:
    """配当履歴を ex:|pay: 形式で取得する"""
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


def fetch_dividends_parallel(symbols: list[str], market: str) -> dict[str, dict]:
    """
    配当情報を並列で取得する。
    ThreadPoolExecutor で同時に DIV_BATCH_SIZE 件を処理。
    """
    results = {}
    total = len(symbols)

    if total == 0:
        return results

    log(f"配当情報の並列取得: {total}銘柄 (並列数: {DIV_BATCH_SIZE})")

    completed = 0
    with ThreadPoolExecutor(max_workers=DIV_BATCH_SIZE) as executor:
        futures = {
            executor.submit(fetch_dividend_info, sym, market): sym
            for sym in symbols
        }

        for future in as_completed(futures):
            symbol = futures[future]
            completed += 1
            try:
                data = future.result()
                if data:
                    results[symbol] = data
            except Exception as e:
                log(f"  ✗ {symbol}: {e}")

            # 進捗表示（100銘柄ごと）
            if completed % 100 == 0 or completed == total:
                log(f"  進捗: {completed}/{total}")

    log(f"  ✓ {len(results)}/{total}銘柄の配当情報を取得")
    return results


# === データ統合 ===

def build_stock_data(
    symbols: list[str],
    prices: dict[str, float],
    div_info: dict[str, dict],
    market: str,
) -> list[dict]:
    """株価と配当情報を統合して、CSV出力用の辞書リストを作る"""
    results = []

    for symbol in symbols:
        price = prices.get(symbol, 0)

        # 配当情報（取得できていれば）
        dinfo = div_info.get(symbol, {})
        yield_x100 = dinfo.get("yield_x100", 0)
        annual_div = dinfo.get("annual_div", 0)
        yf_name = dinfo.get("yf_name", "")
        div_details = dinfo.get("div_details", "")

        # 企業名の決定
        if market == "JP":
            entry = JP_TICKERS.get(symbol)
            name = entry[0] if entry else yf_name
            sector = entry[1] if entry else ""
        else:
            name = yf_name
            sector = US_TICKERS.get(symbol, "")

        results.append({
            "ticker": symbol,
            "name": name,
            "price": price,
            "yield_x100": yield_x100,
            "annual_div": annual_div,
            "sector": sector,
            "div_details": div_details,
        })

    return results


# === CSV書き出し ===

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
    """株価が取得できなかった銘柄を前回データで補完する"""
    if existing_df is None:
        return new_data

    new_tickers = {d["ticker"] for d in new_data if d["price"] > 0}
    no_price = [d for d in new_data if d["price"] == 0]

    if ticker_col not in existing_df.columns:
        return new_data

    # 株価0の銘柄を既存データで補完
    for d in no_price:
        # ticker_col の値を作る（日本株は .T を除去して比較）
        lookup_key = d["ticker"].replace(".T", "") if ".T" in d["ticker"] else d["ticker"]
        match = existing_df[existing_df[ticker_col] == lookup_key]
        if not match.empty:
            row = match.iloc[0]
            cols = list(existing_df.columns)
            try:
                d["price"] = float(row[cols[2]]) if len(cols) > 2 else 0
                if d["yield_x100"] == 0 and len(cols) > 3:
                    d["yield_x100"] = int(row[cols[3]])
                if d["annual_div"] == 0 and len(cols) > 4:
                    d["annual_div"] = float(row[cols[4]])
                if not d["div_details"] and len(cols) > 6:
                    d["div_details"] = str(row[cols[6]]) if pd.notna(row[cols[6]]) else ""
            except (ValueError, IndexError):
                pass

    return new_data


def write_jp_csv(data: list[dict], path: Path):
    rows = []
    for d in sorted(data, key=lambda x: x["ticker"]):
        code = d["ticker"].replace(".T", "")
        details = d.get("div_details", "")
        if "," in details:
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
        if "," in details:
            details = f'"{details}"'
        rows.append(
            f'{d["ticker"]},{d["name"]},{d["price"]},{d["yield_x100"]},'
            f'{d["annual_div"]},{d["sector"]},{details}'
        )
    header = "Ticker,Company,Price,Yield(%),AnnualDiv,Sector,DivDetails"
    content = header + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    log(f"✓ {path.name}: {len(rows)}銘柄を書き出し")


# === メイン処理 ===

def main():
    log("=" * 60)
    log("配当プラス 株価データ自動更新 v3.0")
    log("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 銘柄マスタの読み込み
    global JP_TICKERS, US_TICKERS
    JP_TICKERS = load_jp_master()
    US_TICKERS = load_us_master()
    log(f"銘柄マスタ: 日本株 {len(JP_TICKERS)}銘柄, 米国株 {len(US_TICKERS)}銘柄")

    if not JP_TICKERS and not US_TICKERS:
        log("✗ 銘柄マスタが空です")
        sys.exit(1)

    overrides = load_overrides()
    existing_jp = load_existing_csv(JP_CSV)
    existing_us = load_existing_csv(US_CSV)

    all_jp = list(JP_TICKERS.keys())
    all_us = list(US_TICKERS.keys())
    all_symbols = all_jp + all_us

    # ================================================
    # Phase 1: 全銘柄の株価を一括取得（数十秒で完了）
    # ================================================
    log("\n=== Phase 1: 一括株価取得 ===")
    prices = bulk_download_prices(all_symbols)

    # ================================================
    # Phase 2: 配当情報の並列取得
    # ================================================
    log("\n=== Phase 2: 日本株 配当情報取得 ===")
    jp_div = fetch_dividends_parallel(all_jp, market="JP")

    log("\n=== Phase 2: 米国株 配当情報取得 ===")
    us_div = fetch_dividends_parallel(all_us, market="US")

    # ================================================
    # Phase 3: データ統合・補完・書き出し
    # ================================================
    log("\n=== Phase 3: データ統合 ===")

    jp_data = build_stock_data(all_jp, prices, jp_div, market="JP")
    jp_data = apply_overrides(jp_data, overrides)
    jp_data = merge_with_existing(jp_data, existing_jp, "銘柄コード")
    write_jp_csv(jp_data, JP_CSV)

    us_data = build_stock_data(all_us, prices, us_div, market="US")
    us_data = apply_overrides(us_data, overrides)
    us_data = merge_with_existing(us_data, existing_us, "Ticker")
    write_us_csv(us_data, US_CSV)

    # サマリー
    jp_with_div = sum(1 for d in jp_data if d["yield_x100"] > 0)
    us_with_div = sum(1 for d in us_data if d["yield_x100"] > 0)
    log("\n" + "=" * 60)
    log(f"完了:")
    log(f"  日本株: {len(jp_data)}銘柄（うち配当あり {jp_with_div}）")
    log(f"  米国株: {len(us_data)}銘柄（うち配当あり {us_with_div}）")
    log(f"  株価取得: {len(prices)}/{len(all_symbols)}")
    if overrides:
        log(f"  手動補正: {len(overrides)}銘柄")
    log("=" * 60)


if __name__ == "__main__":
    main()
