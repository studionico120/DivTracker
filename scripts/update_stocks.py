"""
配当プラス - 株価データ自動更新スクリプト v2.0
GitHub Actions から週次で自動実行される。

v2.0 変更点：
  - 支払い月の推定ロジックを追加
  - CSVフォーマットを拡張（配当内訳に権利落ち日＋支払い月を両方格納）
  - overrides.json による手動補正に対応
  - 日本株/米国株それぞれの支払いパターンに基づく推定

CSVフォーマット（v2）：
  配当内訳カラムの形式：
    "ex:2025-03-28|pay:2025-06:50.0, ex:2025-09-26|pay:2025-12:55.0"
    
    ex: = 配当落ち日（yfinanceから自動取得、日付精度）
    pay: = 支払い月（推定 or 取得、年月精度）
    末尾の数値 = 1株あたり配当金額

  支払い月の推定ルール：
    日本株：配当落ち月 + 3ヶ月（3月落ち → 6月支払い）
    米国株：配当落ち月 + 1ヶ月（2月落ち → 3月支払い）
    ※ overrides.json で個別に上書き可能
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
SLEEP_ON_ERROR = 10
MAX_RETRIES = 2

# 支払い月の推定オフセット（配当落ち月からの月数）
PAYMENT_OFFSET_JP = 3  # 日本株：3ヶ月後
PAYMENT_OFFSET_US = 1  # 米国株：1ヶ月後

# パス設定
REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
JP_CSV = DATA_DIR / "jp_stocks.csv"
US_CSV = DATA_DIR / "us_stocks.csv"
OVERRIDES_FILE = DATA_DIR / "overrides.json"
JP_MASTER = DATA_DIR / "jp_ticker_master.csv"
US_MASTER = DATA_DIR / "us_ticker_master.csv"

# === 銘柄マスタの読み込み ===
#
# 銘柄リストは Python ファイル内に直書きせず、
# CSV ファイル（data/jp_ticker_master.csv, data/us_ticker_master.csv）から読み込む。
#
# 銘柄の追加・削除は CSV を編集するだけ。
# Excel / Google Sheets / テキストエディタ、どれでも編集できる。
#
# CSVフォーマット：
#   日本株：ticker,name,sector  （例：8058,三菱商事,商社）
#   米国株：ticker,sector       （例：AAPL,Technology）
#

def load_jp_master() -> dict[str, tuple[str, str]]:
    """
    日本株マスタCSVを読み込み、辞書に変換する。
    
    戻り値: { "8058.T": ("三菱商事", "商社"), ... }
    
    CSVの ticker 列は「8058」（.T なし）で記載し、
    読み込み時に「.T」を自動付与する。
    """
    result = {}
    if not JP_MASTER.exists():
        print(f"⚠ {JP_MASTER} が見つかりません。空の銘柄リストで続行します。")
        return result
    
    df = pd.read_csv(JP_MASTER, dtype=str)
    for _, row in df.iterrows():
        ticker = row["ticker"].strip()
        name = row["name"].strip()
        sector = row["sector"].strip()
        yf_ticker = f"{ticker}.T"  # yfinance 用に .T を付与
        result[yf_ticker] = (name, sector)
    
    return result


def load_us_master() -> dict[str, str]:
    """
    米国株マスタCSVを読み込み、辞書に変換する。
    
    戻り値: { "AAPL": "Technology", ... }
    
    企業名は yfinance から自動取得するため、CSVには不要。
    """
    result = {}
    if not US_MASTER.exists():
        print(f"⚠ {US_MASTER} が見つかりません。空の銘柄リストで続行します。")
        return result
    
    df = pd.read_csv(US_MASTER, dtype=str)
    for _, row in df.iterrows():
        ticker = row["ticker"].strip()
        sector = row["sector"].strip()
        result[ticker] = sector
    
    return result


# スクリプト起動時に読み込み（グローバル変数として保持）
JP_TICKERS: dict[str, tuple[str, str]] = {}
US_TICKERS: dict[str, str] = {}


def get_jp_name(ticker_symbol: str) -> str:
    """JP_TICKERS から日本語企業名を取得する"""
    entry = JP_TICKERS.get(ticker_symbol)
    if entry and isinstance(entry, tuple):
        return entry[0]
    return ""


def get_jp_sector(ticker_symbol: str) -> str:
    """JP_TICKERS からセクターを取得する"""
    entry = JP_TICKERS.get(ticker_symbol)
    if entry and isinstance(entry, tuple):
        return entry[1]
    return ""


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


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


def estimate_payment_month(ex_date: datetime, market: str) -> str:
    """
    配当落ち日から支払い月を推定する。
    
    Args:
        ex_date: 配当落ち日
        market: "JP" or "US"
    
    Returns:
        "YYYY-MM" 形式の支払い月（推定）
    """
    offset = PAYMENT_OFFSET_JP if market == "JP" else PAYMENT_OFFSET_US
    payment_date = ex_date + relativedelta(months=offset)
    return payment_date.strftime("%Y-%m")


def try_get_next_payment_date(ticker_obj) -> str | None:
    """
    yfinance から「次回の支払い日」を取得する。
    取得できない場合は None を返す。
    
    ticker.info["dividendDate"] は Unix timestamp で返る場合がある。
    """
    try:
        info = ticker_obj.info
        div_date = info.get("dividendDate")
        if div_date and isinstance(div_date, (int, float)):
            dt = datetime.fromtimestamp(div_date)
            return dt.strftime("%Y-%m")
        elif div_date and isinstance(div_date, str):
            return div_date[:7]  # "YYYY-MM-DD" → "YYYY-MM"
    except Exception:
        pass
    return None


def get_dividend_details(ticker_obj, market: str, next_payment: str | None) -> str:
    """
    配当履歴を「ex:落ち日|pay:支払い月:金額」形式で取得する。
    
    支払い月の決定ロジック：
      1. 直近1件は ticker.info["dividendDate"] から実値を使う（取得できれば）
      2. 過去分は推定ルールで算出
      3. overrides.json で上書きされている場合はそちらを優先
    
    出力例：
      "ex:2025-03-28|pay:2025-06:50.0, ex:2025-09-26|pay:2025-12:55.0"
    """
    try:
        dividends = ticker_obj.dividends
        if dividends is None or dividends.empty:
            return ""

        # 直近1年分に絞る
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
            
            # 支払い月の決定
            is_latest = (idx == len(items) - 1)
            if is_latest and next_payment:
                # 直近の配当 → yfinance から取得した実値を使う
                pay_month = next_payment
            else:
                # 過去分 → 推定ルールで算出
                pay_month = estimate_payment_month(date.to_pydatetime(), market)
            
            parts.append(f"ex:{ex_date_str}|pay:{pay_month}:{round(amount, 4)}")

        return ", ".join(parts)

    except Exception as e:
        log(f"    配当詳細取得エラー: {e}")
        return ""


def fetch_single_ticker(ticker_symbol: str, market: str) -> dict | None:
    """1銘柄のデータを取得する"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            if not info or "regularMarketPrice" not in info:
                if attempt < MAX_RETRIES:
                    log(f"  ⚠ {ticker_symbol}: データ不完全, リトライ {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(SLEEP_ON_ERROR)
                    continue
                return None

            dividend_yield = info.get("dividendYield", 0) or 0
            dividend_rate = info.get("dividendRate", 0) or 0
            price = info.get("regularMarketPrice", 0) or 0
            name = info.get("shortName", "") or info.get("longName", "")

            # 次回支払い日の取得を試みる
            next_payment = try_get_next_payment_date(ticker)

            # 配当詳細の取得（支払い月推定込み）
            div_details = get_dividend_details(ticker, market, next_payment)

            return {
                "price": round(price, 2),
                "yield_x100": round(dividend_yield * 10000),
                "annual_div": round(dividend_rate, 4),
                "name": name,
                "div_details": div_details,
            }

        except Exception as e:
            if attempt < MAX_RETRIES:
                log(f"  ⚠ {ticker_symbol}: エラー ({e}), リトライ {attempt + 1}/{MAX_RETRIES}")
                time.sleep(SLEEP_ON_ERROR)
            else:
                log(f"  ✗ {ticker_symbol}: 取得失敗 ({e})")
                return None

    return None


def fetch_batch(ticker_symbols: list[str], market: str) -> list[dict]:
    """
    銘柄リストをバッチ処理で取得する。
    
    日本株：企業名は JP_TICKERS の日本語名を使う（yfinanceの英語名は捨てる）
    米国株：企業名は yfinance の shortName をそのまま使う
    """
    results = []
    total = len(ticker_symbols)

    for i in range(0, total, BATCH_SIZE):
        batch = ticker_symbols[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        log(f"バッチ {batch_num}/{total_batches}: {', '.join(batch)}")

        for symbol in batch:
            data = fetch_single_ticker(symbol, market)
            if data:
                data["ticker"] = symbol

                if market == "JP":
                    # 日本株：辞書から日本語名とセクターを取得
                    data["name"] = get_jp_name(symbol) or data["name"]
                    data["sector"] = get_jp_sector(symbol)
                else:
                    # 米国株：yfinance の名前をそのまま使い、辞書からセクターを取得
                    data["sector"] = US_TICKERS.get(symbol, "")

                results.append(data)
                log(f"  ✓ {symbol} ({data['name']}): {data['price']} / {data['yield_x100'] / 100:.2f}%")
            else:
                log(f"  ✗ {symbol}: スキップ（前回データを維持）")

        if i + BATCH_SIZE < total:
            log(f"  ... {SLEEP_BETWEEN_BATCHES}秒待機 ...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    return results


def apply_overrides(data: list[dict], overrides: dict) -> list[dict]:
    """手動補正データを適用する"""
    for d in data:
        ticker = d["ticker"]
        if ticker in overrides:
            for key, value in overrides[ticker].items():
                old_val = d.get(key, "N/A")
                d[key] = value
                log(f"  📝 {ticker}: {key} を手動値で上書き ({old_val} → {value})")
    return data


def load_existing_csv(path: Path) -> pd.DataFrame | None:
    """既存CSVを読み込む"""
    if path.exists():
        try:
            return pd.read_csv(path, dtype=str)
        except Exception:
            return None
    return None


def merge_with_existing(new_data: list[dict], existing_df: pd.DataFrame | None,
                        ticker_col: str) -> list[dict]:
    """取得失敗銘柄を前回データで補完する"""
    if existing_df is None:
        return new_data

    new_tickers = {d["ticker"] for d in new_data}
    
    # ticker_col が存在するか確認
    if ticker_col not in existing_df.columns:
        return new_data
    
    existing_tickers = set(existing_df[ticker_col].tolist())
    missing = existing_tickers - new_tickers

    if missing:
        log(f"  前回データ維持: {', '.join(sorted(missing))}")
        for _, row in existing_df.iterrows():
            if row[ticker_col] in missing:
                # 既存CSVの列名に合わせて復元
                cols = list(existing_df.columns)
                new_data.append({
                    "ticker": row[ticker_col],
                    "name": row[cols[1]] if len(cols) > 1 else "",
                    "price": float(row[cols[2]]) if len(cols) > 2 else 0,
                    "yield_x100": int(row[cols[3]]) if len(cols) > 3 else 0,
                    "annual_div": float(row[cols[4]]) if len(cols) > 4 else 0,
                    "sector": row[cols[5]] if len(cols) > 5 else "",
                    "div_details": row[cols[6]] if len(cols) > 6 else "",
                })

    return new_data


def write_jp_csv(data: list[dict], path: Path):
    """日本株CSVを書き出す"""
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
    """米国株CSVを書き出す"""
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


def main():
    log("=" * 60)
    log("配当プラス 株価データ自動更新 v2.1")
    log("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 銘柄マスタの読み込み
    global JP_TICKERS, US_TICKERS
    JP_TICKERS = load_jp_master()
    US_TICKERS = load_us_master()
    log(f"銘柄マスタ: 日本株 {len(JP_TICKERS)}銘柄, 米国株 {len(US_TICKERS)}銘柄")

    if not JP_TICKERS and not US_TICKERS:
        log("✗ 銘柄マスタが空です。data/jp_ticker_master.csv, data/us_ticker_master.csv を確認してください。")
        sys.exit(1)

    # 手動補正データの読み込み
    overrides = load_overrides()

    # 既存データの読み込み
    existing_jp = load_existing_csv(JP_CSV)
    existing_us = load_existing_csv(US_CSV)

    # --- 日本株 ---
    log("\n=== 日本株 ===")
    jp_symbols = list(JP_TICKERS.keys())
    jp_data = fetch_batch(jp_symbols, market="JP")
    jp_data = apply_overrides(jp_data, overrides)
    jp_data = merge_with_existing(jp_data, existing_jp, "銘柄コード")
    write_jp_csv(jp_data, JP_CSV)

    log(f"\n... 市場切替待機 {SLEEP_BETWEEN_BATCHES * 2}秒 ...\n")
    time.sleep(SLEEP_BETWEEN_BATCHES * 2)

    # --- 米国株 ---
    log("=== 米国株 ===")
    us_symbols = list(US_TICKERS.keys())
    us_data = fetch_batch(us_symbols, market="US")
    us_data = apply_overrides(us_data, overrides)
    us_data = merge_with_existing(us_data, existing_us, "Ticker")
    write_us_csv(us_data, US_CSV)

    # --- サマリー ---
    log("\n" + "=" * 60)
    log(f"完了: 日本株 {len(jp_data)}銘柄, 米国株 {len(us_data)}銘柄")
    if overrides:
        log(f"手動補正: {len(overrides)}銘柄に適用済み")
    log("=" * 60)


if __name__ == "__main__":
    main()
