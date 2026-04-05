"""
配当プラス - 株価データ自動更新スクリプト v3.4

v3.3 からの変更点：
  - 利回りを yfinance の dividendYield に頼らず、自力計算に変更
    （年間配当額 ÷ 株価 × 10000 で正確に算出）
    → yfinance が100倍ズレた値を返す既知問題を根本解決
  - 企業名が数値のみの場合、ティッカーシンボルで代替
    → yfinance が一部銘柄で数値を返す問題の対応

設計（v3.2 から継続）：
  ① ThreadPoolExecutor(max_workers=3) で3並列取得
  ② random.uniform(0.8, 1.5) のランダムスリープ（bot検知回避）
  ③ バッチごとにCSV保存 → 中断しても再開可能（レジューム機能）
"""

import yfinance as yf
import pandas as pd
import json
import time
import random
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# === 設定 ===
MAX_WORKERS = 3
BATCH_SIZE = 50
BASE_SLEEP_MIN = 0.8
BASE_SLEEP_MAX = 1.5
BATCH_SLEEP = 2
RATE_LIMIT_SLEEP = 180

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
JP_PROGRESS = DATA_DIR / "_jp_progress.csv"
US_PROGRESS = DATA_DIR / "_us_progress.csv"
METADATA_FILE = DATA_DIR / "metadata.json"

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

def estimate_payment_month(ex_date, market: str) -> str:
    offset = PAYMENT_OFFSET_JP if market == "JP" else PAYMENT_OFFSET_US
    if hasattr(ex_date, 'to_pydatetime'):
        ex_date = ex_date.to_pydatetime()
    if ex_date.tzinfo is not None:
        ex_date = ex_date.replace(tzinfo=None)
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
                pay_month = estimate_payment_month(date, market)
            parts.append(f"ex:{ex_date_str}|pay:{pay_month}:{round(amount, 4)}")

        return ", ".join(parts)
    except Exception:
        return ""


# === 1銘柄の取得 ===

def process_ticker(ticker: str, market: str, one_year_ago) -> dict | str | None:
    try:
        time.sleep(random.uniform(BASE_SLEEP_MIN, BASE_SLEEP_MAX))

        stock = yf.Ticker(ticker)
        info = stock.info

        if not info or "regularMarketPrice" not in info:
            return None

        price = info.get("regularMarketPrice") or info.get("currentPrice", 0) or 0

        # === 年間配当額の取得（2段階フォールバック） ===
        #
        # 優先1: info["dividendRate"]（通常株で取得可能）
        # 優先2: ticker.dividends から過去1年の合計を自力計算
        #        → ETF（JEPI, SCHD, VYM 等）では dividendRate が
        #          MISSING になるため、こちらで補完する
        #
        dividend_rate = info.get("dividendRate", 0) or 0
        div_details = ""
        annual_div_from_history = 0

        # 配当履歴は常にチェックする（dividendRate の有無に関わらず）
        try:
            dividends = stock.dividends
            if dividends is not None and not dividends.empty:
                one_year_ts = pd.Timestamp.now(tz="UTC") - pd.DateOffset(years=1)
                if dividends.index.tz is None:
                    dividends.index = dividends.index.tz_localize("UTC")
                recent = dividends[dividends.index >= one_year_ts]
                if not recent.empty:
                    annual_div_from_history = round(float(recent.sum()), 4)
                    div_details = get_dividend_details(stock, market)
        except Exception:
            pass

        # dividendRate が取れなかった場合、履歴から補完
        if dividend_rate == 0 and annual_div_from_history > 0:
            dividend_rate = annual_div_from_history

        # 利回りは自力計算（年間配当額 ÷ 株価）
        if price > 0 and dividend_rate > 0:
            yield_x100 = round(dividend_rate / float(price) * 10000)
        else:
            yield_x100 = 0

        # 企業名の検証（数値のみの場合はティッカーで代替）
        yf_name = info.get("shortName") or info.get("longName") or ""
        if not yf_name or yf_name.replace(".", "").replace("-", "").isdigit():
            yf_name = ticker.replace(".T", "")

        return {
            "ticker": ticker,
            "price": round(float(price), 2),
            "yield_x100": yield_x100,
            "annual_div": round(float(dividend_rate), 4),
            "yf_name": yf_name,
            "div_details": div_details,
        }

    except Exception as e:
        if "429" in str(e) or "Too Many Requests" in str(e) or "Rate" in str(e):
            return "BLOCK"
        return None


# === 市場ごとの取得（レジューム対応） ===

def fetch_market(symbols: list[str], market: str, progress_file: Path) -> list[dict]:
    global rate_limit_count
    one_year_ago = datetime.now() - timedelta(days=365)

    # レジューム
    results = []
    done_tickers = set()
    if progress_file.exists():
        try:
            df_progress = pd.read_csv(progress_file, dtype=str).fillna("")
            results = df_progress.to_dict("records")
            done_tickers = {r["ticker"] for r in results}
            for r in results:
                r["price"] = float(r.get("price", 0) or 0)
                r["yield_x100"] = int(float(r.get("yield_x100", 0) or 0))
                r["annual_div"] = float(r.get("annual_div", 0) or 0)
            log(f"  レジューム: {len(done_tickers)}銘柄取得済み → 続きから開始")
        except Exception:
            results, done_tickers = [], set()

    remaining = [s for s in symbols if s not in done_tickers]
    total_all = len(symbols)
    total_remaining = len(remaining)

    if total_remaining == 0:
        log(f"  全{total_all}銘柄取得済み（レジュームで完了）")
        return results

    log(f"  取得対象: {total_remaining}銘柄（全{total_all}銘柄中）")
    start_time = time.time()
    batch_success = 0
    batch_skip = 0

    for i in range(0, total_remaining, BATCH_SIZE):
        batch = remaining[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total_remaining + BATCH_SIZE - 1) // BATCH_SIZE

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {
                executor.submit(process_ticker, t, market, one_year_ago): t
                for t in batch
            }
            for future in as_completed(future_to_ticker):
                ticker_sym = future_to_ticker[future]
                try:
                    res = future.result()
                    if res == "BLOCK":
                        rate_limit_count += 1
                        log(f"  🚫 レート制限(429)検知（{rate_limit_count}回目）→ {RATE_LIMIT_SLEEP}秒停止...")
                        time.sleep(RATE_LIMIT_SLEEP)
                        results.append({
                            "ticker": ticker_sym, "price": 0, "yield_x100": 0,
                            "annual_div": 0, "yf_name": "", "div_details": "",
                        })
                        batch_skip += 1
                    elif res:
                        results.append(res)
                        batch_success += 1
                    else:
                        results.append({
                            "ticker": ticker_sym, "price": 0, "yield_x100": 0,
                            "annual_div": 0, "yf_name": "", "div_details": "",
                        })
                        batch_skip += 1
                except Exception:
                    results.append({
                        "ticker": ticker_sym, "price": 0, "yield_x100": 0,
                        "annual_div": 0, "yf_name": "", "div_details": "",
                    })
                    batch_skip += 1

        # 中間保存
        pd.DataFrame(results).to_csv(progress_file, index=False, encoding="utf-8")

        fetched_so_far = len(done_tickers) + i + len(batch)
        elapsed = time.time() - start_time
        rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
        remaining_time = (total_remaining - i - len(batch)) / rate if rate > 0 else 0

        log(f"  📊 バッチ {batch_num}/{total_batches} 完了 | "
            f"全体: {fetched_so_far}/{total_all} | "
            f"成功:{batch_success} スキップ:{batch_skip} | "
            f"残り約{int(remaining_time / 60)}分")

        if i + BATCH_SIZE < total_remaining:
            time.sleep(BATCH_SLEEP)

    elapsed_total = time.time() - start_time
    total_success = len(done_tickers) + batch_success
    log(f"  ✓ 完了: {total_success}/{total_all}銘柄取得 "
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
                d["price"] = float(row[cols[2]]) if len(cols) > 2 and row[cols[2]] else 0
                if d["yield_x100"] == 0 and len(cols) > 3 and row[cols[3]]:
                    d["yield_x100"] = int(float(row[cols[3]]))
                if d["annual_div"] == 0 and len(cols) > 4 and row[cols[4]]:
                    d["annual_div"] = float(row[cols[4]])
                if not d.get("div_details") and len(cols) > 6:
                    d["div_details"] = str(row[cols[6]]) if pd.notna(row[cols[6]]) else ""
            except (ValueError, IndexError):
                pass

    return new_data


def csv_quote(value: str) -> str:
    """カンマを含むフィールドをダブルクォートで囲む（RFC 4180準拠）"""
    s = str(value)
    if "," in s or '"' in s:
        return '"' + s.replace('"', '""') + '"'
    return s


def write_jp_csv(data: list[dict], path: Path):
    rows = []
    for d in sorted(data, key=lambda x: x["ticker"]):
        code = d["ticker"].replace(".T", "")
        name = csv_quote(d["name"])
        sector = csv_quote(d["sector"])
        details = csv_quote(d.get("div_details", ""))
        rows.append(
            f'{code},{name},{d["price"]},{d["yield_x100"]},'
            f'{d["annual_div"]},{sector},{details}'
        )
    header = "銘柄コード,企業名,価格,利回り(%),年間配当,セクター,配当内訳"
    content = header + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    log(f"✓ {path.name}: {len(rows)}銘柄を書き出し")


def write_us_csv(data: list[dict], path: Path):
    rows = []
    for d in sorted(data, key=lambda x: x["ticker"]):
        name = csv_quote(d["name"])
        sector = csv_quote(d["sector"])
        details = csv_quote(d.get("div_details", ""))
        rows.append(
            f'{d["ticker"]},{name},{d["price"]},{d["yield_x100"]},'
            f'{d["annual_div"]},{sector},{details}'
        )
    header = "Ticker,Company,Price,Yield(%),AnnualDiv,Sector,DivDetails"
    content = header + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    log(f"✓ {path.name}: {len(rows)}銘柄を書き出し")


def write_metadata(jp_count: int, us_count: int):
    """
    metadata.json を自動生成する。
    アプリはこのファイルを参照して、データ更新の有無を判断する。

    従来は手動更新だったが、GitHub Actions での自動生成に移行。
    バージョンは更新日時ベースの自動採番（YYYY.MM.DD 形式）。
    """
    now = datetime.now()
    # バージョン: 年.月.日 形式（例: 2026.04.06）
    version = now.strftime("%Y.%-m.%-d")
    # ISO 8601 形式（日本時間）
    last_updated = now.strftime("%Y-%m-%dT%H:%M:%S+09:00")

    metadata = {
        "lastUpdated": last_updated,
        "version": version,
        "jpStocksCount": jp_count,
        "usStocksCount": us_count,
    }

    METADATA_FILE.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    log(f"✓ metadata.json: version={version}, JP={jp_count}, US={us_count}")


def cleanup_progress():
    for f in [JP_PROGRESS, US_PROGRESS]:
        if f.exists():
            f.unlink()
            log(f"  中間ファイル削除: {f.name}")


# === メイン ===

def main():
    log("=" * 60)
    log("配当プラス 株価データ自動更新 v3.3")
    log("（3並列 + ランダムスリープ + レジューム + 利回り正規化）")
    log("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    global JP_TICKERS, US_TICKERS
    JP_TICKERS = load_jp_master()
    US_TICKERS = load_us_master()
    total = len(JP_TICKERS) + len(US_TICKERS)
    log(f"銘柄マスタ: 日本株 {len(JP_TICKERS)} + 米国株 {len(US_TICKERS)} = {total}銘柄")

    estimated_minutes = int(total / 3 * 1.2 / 60)
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
    jp_raw = fetch_market(list(JP_TICKERS.keys()), market="JP", progress_file=JP_PROGRESS)
    jp_data = build_final_data(jp_raw, market="JP")
    jp_data = apply_overrides(jp_data, overrides)
    jp_data = merge_with_existing(jp_data, existing_jp, "銘柄コード")
    write_jp_csv(jp_data, JP_CSV)

    log(f"\n... 市場切替: 30秒待機 ...\n")
    time.sleep(30)

    # --- 米国株 ---
    log("=" * 60)
    log(f"=== 米国株 ({len(US_TICKERS)}銘柄) ===")
    log("=" * 60)
    us_raw = fetch_market(list(US_TICKERS.keys()), market="US", progress_file=US_PROGRESS)
    us_data = build_final_data(us_raw, market="US")
    us_data = apply_overrides(us_data, overrides)
    us_data = merge_with_existing(us_data, existing_us, "Ticker")
    write_us_csv(us_data, US_CSV)

    cleanup_progress()

    # --- metadata.json の自動生成 ---
    write_metadata(len(jp_data), len(us_data))

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
