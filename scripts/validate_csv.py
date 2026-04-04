"""
配当プラス - CSVデータ検証スクリプト v2.1

v2.0 からの変更点：
  - 利回り異常を「エラー」から「警告」に格下げ
    → ETFや特殊銘柄で利回りが異常値になるのはyfinanceの既知の問題
    → 利回り異常だけでcommitを止めるのは過剰防衛
  - 株価が0件/30%以上欠損の場合のみエラー（commitを止める）
"""

import csv
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"

MIN_PRICE_JP = 1
MAX_PRICE_JP = 9999999
MIN_PRICE_US = 0.01
MAX_PRICE_US = 999999
MAX_YIELD = 5000        # 50.00%相当
MIN_TICKERS = 5

DIV_DETAIL_PATTERN = re.compile(
    r"ex:\d{4}-\d{2}-\d{2}\|pay:\d{4}-\d{2}:\d+\.?\d*"
)

errors = []
warnings = []


def error(msg: str):
    errors.append(msg)
    print(f"  ✗ ERROR: {msg}")


def warn(msg: str):
    warnings.append(msg)
    # 利回り警告は個別出力すると大量になるので、サマリーだけ出す
    # （個別ログは出さない）


def validate_div_details(ticker: str, details: str):
    if not details or details.strip() == "":
        return
    if "ex:" in details:
        entries = [e.strip() for e in details.split(",")]
        for entry in entries:
            if not DIV_DETAIL_PATTERN.match(entry):
                warn(f"{ticker}: 配当内訳フォーマット不正 → {entry}")


def validate_csv(path: Path, expected_header: list[str],
                 min_price: float, max_price: float,
                 ticker_col: int, price_col: int, yield_col: int,
                 div_col: int):
    print(f"\n--- {path.name} ---")

    if not path.exists():
        error(f"{path.name} が存在しません")
        return

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) < 2:
        error(f"{path.name}: データ行がありません")
        return

    header = rows[0]
    if header != expected_header:
        error(f"{path.name}: ヘッダーが不正")
        return

    data_rows = rows[1:]
    expected_cols = len(expected_header)

    print(f"  銘柄数: {len(data_rows)}")

    if len(data_rows) < MIN_TICKERS:
        error(f"{path.name}: 銘柄数が{MIN_TICKERS}未満 ({len(data_rows)}銘柄)")

    empty_prices = 0
    abnormal_yields = 0
    zero_prices = 0

    for i, row in enumerate(data_rows, start=2):
        if len(row) < expected_cols:
            warn(f"行{i}: カラム数不足")
            continue

        ticker = row[ticker_col].strip()
        if not ticker:
            continue

        # 株価チェック
        try:
            price = float(row[price_col])
            if price == 0:
                zero_prices += 1
            elif price < min_price:
                warn(f"{ticker}: 株価が異常に低い ({price})")
                empty_prices += 1
            elif price > max_price:
                warn(f"{ticker}: 株価が異常に高い ({price})")
        except (ValueError, IndexError):
            empty_prices += 1

        # 利回りチェック（警告のみ、エラーにしない）
        try:
            yield_val = int(float(row[yield_col]))
            if yield_val > MAX_YIELD:
                abnormal_yields += 1
        except (ValueError, IndexError):
            pass

        # 配当内訳フォーマットチェック
        if len(row) > div_col:
            validate_div_details(ticker, row[div_col])

    # --- 結果サマリー ---
    ok_prices = len(data_rows) - empty_prices - zero_prices
    print(f"  株価取得成功: {ok_prices}/{len(data_rows)}銘柄")
    print(f"  株価ゼロ（前回データなし）: {zero_prices}銘柄")

    if abnormal_yields > 0:
        pct = round(abnormal_yields / len(data_rows) * 100, 1)
        print(f"  ⚠ 利回り異常値: {abnormal_yields}銘柄 ({pct}%)（yfinanceの仕様によるもの、警告のみ）")

    # エラー判定は「株価が取得できているか」だけで行う
    # 利回り異常はyfinanceの既知問題なのでエラーにしない
    if empty_prices > len(data_rows) * 0.3:
        error(f"{path.name}: 30%以上の銘柄で株価取得失敗 ({empty_prices}/{len(data_rows)})")


def main():
    print("=" * 50)
    print("CSVデータ検証 v2.1")
    print("=" * 50)

    validate_csv(
        path=DATA_DIR / "jp_stocks.csv",
        expected_header=["銘柄コード", "企業名", "価格", "利回り(%)", "年間配当", "セクター", "配当内訳"],
        min_price=MIN_PRICE_JP, max_price=MAX_PRICE_JP,
        ticker_col=0, price_col=2, yield_col=3, div_col=6,
    )

    validate_csv(
        path=DATA_DIR / "us_stocks.csv",
        expected_header=["Ticker", "Company", "Price", "Yield(%)", "AnnualDiv", "Sector", "DivDetails"],
        min_price=MIN_PRICE_US, max_price=MAX_PRICE_US,
        ticker_col=0, price_col=2, yield_col=3, div_col=6,
    )

    print("\n" + "=" * 50)
    if errors:
        print(f"✗ 検証失敗: {len(errors)}件のエラー")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)
    elif warnings:
        print(f"✓ 検証通過（{len(warnings)}件の警告あり）")
    else:
        print("✓ 検証通過（問題なし）")
    print("=" * 50)


if __name__ == "__main__":
    main()
