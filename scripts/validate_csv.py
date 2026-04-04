"""
配当プラス - CSVデータ検証スクリプト v2.0
配当内訳の新フォーマット（ex:|pay: 形式）にも対応。
"""

import csv
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"

MIN_PRICE_JP = 1
MAX_PRICE_JP = 500000
MIN_PRICE_US = 0.01
MAX_PRICE_US = 50000
MAX_YIELD = 5000
MIN_TICKERS = 5

# 配当内訳の v2 フォーマット検証パターン
# ex:2025-03-28|pay:2025-06:50.0
DIV_DETAIL_PATTERN = re.compile(
    r"ex:\d{4}-\d{2}-\d{2}\|pay:\d{4}-\d{2}:\d+\.?\d*"
)

errors = []
warnings = []


def error(msg: str):
    errors.append(msg)
    print(f"  ✗ {msg}")


def warn(msg: str):
    warnings.append(msg)
    print(f"  ⚠ {msg}")


def validate_div_details(ticker: str, details: str):
    """配当内訳フィールドのフォーマットを検証する"""
    if not details or details.strip() == "":
        return  # 空は許容（無配銘柄）

    # v2形式（ex:|pay:）か v1形式（日付:金額）か判定
    if "ex:" in details:
        # v2形式の検証
        entries = [e.strip() for e in details.split(",")]
        for entry in entries:
            if not DIV_DETAIL_PATTERN.match(entry):
                warn(f"{ticker}: 配当内訳のフォーマット不正 → {entry}")
    # v1形式は後方互換のため警告のみ


def validate_csv(path: Path, expected_header: list[str],
                 min_price: float, max_price: float,
                 ticker_col: int, price_col: int, yield_col: int,
                 div_col: int):
    """1つのCSVファイルを検証する"""
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
        error(f"{path.name}: ヘッダーが不正 (期待: {expected_header}, 実際: {header})")
        return

    data_rows = rows[1:]
    expected_cols = len(expected_header)

    print(f"  銘柄数: {len(data_rows)}")

    if len(data_rows) < MIN_TICKERS:
        error(f"{path.name}: 銘柄数が{MIN_TICKERS}未満 ({len(data_rows)}銘柄)")

    empty_prices = 0
    abnormal_yields = 0

    for i, row in enumerate(data_rows, start=2):
        if len(row) < expected_cols:
            warn(f"行{i}: カラム数不足 (期待: {expected_cols}, 実際: {len(row)})")
            continue

        ticker = row[ticker_col].strip()
        if not ticker:
            warn(f"行{i}: 銘柄コードが空")
            continue

        # 株価チェック
        try:
            price = float(row[price_col])
            if price < min_price:
                warn(f"{ticker}: 株価が異常に低い ({price})")
                empty_prices += 1
            elif price > max_price:
                warn(f"{ticker}: 株価が異常に高い ({price})")
        except (ValueError, IndexError):
            warn(f"{ticker}: 株価が数値でない")
            empty_prices += 1

        # 利回りチェック
        try:
            yield_val = int(row[yield_col])
            if yield_val < 0:
                warn(f"{ticker}: 利回りが負 ({yield_val})")
                abnormal_yields += 1
            elif yield_val > MAX_YIELD:
                warn(f"{ticker}: 利回りが異常に高い ({yield_val / 100:.2f}%)")
                abnormal_yields += 1
        except (ValueError, IndexError):
            pass

        # 配当内訳フォーマットチェック
        if len(row) > div_col:
            validate_div_details(ticker, row[div_col])

    if empty_prices > len(data_rows) * 0.3:
        error(f"{path.name}: 30%以上の銘柄で株価が異常 ({empty_prices}/{len(data_rows)})")

    if abnormal_yields > len(data_rows) * 0.2:
        error(f"{path.name}: 20%以上の銘柄で利回りが異常 ({abnormal_yields}/{len(data_rows)})")

    ok_count = len(data_rows) - empty_prices - abnormal_yields
    print(f"  正常: {ok_count}/{len(data_rows)}銘柄")


def main():
    print("=" * 50)
    print("CSVデータ検証 v2.0")
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
        print(f"⚠ 検証通過（{len(warnings)}件の警告あり）")
    else:
        print("✓ 検証通過（問題なし）")
    print("=" * 50)


if __name__ == "__main__":
    main()
