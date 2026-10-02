"""
配当プラス - CSVデータ検証スクリプト v2.2

v2.1 からの変更点：
  - 支払い月の整合性を検査（エラー）: 支払い月が権利落ち月より前、または 6 か月以上後の
    配当内訳が 1 件でもあれば止める（2026-09 時点で 514 銘柄にあった異常値の再発防止）
  - 前回のデータ（git の HEAD の CSV）との比較を追加（エラー）: 銘柄数、または配当のある銘柄数が
    前回より 10% 以上減っていたら止める。Yahoo が突然配当を返さなくなった場合に、
    株価だけ取れていて検証を通り、配当なしのデータで公開されるのを防ぐ
    ・LIMIT_TICKERS（動作確認用）が設定されているとき、または SKIP_REGRESSION_GUARD=1 のときは省略
  - 配当内訳のフォーマット不正は、従来どおり警告のみ

v2.0 からの変更点：
  - 利回り異常を「エラー」から「警告」に格下げ
    → ETFや特殊銘柄で利回りが異常値になるのはyfinanceの既知の問題
    → 利回り異常だけでcommitを止めるのは過剰防衛
  - 株価が0件/30%以上欠損の場合のみエラー（commitを止める）
"""

import csv
import io
import os
import re
import subprocess
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

# 支払い月が権利落ち月から何か月後まで許すか（0〜5 か月）。推定の上限は遅れ 120 日 ≒ 4 か月
MAX_PAY_MONTH_GAP = 5
# 前回比で、この割合を下回ったらエラー
MIN_RATIO_VS_PREVIOUS = 0.9

PAY_MONTH_ENTRY = re.compile(r"ex:(\d{4})-(\d{2})-\d{2}\|pay:(\d{4})-(\d{2}):")

errors = []
warnings = []


def find_pay_month_violations(details: str) -> list[str]:
    """支払い月が権利落ち月より前、または MAX_PAY_MONTH_GAP か月を超えて後の配当内訳を返す"""
    bad = []
    for entry in (details or "").split(","):
        m = PAY_MONTH_ENTRY.search(entry)
        if not m:
            continue
        ey, em, py, pm = (int(x) for x in m.groups())
        gap = (py * 12 + pm) - (ey * 12 + em)
        if gap < 0 or gap > MAX_PAY_MONTH_GAP:
            bad.append(entry.strip())
    return bad


def regression_messages(new_rows: list[list[str]], prev_rows: list[list[str]],
                        div_col: int, name: str, min_ratio: float = MIN_RATIO_VS_PREVIOUS) -> list[str]:
    """前回のデータと比べて、銘柄数・配当のある銘柄数が大きく減っていれば、エラーメッセージを返す"""
    def count(rows):
        data = rows[1:]
        with_div = sum(1 for r in data if len(r) > div_col and r[div_col].strip())
        return len(data), with_div

    n_new, d_new = count(new_rows)
    n_prev, d_prev = count(prev_rows)
    msgs = []
    if n_prev and n_new < n_prev * min_ratio:
        msgs.append(f"{name}: 銘柄数が前回より {100 - n_new * 100 // n_prev}% 以上減少 ({n_prev} → {n_new})")
    if d_prev and d_new < d_prev * min_ratio:
        msgs.append(f"{name}: 配当のある銘柄数が前回より {100 - d_new * 100 // d_prev}% 以上減少 ({d_prev} → {d_new})")
    return msgs


def load_previous_rows(path: Path) -> list[list[str]] | None:
    """git の HEAD にある前回の CSV を読む。取得できなければ None（初回など）"""
    try:
        rel = path.relative_to(REPO_ROOT).as_posix()
        out = subprocess.run(["git", "show", f"HEAD:{rel}"], cwd=REPO_ROOT, capture_output=True,
                             text=True, encoding="utf-8", check=True).stdout
        return list(csv.reader(io.StringIO(out)))
    except Exception:
        return None


def regression_guard_enabled() -> bool:
    if os.environ.get("SKIP_REGRESSION_GUARD") == "1":
        return False
    return int(os.environ.get("LIMIT_TICKERS", "0") or "0") <= 0


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
    pay_month_violations = []   # (ティッカー, 配当内訳の 1 件)

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
            for entry in find_pay_month_violations(row[div_col]):
                pay_month_violations.append((ticker, entry))

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

    # 支払い月の整合性（権利落ち月より前、または離れすぎ）
    if pay_month_violations:
        examples = "、".join(f"{t}: {e}" for t, e in pay_month_violations[:5])
        error(f"{path.name}: 支払い月が権利落ち月より前、または {MAX_PAY_MONTH_GAP} か月を超えて後の配当内訳 "
              f"{len(pay_month_violations)}件（例: {examples}）")
    else:
        print("  支払い月の整合性: 問題なし")

    # 前回のデータとの比較
    if regression_guard_enabled():
        prev_rows = load_previous_rows(path)
        if prev_rows:
            msgs = regression_messages(rows, prev_rows, div_col, path.name)
            for m in msgs:
                error(m)
            if not msgs:
                print("  前回データとの比較: 問題なし")
        else:
            print("  前回データとの比較: 前回のデータがないため省略")
    else:
        print("  前回データとの比較: 動作確認モードのため省略")


def main():
    print("=" * 50)
    print("CSVデータ検証 v2.2")
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
