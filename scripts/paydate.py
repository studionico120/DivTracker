"""
配当プラス - 支払い月の推定ロジック（yfinance に依存しない純粋関数だけ）

背景:
  Yahoo Finance は、過去の配当の「支払日」を提供しない。提供するのは、次回（または直近）の配当の
  権利落ち日 exDividendDate と支払日 dividendDate の組だけで、しかも米国の ETF と日本株には
  この組がほとんどない。

  従来は dividendDate を「履歴の直近の権利落ち日」に当てていたが、dividendDate は多くの場合
  次回の配当の支払日で、過去の権利落ち日とは対応しない（例: MSFT は 8 月の権利落ちに 12 月の
  支払いが付いた）。古い日付が残る銘柄もあり、支払いが権利落ちより前になる異常値が 514 銘柄あった。
  それ以外は、権利落ち日に固定の月数（日本株 +3 か月、米国株 +1 か月）を足す推定で、
  2026-09-30 の標本監査（doc: アプリ側リポジトリ doc/audit）では、米国株の支払い月の一致は 10%
  だった。

方針:
  1. exDividendDate と dividendDate の両方があり、遅れが 0〜120 日なら、その組から
     「権利落ち → 支払い」の日数を求め、履歴の各権利落ち日に足して支払い月にする。
     履歴の権利落ち日と一致するものは、実際の支払日の月をそのまま使う。
  2. 組がない銘柄は、市場・種別ごとの既定値を使う（下の定数。標本監査に基づく）。

月の判定は、日数を足した日付の年月で行う（"YYYY-MM"）。
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from dateutil.relativedelta import relativedelta

# Yahoo の (exDividendDate, dividendDate) の組を採用する遅れ日数の上限。
# 範囲外は、古い日付（例: 2016 年）や、別の配当の日付とみなして捨てる。
MAX_PAIR_LAG_DAYS = 120

# 履歴の権利落ち日と exDividendDate を同一とみなす許容日数（最も近い 1 件だけに適用する）
PAIR_MATCH_TOLERANCE_DAYS = 3

# --- 既定値（Yahoo に支払日がない銘柄）。2026-09-30 の標本監査に基づく -------------------
# 米国の個別株: 権利落ちの 8〜21 日後に払う銘柄が多い（標本 4 銘柄の中央値 15.5 日）
US_STOCK_LAG_DAYS = 14
# 米国の ETF: 権利落ちの 1〜8 日後に払う（標本 9 銘柄。+0〜1 日が月の一致 93% で最良）
US_ETF_LAG_DAYS = 1
# 日本の ETF: 決算日の 37〜39 日後に払う（標本 10 銘柄・20 件。+37 日で月の一致 95%、+3 か月は 0%）
JP_ETF_LAG_DAYS = 37
# 日本の個別株・J-REIT: 権利落ち月の 3 か月後（標本 13 銘柄中 12 銘柄で全件一致）
JP_STOCK_OFFSET_MONTHS = 3

# Yahoo の quoteType のうち、ETF として扱うもの
ETF_QUOTE_TYPES = frozenset({"ETF", "MUTUALFUND"})


@dataclass(frozen=True)
class PayPair:
    """Yahoo が返す、同じ配当の権利落ち日と支払日"""
    ex: date
    pay: date

    @property
    def lag_days(self) -> int:
        return (self.pay - self.ex).days


def epoch_to_date(value) -> date | None:
    """Yahoo のエポック秒を UTC の日付にする。数値でない・不正な値は None"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if value <= 0:
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).date()
    except (OverflowError, OSError, ValueError):
        return None


def yahoo_pair(info: dict | None) -> PayPair | None:
    """info の exDividendDate と dividendDate が、同じ配当の組として妥当なら PayPair を返す。

    - どちらかが欠けている → None（dividendDate だけでは、どの配当の支払日か分からない）
    - 支払日が権利落ち日より前、または遅れが MAX_PAIR_LAG_DAYS を超える → None
    """
    if not info:
        return None
    ex = epoch_to_date(info.get("exDividendDate"))
    pay = epoch_to_date(info.get("dividendDate"))
    if ex is None or pay is None:
        return None
    lag = (pay - ex).days
    if lag < 0 or lag > MAX_PAIR_LAG_DAYS:
        return None
    return PayPair(ex=ex, pay=pay)


def is_etf_like(quote_type: str | None) -> bool:
    return (quote_type or "").upper() in ETF_QUOTE_TYPES


def default_pay_month(ex: date, market: str, quote_type: str | None) -> str:
    """Yahoo に支払日がない銘柄の、支払い月（"YYYY-MM"）"""
    etf = is_etf_like(quote_type)
    if market == "JP":
        if etf:
            pay = ex + timedelta(days=JP_ETF_LAG_DAYS)
        else:
            pay = ex + relativedelta(months=JP_STOCK_OFFSET_MONTHS)
    else:
        pay = ex + timedelta(days=US_ETF_LAG_DAYS if etf else US_STOCK_LAG_DAYS)
    return pay.strftime("%Y-%m")


def estimate_pay_months(ex_dates: list[date], market: str, info: dict | None) -> tuple[list[str], str]:
    """権利落ち日ごとの支払い月（"YYYY-MM"）と、推定の根拠を返す。

    根拠:
      "yahoo_exact" … Yahoo の組の権利落ち日が履歴と一致し、その支払日を使った（他は遅れを補正）
      "yahoo_lag"   … Yahoo の組から求めた遅れを、全件に当てはめた（組の権利落ち日は履歴にない）
      "default"     … 組がなく、市場・種別ごとの既定値を使った
    """
    info = info or {}
    quote_type = info.get("quoteType")
    pair = yahoo_pair(info)
    if pair is None:
        return [default_pay_month(d, market, quote_type) for d in ex_dates], "default"

    # 組の権利落ち日に最も近い履歴の 1 件だけを「実際の支払日」で確定する
    exact_index = None
    if ex_dates:
        nearest = min(range(len(ex_dates)), key=lambda i: abs((ex_dates[i] - pair.ex).days))
        if abs((ex_dates[nearest] - pair.ex).days) <= PAIR_MATCH_TOLERANCE_DAYS:
            exact_index = nearest

    months = []
    for i, d in enumerate(ex_dates):
        if i == exact_index:
            months.append(pair.pay.strftime("%Y-%m"))
        else:
            months.append((d + timedelta(days=pair.lag_days)).strftime("%Y-%m"))
    return months, ("yahoo_exact" if exact_index is not None else "yahoo_lag")


# 配当内訳の 1 件: ex:YYYY-MM-DD|pay:YYYY-MM:金額
_ENTRY = re.compile(r"^ex:(\d{4})-(\d{2})-(\d{2})\|pay:(\d{4})-(\d{2}):(.+)$")
# 支払い月が権利落ち月から何か月後まで妥当とみなすか（validate_csv.py の MAX_PAY_MONTH_GAP と揃える）
MAX_PAY_MONTH_GAP = 5


def sanitize_details(details: str, market: str) -> str:
    """前回の CSV から引き継ぐ配当内訳の、支払い月の異常値を既定値で置き換える。

    取得に失敗した銘柄（上場廃止など）は、前回の CSV の値を引き継ぐ。前回の値に古い支払い月
    （例: 2016-12）が残っていると、検証が毎週失敗して更新が止まるため、異常な 1 件だけを補正する。
    形式が不明な内訳は、そのまま返す。
    """
    if not details or "ex:" not in details:
        return details
    fixed = []
    for entry in details.split(","):
        e = entry.strip()
        m = _ENTRY.match(e)
        if not m:
            fixed.append(e)
            continue
        ey, em, ed, py, pm, amount = m.groups()
        gap = (int(py) * 12 + int(pm)) - (int(ey) * 12 + int(em))
        if 0 <= gap <= MAX_PAY_MONTH_GAP:
            fixed.append(e)
            continue
        try:
            pay = default_pay_month(date(int(ey), int(em), int(ed)), market, None)
        except ValueError:
            fixed.append(e)
            continue
        fixed.append(f"ex:{ey}-{em}-{ed}|pay:{pay}:{amount}")
    return ", ".join(fixed)
