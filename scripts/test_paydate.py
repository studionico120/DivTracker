"""paydate.py の単体テスト（標準ライブラリの unittest。`python -m unittest discover -s scripts` で実行）"""

import random
import unittest
from datetime import date, datetime, timezone

import paydate
from paydate import estimate_pay_months, yahoo_pair, epoch_to_date, default_pay_month, sanitize_details


def epoch(y, m, d):
    """UTC 0 時のエポック秒（Yahoo の exDividendDate / dividendDate と同じ形式）"""
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp())


class EpochToDateTest(unittest.TestCase):
    def test_utc_date(self):
        self.assertEqual(epoch_to_date(epoch(2026, 11, 19)), date(2026, 11, 19))

    def test_invalid_values(self):
        for v in (None, "2026-11-19", float("nan"), float("inf"), 0, -5, True, [], {}):
            self.assertIsNone(epoch_to_date(v), v)


class YahooPairTest(unittest.TestCase):
    def test_valid_pair(self):
        p = yahoo_pair({"exDividendDate": epoch(2026, 11, 19), "dividendDate": epoch(2026, 12, 10)})
        self.assertEqual((p.ex, p.pay, p.lag_days), (date(2026, 11, 19), date(2026, 12, 10), 21))

    def test_requires_both_fields(self):
        # OXLCI: 支払日だけ。どの配当の支払日か分からないので使わない
        self.assertIsNone(yahoo_pair({"dividendDate": epoch(2026, 12, 31)}))
        self.assertIsNone(yahoo_pair({"exDividendDate": epoch(2026, 11, 19)}))
        self.assertIsNone(yahoo_pair({}))
        self.assertIsNone(yahoo_pair(None))

    def test_rejects_stale_or_reversed(self):
        # AAXJ: 支払日が 2016 年（古い日付）
        self.assertIsNone(yahoo_pair({"exDividendDate": epoch(2026, 6, 15), "dividendDate": epoch(2016, 12, 28)}))
        # 支払日が権利落ち日より前
        self.assertIsNone(yahoo_pair({"exDividendDate": epoch(2026, 6, 15), "dividendDate": epoch(2026, 6, 14)}))

    def test_lag_boundaries(self):
        ex = epoch(2026, 1, 1)
        self.assertIsNotNone(yahoo_pair({"exDividendDate": ex, "dividendDate": ex}))                  # 0 日
        self.assertIsNotNone(yahoo_pair({"exDividendDate": ex, "dividendDate": ex + 120 * 86400}))    # 120 日
        self.assertIsNone(yahoo_pair({"exDividendDate": ex, "dividendDate": ex + 121 * 86400}))       # 121 日


class DefaultPayMonthTest(unittest.TestCase):
    def test_us_etf_pays_next_day(self):
        # OSCV: 12/30 権利落ち → 12/31 支払い。従来の +1 か月は 1 月で外れていた
        self.assertEqual(default_pay_month(date(2025, 12, 30), "US", "ETF"), "2025-12")
        self.assertEqual(default_pay_month(date(2026, 3, 30), "US", "ETF"), "2026-03")
        self.assertEqual(default_pay_month(date(2025, 10, 1), "US", "ETF"), "2025-10")

    def test_us_stock_default(self):
        # 個別株の既定は +14 日（10 日 → 同じ月、月末 → 翌月）
        self.assertEqual(default_pay_month(date(2026, 3, 10), "US", "EQUITY"), "2026-03")
        self.assertEqual(default_pay_month(date(2026, 3, 31), "US", "EQUITY"), "2026-04")
        self.assertEqual(default_pay_month(date(2026, 9, 15), "US", None), "2026-09")

    def test_jp_stock_plus_three_months(self):
        self.assertEqual(default_pay_month(date(2026, 3, 30), "JP", "EQUITY"), "2026-06")
        self.assertEqual(default_pay_month(date(2025, 9, 29), "JP", "EQUITY"), "2025-12")
        self.assertEqual(default_pay_month(date(2025, 12, 29), "JP", "EQUITY"), "2026-03")   # 年をまたぐ

    def test_jp_etf_37_days(self):
        # 標本 10 銘柄はいずれも決算日の 37〜39 日後に払う
        self.assertEqual(default_pay_month(date(2026, 3, 10), "JP", "ETF"), "2026-04")     # 実際 4/17
        self.assertEqual(default_pay_month(date(2025, 12, 24), "JP", "ETF"), "2026-01")    # 実際 1/30
        self.assertEqual(default_pay_month(date(2026, 1, 8), "JP", "ETF"), "2026-02")      # 実際 2/16


class EstimatePayMonthsTest(unittest.TestCase):
    def test_msft_next_dividend_pair_is_not_attached_to_last_ex_date(self):
        # 従来の不具合: dividendDate（次回=12/10）が、直近の権利落ち日（8/20）に付いて 12 月になっていた
        ex = [date(2025, 11, 20), date(2026, 2, 19), date(2026, 5, 21), date(2026, 8, 20)]
        info = {"quoteType": "EQUITY", "exDividendDate": epoch(2026, 11, 19), "dividendDate": epoch(2026, 12, 10)}
        months, source = estimate_pay_months(ex, "US", info)
        self.assertEqual(months, ["2025-12", "2026-03", "2026-06", "2026-09"])   # 遅れ 21 日
        self.assertEqual(source, "yahoo_lag")

    def test_pair_matching_history_uses_actual_pay_date(self):
        # LNT: 直近の権利落ち日 7/31 の支払日 8/17 が分かる。他は遅れ 17 日で補正
        ex = [date(2025, 10, 31), date(2026, 1, 30), date(2026, 4, 30), date(2026, 7, 31)]
        info = {"quoteType": "EQUITY", "exDividendDate": epoch(2026, 7, 31), "dividendDate": epoch(2026, 8, 17)}
        months, source = estimate_pay_months(ex, "US", info)
        self.assertEqual(months, ["2025-11", "2026-02", "2026-05", "2026-08"])
        self.assertEqual(source, "yahoo_exact")

    def test_only_the_nearest_history_entry_is_fixed(self):
        # 権利落ち日が近接する 2 件（7/29 と 7/31。どちらも組の 7/31 から 3 日以内）でも、
        # 実際の支払日で確定するのは最も近い 7/31 の 1 件だけ。遅れは 32 日（7/31 → 9/1）
        ex = [date(2026, 7, 29), date(2026, 7, 31), date(2026, 8, 5)]
        info = {"quoteType": "EQUITY", "exDividendDate": epoch(2026, 7, 31), "dividendDate": epoch(2026, 9, 1)}
        months, source = estimate_pay_months(ex, "US", info)
        self.assertEqual(months, ["2026-08", "2026-09", "2026-09"])   # 7/29+32d=8/30、実際 9/1、8/5+32d=9/6
        self.assertEqual(source, "yahoo_exact")

    def test_stale_pay_date_is_ignored(self):
        # AAXJ: dividendDate=2016-12-28 → 組が成立しないので、ETF の既定値（+1 日）を使う
        ex = [date(2025, 12, 16), date(2026, 6, 15)]
        info = {"quoteType": "ETF", "dividendDate": epoch(2016, 12, 28)}
        months, source = estimate_pay_months(ex, "US", info)
        self.assertEqual(months, ["2025-12", "2026-06"])
        self.assertEqual(source, "default")

    def test_pay_date_without_ex_date_is_ignored(self):
        # OXLCI: dividendDate=2026-12-31 は次回の利払い。直近（9/15）の支払日ではない
        ex = [date(2025, 12, 15), date(2026, 3, 13), date(2026, 6, 15), date(2026, 9, 15)]
        info = {"quoteType": "EQUITY", "dividendDate": epoch(2026, 12, 31)}
        months, source = estimate_pay_months(ex, "US", info)
        self.assertEqual(months, ["2025-12", "2026-03", "2026-06", "2026-09"])   # 実際は 12/31・3/31・6/30・9/30
        self.assertEqual(source, "default")

    def test_jp_without_pair_uses_defaults(self):
        info = {"quoteType": "EQUITY", "exDividendDate": epoch(2027, 3, 30)}     # 9434.T: 支払日なし
        months, source = estimate_pay_months([date(2025, 9, 29), date(2026, 3, 30)], "JP", info)
        self.assertEqual(months, ["2025-12", "2026-06"])
        self.assertEqual(source, "default")

    def test_empty_inputs(self):
        self.assertEqual(estimate_pay_months([], "US", {}), ([], "default"))
        self.assertEqual(estimate_pay_months([date(2026, 1, 1)], "US", None)[1], "default")

    def test_pay_month_is_never_before_ex_month(self):
        # 性質: どの入力でも、支払い月は権利落ち月以後で、最大でも 4 か月後（遅れの上限 120 日）
        rng = random.Random(1)
        for _ in range(2000):
            ex = date(2025, 1, 1).fromordinal(date(2025, 1, 1).toordinal() + rng.randrange(0, 700))
            market = rng.choice(["JP", "US"])
            qt = rng.choice([None, "EQUITY", "ETF", "MUTUALFUND"])
            info = {"quoteType": qt}
            if rng.random() < 0.6:
                e = date(2025, 1, 1).toordinal() + rng.randrange(0, 900)
                info["exDividendDate"] = epoch(*date.fromordinal(e).timetuple()[:3])
                info["dividendDate"] = info["exDividendDate"] + rng.randrange(-30, 200) * 86400
            months, _ = estimate_pay_months([ex], market, info)
            diff = (int(months[0][:4]) * 12 + int(months[0][5:])) - (ex.year * 12 + ex.month)
            self.assertTrue(0 <= diff <= 5, (ex, market, info, months))


class SanitizeDetailsTest(unittest.TestCase):
    def test_valid_details_are_unchanged(self):
        d = "ex:2026-03-30|pay:2026-06:70.0, ex:2025-12-29|pay:2026-03:27.0"
        self.assertEqual(sanitize_details(d, "JP"), d)

    def test_stale_pay_month_is_replaced_by_default(self):
        # AAXJ: 前回の CSV に残っていた古い支払い月（2016-12）だけを補正する（種別が不明なので +14 日の既定値）
        d = "ex:2025-12-16|pay:2026-01:1.125, ex:2026-06-15|pay:2016-12:0.421"
        self.assertEqual(sanitize_details(d, "US"), "ex:2025-12-16|pay:2026-01:1.125, ex:2026-06-15|pay:2026-06:0.421")

    def test_pay_too_far_after_ex_is_replaced(self):
        self.assertEqual(sanitize_details("ex:2026-01-10|pay:2026-08:1.0", "JP"), "ex:2026-01-10|pay:2026-04:1.0")

    def test_empty_and_unknown_formats_pass_through(self):
        for d in ("", "No Div", "2025-03-28:35.0"):
            self.assertEqual(sanitize_details(d, "US"), d)


if __name__ == "__main__":
    unittest.main()
