"""validate_csv.py の単体テスト（`python -m unittest discover -s scripts` で実行）"""

import unittest

from validate_csv import find_pay_month_violations, regression_messages


class PayMonthViolationsTest(unittest.TestCase):
    def test_valid_entries(self):
        d = "ex:2026-03-30|pay:2026-06:70.0, ex:2025-12-29|pay:2026-03:27.0, ex:2025-12-30|pay:2025-12:0.15"
        self.assertEqual(find_pay_month_violations(d), [])

    def test_pay_before_ex(self):
        # 従来 514 銘柄にあった異常値（支払い月に古い日付が入る）
        d = "ex:2026-06-15|pay:2016-12:0.42, ex:2025-12-16|pay:2026-01:1.1"
        self.assertEqual(find_pay_month_violations(d), ["ex:2026-06-15|pay:2016-12:0.42"])

    def test_gap_boundaries(self):
        self.assertEqual(find_pay_month_violations("ex:2026-01-10|pay:2026-06:1.0"), [])            # 5 か月後は許容
        self.assertEqual(len(find_pay_month_violations("ex:2026-01-10|pay:2026-07:1.0")), 1)       # 6 か月後は違反

    def test_empty_and_unparsable(self):
        self.assertEqual(find_pay_month_violations(""), [])
        self.assertEqual(find_pay_month_violations("No Div"), [])


class RegressionTest(unittest.TestCase):
    HEADER = ["Ticker", "Company", "Price", "Yield(%)", "AnnualDiv", "Sector", "DivDetails"]

    def rows(self, n_total, n_div):
        data = [[f"T{i}", "n", "10", "1", "1", "", "ex:2026-01-01|pay:2026-02:1.0" if i < n_div else ""] for i in range(n_total)]
        return [self.HEADER] + data

    def test_no_regression(self):
        self.assertEqual(regression_messages(self.rows(100, 50), self.rows(100, 50), 6, "us"), [])
        self.assertEqual(regression_messages(self.rows(95, 46), self.rows(100, 50), 6, "us"), [])   # 5〜8% 減までは許容

    def test_dividends_disappear(self):
        # 株価は取れているが配当が取れなくなった（Yahoo の仕様変更など）
        msgs = regression_messages(self.rows(100, 0), self.rows(100, 50), 6, "us")
        self.assertEqual(len(msgs), 1)
        self.assertIn("配当のある銘柄数", msgs[0])

    def test_row_count_drop(self):
        msgs = regression_messages(self.rows(50, 25), self.rows(100, 50), 6, "us")
        self.assertEqual(len(msgs), 2)

    def test_first_run_or_empty_previous(self):
        self.assertEqual(regression_messages(self.rows(10, 5), [self.HEADER], 6, "us"), [])


if __name__ == "__main__":
    unittest.main()
