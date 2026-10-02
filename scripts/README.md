# scripts/ — 株価・配当データの自動更新

アプリ「配当管理」が参照する `data/jp_stocks.csv`・`data/us_stocks.csv`・`data/metadata.json` を、
毎週 GitHub Actions（`.github/workflows/update-stocks.yml`）で作り直す。

| ファイル | 役割 |
|---|---|
| `update_stocks.py` | 銘柄マスタ（`data/*_ticker_master.csv`）の全銘柄を yfinance で取得し、CSV を書き出す |
| `paydate.py` | 支払い月の推定ロジック（純粋関数。yfinance に依存しない） |
| `validate_csv.py` | 書き出した CSV の検証。エラーがあると、コミット（公開）されない |
| `test_paydate.py`, `test_validate_csv.py` | 単体テスト（標準ライブラリの `unittest`） |
| `data/overrides.json` | yfinance の値が不正確な銘柄の手動補正（スクリプトの入力。CSV は毎週作り直されるので、CSV を直接編集しても消える） |

## 支払い月の推定

CSV の配当内訳 `ex:権利落ち日|pay:支払い月:金額` の **支払い月は推定値**（Yahoo Finance は過去の配当の支払日を提供しない）。
権利落ち日は Yahoo の実績で、月は 30 銘柄の監査で 100% 一致した。

1. Yahoo の `exDividendDate` と `dividendDate` の組が揃い、遅れが 0〜120 日なら、その日数を全件の権利落ち日に足す
   （履歴の権利落ち日と一致する 1 件は、実際の支払日をそのまま使う）。`dividendDate` だけがある場合や、古い日付は使わない。
2. 組がない銘柄は、市場・種別ごとの既定値:

| 対象 | 既定値 | 根拠（実際の支払日との突き合わせ） |
|---|---|---|
| 米国 ETF | 権利落ち + 1 日 | 標本 9 銘柄で +0〜1 日が月の一致 93% で最良 |
| 米国 個別株 | 権利落ち + 14 日 | 標本 4 銘柄の中央値 15.5 日 |
| 日本 ETF | 決算日 + 37 日 | 14 銘柄・27 件が決算日の 37〜39 日後。+37 日で月の一致 95%（旧 +3 か月は 0%） |
| 日本 個別株・J-REIT | 権利落ち月 + 3 か月 | 標本 13 銘柄中 12 銘柄で全件一致 |

結果（2026-09-30、実際の支払日と支払い月を比較。旧: 日本 +3 か月 / 米国 +1 か月の固定）:

- 30 銘柄の標本（既定値の決定にも使った）: 旧 18%（36/195）→ 新 95%（186/195）
- ホールドアウト 14 銘柄（既定値の決定に使っていない）: 旧 9%（5/55）→ 新 100%（55/55）
- 弱点: 月末に権利落ちして、翌月に払う銘柄（例: 12/30 権利落ち → 1/5 払い）は、日数の遅れが銘柄ごとに違うため外れることがある。

## 動作確認・テスト

```bash
pip install yfinance pandas python-dateutil
python -m unittest discover -s scripts -p "test_*.py" -v      # 単体テスト
LIMIT_TICKERS=30 python scripts/update_stocks.py               # 市場ごとに 30 銘柄だけ取得（約 2 分）
LIMIT_TICKERS=30 python scripts/validate_csv.py                # 前回比の検査を省略して検証
git checkout -- data/                                          # 動作確認で書き換わった CSV を戻す
```

GitHub 上では、Actions の **Update Stock Data** を `Run workflow` で実行し、`limit_tickers` に銘柄数（例: 300）を入れる。
**main 以外のブランチで実行したときは、結果をコミットせず、artifact（`stock-data-<run id>`、14 日保持）に保存するだけ**で、`main`・Pages には影響しない。
ログの「支払い月の推定の根拠」で、Yahoo の支払日が使えた割合（`yahoo_exact` / `yahoo_lag`）と既定値（`default`）の内訳を確認できる。

## 運用上の注意

- **実行時間**: 約 2.5〜3 時間（上限 6 時間）。定期実行は日曜 15:00 UTC（月曜 0:00 JST）だが、実際の開始は数時間遅れる。
- **`main` への push のタイミング**: 更新は最後に `main` へ push する（直前に `git pull --rebase` で取り込む）。
  実行中に `main` を更新すると、まれに競合して、その週の更新が失敗する。**月曜 0:00〜8:00 JST ごろの `main` への push は避ける。**
- **CSV の互換性**: リリース済みのアプリは、ヘッダー名で列を読み（列の追加は無害）、配当内訳を `ex:YYYY-MM-DD|pay:YYYY-MM:金額` の形式で解釈する。
  ヘッダー名の変更、列の並べ替え、配当内訳の形式変更は、既存のアプリを壊す。`validate_csv.py` はヘッダーの不一致をエラーにする（配当内訳の形式の不正は警告のみ）。
- **検証の安全弁**（`validate_csv.py`）: 株価の 30% 以上の欠損、支払い月の異常（権利落ち月より前・6 か月以上後）、
  前回比で銘柄数・配当のある銘柄数が 10% 以上減った場合は、エラーとして公開しない（前週の CSV が残る）。
- ルート直下の `jp_stocks.csv` などは、旧版のアプリ向けの手動アップロード（2026-04-11）で、自動更新の対象外。
