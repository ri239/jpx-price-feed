# jpx-price-feed

JPX(東京証券取引所)上場銘柄の日次終値と出来高を、CSV(gzip)で配信するリポジトリです。
`tickers.txt` の全銘柄について、直近60営業日分を GitHub Actions で毎営業日更新します。

## ファイル構成

| ファイル | 内容 |
|---|---|
| `daily_price_latest.csv.gz` | 最新の出力データ(毎営業日更新) |
| `daily_price_latest.csv` | 旧形式の出力。現在は更新されていません(下記「注意」参照) |
| `tickers.txt` | 取得対象のティッカー一覧(`1301.T` 形式、約4,400銘柄) |
| `update_csv.py` | `tickers.txt` の銘柄を yfinance で取得し、`daily_price_latest.csv.gz` を出力するスクリプト |
| `.github/workflows/update_csv.yml` | `update_csv.py` を定期実行し、結果をコミット・push する GitHub Actions |
| `.nojekyll` | GitHub Pages 用の空ファイル |

## 出力形式

`daily_price_latest.csv.gz` は、ヘッダ付きの CSV(UTF-8)を gzip 圧縮したものです。

| 列 | 内容 | 例 |
|---|---|---|
| `Date` | 取引日(`YYYY-MM-DD`) | `2026-10-07` |
| `Ticker` | ティッカー | `1301.T` |
| `Close` | 終値(未調整) | `4495.0` |
| `Volume` | 出来高 | `33900.0` |

- 1銘柄あたり直近60営業日分です(`update_csv.py` の `N_DAYS`)。
- 行は銘柄順、同一銘柄内は日付の昇順です。
- 終値は `auto_adjust=False` で取得した未調整値です。株式分割や配当の影響は補正されません。
- データ元は yfinance(Yahoo Finance)です。

## 更新頻度

GitHub Actions が平日(月〜金)16:00 UTC(日本時間の翌 01:00)に自動実行します。手動実行(`workflow_dispatch`)もできます。
GitHub Actions の定期実行は遅延することがあります。祝日や取得失敗時の欠損は保証していません。

## 使い方

```python
import pandas as pd

df = pd.read_csv(
    "daily_price_latest.csv.gz",
    parse_dates=["Date"],
)

# 例: 1301.T の直近の終値
print(df[df["Ticker"] == "1301.T"].tail())
```

ローカルで更新する場合:

```bash
pip install yfinance pandas
python update_csv.py
```

## 注意

- `daily_price_latest.csv` は `update_csv.py` の出力先ではなく、古い日付のデータのままです。最新データは `daily_price_latest.csv.gz` を使ってください。
- データは yfinance 経由で取得しており、正確性・完全性は保証しません。Yahoo Finance のデータの利用条件は確認していないため、利用前に各自でご確認ください。
