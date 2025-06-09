#!/usr/bin/env python
# update_csv.py  — JPX 全銘柄を高速に取得して CSV 出力
#   ・50 銘柄ずつ一括取得（CHUNK）
#   ・タイムアウト／欠損は最大 RETRY 回リトライ
#   ・空データ銘柄は自動スキップ
#   ・FutureWarning を非表示
#   ・最終出力列：Date, Ticker, Open, High, Low, Close, Volume

import warnings, time
from pathlib import Path
from datetime import datetime
import pandas as pd
import yfinance as yf

# ---------- 設定 ----------
CHUNK   = 50      # ← 1 回に呼ぶティッカー数
RETRY   = 2       # ← 失敗チャンクのリトライ回数
N_DAYS  = 60      # ← 保存する営業日数
OUTFILE = "daily_price_latest.csv"
# --------------------------

warnings.filterwarnings("ignore", category=FutureWarning)  # yfinance のお知らせ抑制

tickers_file = Path("tickers.txt")
if not tickers_file.exists():
    raise FileNotFoundError("tickers.txt がありません")

tickers = tickers_file.read_text().split()
dfs = []

def fetch_chunk(symbols: list[str]) -> pd.DataFrame | None:
    """50銘柄まとめて取得して DataFrame を返す。失敗時は None"""
    try:
        return yf.download(
            " ".join(symbols),
            period="3mo",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,        # ← adjust 明示で Warning 抑制
            threads=True,
            progress=False,
        )
    except Exception as e:
        print("⚠ download error:", e)
        return None

print(f"⏱  {datetime.now():%H:%M:%S}  Start  ({len(tickers)} tickers, {CHUNK} per chunk)")

for i in range(0, len(tickers), CHUNK):
    group = tickers[i:i + CHUNK]
    panel = None
    for attempt in range(1, RETRY + 1):
        panel = fetch_chunk(group)
        if panel is not None and not panel.empty:
            break
        print(f"🔄 retry {attempt}/{RETRY} for chunk {group[0]}…")
        time.sleep(3)

    if panel is None or panel.empty:
        continue  # このチャンクを諦める

    # ── 銘柄ごとに展開して DataFrame を整形 ──
    for tkr in group:
        if tkr not in panel.columns.get_level_values(0):
            continue
        sub = panel[tkr].tail(N_DAYS).reset_index()
        if sub.empty:
            continue
        sub["Ticker"] = tkr
        dfs.append(
            sub[["Date", "Ticker", "Close", "Volume"]]
        )

if not dfs:
    raise RuntimeError("❌ 取得ゼロ：API ブロックやネット障害を確認してください")

out_df = pd.concat(dfs, ignore_index=True)
out_df.to_csv(OUTFILE, index=False, encoding="utf-8")
print(
    f"✅ {OUTFILE} written — rows: {len(out_df):,}, "
    f"file size: {Path(OUTFILE).stat().st_size/1_048_576:.1f} MB"
)
