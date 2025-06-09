# update_csv.py  ── v2 robust
import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

N_DAYS = 60                     # 60 営業日あれば RSI14/SMA20 は可
TICKERS_FILE = "tickers.txt"
OUTFILE = "daily_price_latest.csv"

tickers = Path(TICKERS_FILE).read_text().strip().split()
if not tickers:
    sys.exit("❌ tickers.txt が空です")

dfs = []

for tkr in tickers:
    try:
        df = yf.download(tkr, period="3mo", interval="1d",
                         auto_adjust=False, progress=False, threads=True)
        if df.empty:
            print(f"⚠ NO DATA: {tkr}")
            continue
        df = df.tail(N_DAYS).reset_index()
        df["Ticker"] = tkr
        dfs.append(df[["Date", "Ticker",
                       "Open", "High", "Low", "Close",
                       "Volume", "Adj Close"]])
    except Exception as e:
        print(f"⚠ {tkr} -> {e}")

if not dfs:
    sys.exit("❌ 取得ゼロ：yfinance ブロックかティッカー不正を確認")

pd.concat(dfs).to_csv(OUTFILE, index=False, encoding="utf-8")
print(f"✅ CSV updated: {OUTFILE} rows={sum(len(d) for d in dfs)}")
