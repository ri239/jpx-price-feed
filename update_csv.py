import yfinance as yf, pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

N_DAYS = 90
TICKERS_FILE = "tickers.txt"
OUTFILE = "daily_price_latest.csv"

tickers = Path(TICKERS_FILE).read_text().strip().split()
end = datetime.now().strftime("%Y-%m-%d")
start = (datetime.now() - timedelta(days=N_DAYS*1.5)).strftime("%Y-%m-%d")

dfs = []
for tkr in tickers:
    try:
        df = yf.download(tkr, start=start, end=end, interval="1d", progress=False)
        df = df.tail(N_DAYS).reset_index()
        df["Ticker"] = tkr
        dfs.append(df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume", "Adj Close"]])
    except Exception as e:
        print("⚠", tkr, e)
pd.concat(dfs).to_csv(OUTFILE, index=False, encoding="utf-8")
print("✅ CSV updated:", OUTFILE)
