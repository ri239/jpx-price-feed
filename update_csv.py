import yfinance as yf, pandas as pd, time
from datetime import datetime
from pathlib import Path

N_DAYS = 60
CHUNK  = 50            # ← 50銘柄ずつ一括取得
RETRY  = 2             # ← 失敗した銘柄を最大2回リトライ
OUTFILE = "daily_price_latest.csv"

tickers = Path("tickers.txt").read_text().split()

def fetch_chunk(chunk):
    try:
        df = yf.download(" ".join(chunk), period="3mo", interval="1d",
                         group_by="ticker", threads=True, progress=False)
        return df
    except Exception as e:
        print("⚠ chunk failed:", e)
        return None

dfs = []
for i in range(0, len(tickers), CHUNK):
    group = tickers[i:i+CHUNK]
    for attempt in range(RETRY):
        panel = fetch_chunk(group)
        if panel is not None and not panel.empty:
            break
        print(f"🔄 retry {attempt+1}/{RETRY} for chunk {group[0]} …")
        time.sleep(3)
    if panel is None or panel.empty:
        continue

    # ── ここで銘柄ごとに展開 ──────────────────────
    for tkr in group:
        if tkr not in panel.columns.get_level_values(0):
            continue
        sub = panel[tkr].tail(N_DAYS).reset_index()
        if sub.empty:                     # データゼロはスキップ
            continue
        sub["Ticker"] = tkr
        dfs.append(sub[["Date","Ticker","Open","High","Low","Close","Volume"]])
    # ────────────────────────────────────────

if not dfs:
    raise RuntimeError("❌ 全銘柄取得に失敗しました。")

pd.concat(dfs).to_csv(OUTFILE, index=False, encoding="utf-8")
print("✅ CSV updated:", OUTFILE, "rows:", sum(len(x) for x in dfs))
