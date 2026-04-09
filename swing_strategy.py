#!/usr/bin/env python
"""
swing_strategy.py — JPX 日本株スイングトレード戦略

設計目標
  - プロフィットファクター (PF) 1.6 以上
  - 最大ドローダウン (MDD) -12% 以内
  - 銘柄あたり四半期 1 回以上のエントリー
  - スイング主体・利が乗れば中長期へ延長

使用方法
  python swing_strategy.py               # yfinance でデータ取得してバックテスト
  python swing_strategy.py --no-fetch    # ohlcv_cache.csv.gz を再利用
  python swing_strategy.py --demo        # 合成データで動作確認（ネット不要）
  python swing_strategy.py --top N       # 流動性上位 N 銘柄（デフォルト 200）

戦略概要
  エントリー条件（全充足）
    1. EMA5 が EMA25 を上抜け（短期ゴールデンクロス）
    2. 終値 > EMA75（中期上昇トレンド）
    3. RSI(14) が 45-70（過熱なき上昇モメンタム）
    4. 当日出来高 ≥ 20 日平均出来高 × 1.3（出来高確認）
    5. ATR/終値 ≤ 7%（過度なボラティリティ除外）

  イグジット条件（ATR トレーリングストップ 3 段階）
    Stage 1  含み益 < 5%    → 高値 - 2.0 × ATR
    Stage 2  含み益 5-12%   → 高値 - 2.5 × ATR（利益を伸ばす）
    Stage 3  含み益 ≥ 12%   → 高値 - 3.2 × ATR（中長期保有）
    ハードストップ          → エントリー比 -8%
    タイムストップ          → 25 日経過かつ含み益 < 1%

  ポジション管理
    リスク per トレード     : NAV の 1.2%
    最大同時保有銘柄        : 20
    1 銘柄最大配分          : NAV の 10%
"""

import warnings
import argparse
import time
import sys
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
#  戦略パラメータ
# ─────────────────────────────────────────────
PARAMS = dict(
    ema_fast        = 5,
    ema_mid         = 25,
    ema_slow        = 75,
    rsi_period      = 14,
    atr_period      = 14,
    vol_ma_period   = 20,

    rsi_lo          = 40,
    rsi_hi          = 72,
    vol_mult        = 1.2,
    max_atr_ratio   = 0.07,

    atr_stop_s1     = 2.0,
    atr_stop_s2     = 2.5,
    atr_stop_s3     = 3.2,
    profit_s2       = 0.05,
    profit_s3       = 0.12,
    hard_stop_pct   = 0.08,

    time_stop_days  = 12,
    time_stop_ret   = 0.005,   # 12 日で +0.5% 未満なら切る

    risk_per_trade  = 0.012,
    atr_risk_mult   = 2.0,
    max_positions   = 25,
    max_alloc_pct   = 0.10,

    initial_capital = 10_000_000,
)

CACHE_FILE = Path("ohlcv_cache.csv.gz")


# ─────────────────────────────────────────────
#  合成データ生成（デモ用・ネット不要）
# ─────────────────────────────────────────────
def generate_synthetic_data(
    n_stocks: int = 100,
    n_years:  int = 4,
    seed:     int = 42,
) -> pd.DataFrame:
    """
    JPX 株価に近似した合成 OHLCV データを生成する。
    各銘柄のパラメータをランダムに設定し現実的な挙動を再現する。
    """
    rng = np.random.default_rng(seed)
    start = datetime(2022, 1, 4)
    bdays = pd.bdate_range(start, periods=n_years * 252)
    n_days = len(bdays)

    rows = []
    for i in range(n_stocks):
        tkr = f"SYNTH{1000 + i}.T"

        # 銘柄固有パラメータ
        annual_drift = rng.uniform(-0.05, 0.25)   # 年率 -5%〜+25%
        annual_vol   = rng.uniform(0.15, 0.50)    # 年率ボラ 15%〜50%
        daily_drift  = annual_drift / 252
        daily_vol    = annual_vol / np.sqrt(252)

        # 2〜4 回のレジームシフト
        n_regimes = rng.integers(2, 5)
        regime_starts = sorted(rng.integers(0, n_days, n_regimes))
        regime_mults  = rng.uniform(0.5, 1.5, n_regimes)

        # 初期価格（500〜5,000 円）
        price0 = rng.uniform(500, 5000)
        closes = np.empty(n_days)
        closes[0] = price0

        for d in range(1, n_days):
            # レジーム係数
            mult = 1.0
            for ri, rs in enumerate(regime_starts):
                if d >= rs:
                    mult = regime_mults[ri]
            drift = daily_drift * mult
            vol   = daily_vol * mult
            ret   = drift + vol * rng.standard_normal()
            closes[d] = closes[d - 1] * (1 + ret)

        closes = np.maximum(closes, 50.0)   # 最低株価

        # High / Low を Close から近似生成
        # 日中変動幅 ≈ ATR(14) ≈ daily_vol * price * sqrt(13/14) * 1.6
        intraday_range = daily_vol * closes * rng.uniform(0.8, 1.4, n_days) * np.sqrt(252 / 14)
        highs  = closes + intraday_range * rng.uniform(0.3, 0.7, n_days)
        lows   = closes - intraday_range * rng.uniform(0.3, 0.7, n_days)
        lows   = np.maximum(lows, closes * 0.80)
        opens  = np.roll(closes, 1)
        opens[0] = price0

        # 出来高（出来高急増あり）
        base_vol   = rng.uniform(50_000, 2_000_000)
        vol_arr    = base_vol * rng.lognormal(0, 0.5, n_days)
        # 大きな価格変動日は出来高も増加
        abs_ret    = np.abs(np.diff(closes, prepend=closes[0]) / closes)
        vol_arr   *= (1 + 3 * abs_ret)

        for d in range(n_days):
            rows.append(dict(
                Date   = bdays[d],
                Ticker = tkr,
                Open   = round(opens[d],  1),
                High   = round(highs[d],  1),
                Low    = round(lows[d],   1),
                Close  = round(closes[d], 1),
                Volume = int(vol_arr[d]),
            ))

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    print(f"✅  合成データ生成: {n_stocks} 銘柄 × {n_days} 日 = {len(df):,} 行")
    return df


# ─────────────────────────────────────────────
#  yfinance データ取得
# ─────────────────────────────────────────────
def load_tickers(top_n: int) -> list[str]:
    for fname in ("daily_price_latest.csv", "daily_price_latest.csv.gz"):
        p = Path(fname)
        if p.exists():
            df = pd.read_csv(p)
            med_vol = (
                df.groupby("Ticker")["Volume"]
                .median()
                .sort_values(ascending=False)
                .head(top_n)
            )
            return med_vol.index.tolist()
    raise FileNotFoundError("daily_price_latest.csv[.gz] が見つかりません")


def fetch_ohlcv(tickers: list[str], years: int = 4) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance が見つかりません: pip install yfinance")

    period = f"{years}y"
    CHUNK  = 50
    dfs    = []

    print(f"📥  {len(tickers)} 銘柄の OHLCV を取得中 (period={period}) …")
    for i in range(0, len(tickers), CHUNK):
        group = tickers[i: i + CHUNK]
        for attempt in range(3):
            try:
                raw = yf.download(
                    " ".join(group),
                    period=period,
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=True,
                    threads=True,
                    progress=False,
                )
                break
            except Exception as e:
                print(f"  ⚠ retry {attempt+1}/3 : {e}")
                time.sleep(3 * (attempt + 1))
        else:
            continue

        if raw is None or raw.empty:
            continue

        for tkr in group:
            try:
                sub = raw[tkr].dropna(subset=["Close"]).copy()
            except KeyError:
                continue
            if len(sub) < 120:
                continue
            sub = sub.reset_index()
            sub.columns = [str(c).replace(" ", "_") for c in sub.columns]
            sub["Ticker"] = tkr
            needed = [c for c in ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
                      if c in sub.columns]
            dfs.append(sub[needed])

        if (i // CHUNK) % 5 == 0:
            print(f"  … {i + len(group)}/{len(tickers)} 完了")

    if not dfs:
        raise RuntimeError("データ取得ゼロ。--demo を試してください")

    out = pd.concat(dfs, ignore_index=True)
    out["Date"] = pd.to_datetime(out["Date"]).dt.tz_localize(None)
    out.to_csv(CACHE_FILE, index=False, compression="gzip")
    print(f"✅  キャッシュ保存: {CACHE_FILE}  ({len(out):,} 行)")
    return out


def load_or_fetch(tickers: list[str], no_fetch: bool) -> pd.DataFrame:
    if CACHE_FILE.exists() and no_fetch:
        df = pd.read_csv(CACHE_FILE, parse_dates=["Date"])
        print(f"📂  キャッシュ読み込み: {len(df):,} 行")
        return df
    return fetch_ohlcv(tickers)


# ─────────────────────────────────────────────
#  テクニカル指標
# ─────────────────────────────────────────────
def add_indicators(df: pd.DataFrame, p: dict) -> pd.DataFrame:
    df = df.sort_values("Date").copy()

    c = df["Close"]

    # EMA
    df["ema_fast"] = c.ewm(span=p["ema_fast"], adjust=False).mean()
    df["ema_mid"]  = c.ewm(span=p["ema_mid"],  adjust=False).mean()
    df["ema_slow"] = c.ewm(span=p["ema_slow"], adjust=False).mean()

    # RSI
    delta  = c.diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    avg_g  = gain.ewm(alpha=1/p["rsi_period"], adjust=False).mean()
    avg_l  = loss.ewm(alpha=1/p["rsi_period"], adjust=False).mean()
    rs     = avg_g / avg_l.replace(0, np.nan)
    df["rsi"] = 100 - 100 / (1 + rs)

    # ATR（OHLCV があれば True Range、なければ Close 変動から近似）
    if {"High", "Low"}.issubset(df.columns):
        hl  = df["High"] - df["Low"]
        hc  = (df["High"] - c.shift()).abs()
        lc  = (df["Low"]  - c.shift()).abs()
        tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    else:
        tr  = c.diff().abs()   # fallback

    df["atr"] = tr.ewm(span=p["atr_period"], adjust=False).mean()

    # 出来高移動平均
    df["vol_ma"] = df["Volume"].rolling(p["vol_ma_period"]).mean()

    # シグナル 1: EMA5 クロスアップ（短期 GC）
    pf = df["ema_fast"].shift(1)
    pm = df["ema_mid"].shift(1)
    df["cross_up"] = (pf <= pm) & (df["ema_fast"] > df["ema_mid"])

    # シグナル 2: 上昇トレンド中の押し目（EMA5 > EMA25 で EMA5 まで押してからの回復）
    prev_c = c.shift(1)
    ef1    = df["ema_fast"].shift(1)
    df["pullback"] = (
        (df["ema_fast"] > df["ema_mid"])    # すでに上昇トレンド
        & (prev_c <= ef1)                   # 前日 Close ≤ EMA5（押し目）
        & (c > df["ema_fast"])              # 当日 Close が EMA5 を回復
    )

    # シグナル 3: 20 日高値ブレイクアウト（出来高確認あり）
    high20       = c.shift(1).rolling(20).max()
    df["breakout"] = (
        (c > high20)                        # 20 日高値更新
        & (df["ema_fast"] > df["ema_mid"])  # 短期上昇トレンド確認
    )

    # シグナル 4: RSI が 50 を上抜け（押し目後の勢い回復）
    rsi_prev = df["rsi"].shift(1)
    df["rsi_cross50"] = (
        (rsi_prev < 50)
        & (df["rsi"] >= 50)
        & (c > df["ema_mid"])               # EMA25 より上
        & (df["ema_fast"] > df["ema_mid"])  # 上昇トレンド中
    )

    # 統合エントリーシグナル
    df["entry_signal"] = (
        df["cross_up"] | df["pullback"] | df["breakout"] | df["rsi_cross50"]
    )

    return df.dropna(
        subset=["ema_slow", "rsi", "atr", "vol_ma", "entry_signal"]
    )


# ─────────────────────────────────────────────
#  バックテストエンジン
# ─────────────────────────────────────────────
def backtest(price_df: pd.DataFrame, p: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns
    -------
    trades_df : 全トレード記録
    equity_df : 日次資産曲線
    """
    groups = []
    for tkr, g in price_df.groupby("Ticker"):
        g2 = add_indicators(g, p)
        min_rows = p["ema_slow"] + p["rsi_period"] + 5
        if len(g2) >= min_rows:
            groups.append(g2)

    if not groups:
        raise ValueError("有効銘柄ゼロ（データ不足）")

    all_data  = pd.concat(groups).sort_values(["Date", "Ticker"])
    dates     = sorted(all_data["Date"].unique())
    tkr_daily = {
        tkr: grp.set_index("Date")
        for tkr, grp in all_data.groupby("Ticker")
    }

    cash      = float(p["initial_capital"])
    positions = {}
    trades    = []
    equity    = []

    for date in dates:
        # 時価評価
        nav = cash + sum(
            pos["shares"] * tkr_daily[tkr].at[date, "Close"]
            for tkr, pos in positions.items()
            if date in tkr_daily[tkr].index
        )
        equity.append({"Date": date, "NAV": nav})

        to_close = []

        # ── イグジット判定 ──
        for tkr, pos in positions.items():
            idx = tkr_daily[tkr]
            if date not in idx.index:
                continue

            row   = idx.loc[date]
            close = float(row["Close"])
            atr   = float(row["atr"])
            ret   = close / pos["entry_price"] - 1.0
            days_held = (date - pos["entry_date"]).days

            # ハードストップ
            if ret <= -p["hard_stop_pct"]:
                to_close.append((tkr, close, "hard_stop", ret, days_held))
                continue

            # ステージ更新 & トレーリング
            if ret >= p["profit_s3"]:
                mult  = p["atr_stop_s3"]
                stage = 3
            elif ret >= p["profit_s2"]:
                mult  = p["atr_stop_s2"]
                stage = 2
            else:
                mult  = p["atr_stop_s1"]
                stage = 1

            pos["highest"] = max(pos["highest"], close)
            new_stop = pos["highest"] - mult * atr
            pos["stop"]  = max(pos["stop"], new_stop)
            pos["stage"] = stage

            if close <= pos["stop"]:
                to_close.append((tkr, close, f"trail_s{stage}", ret, days_held))
                continue

            # タイムストップ
            if days_held >= p["time_stop_days"] and ret < p["time_stop_ret"]:
                to_close.append((tkr, close, "time_stop", ret, days_held))

        # ── クローズ処理 ──
        for tkr, price, reason, ret, days_held in to_close:
            pos  = positions.pop(tkr)
            pnl  = pos["shares"] * (price - pos["entry_price"])
            cash += pos["shares"] * price
            trades.append(dict(
                ticker      = tkr,
                entry_date  = pos["entry_date"],
                exit_date   = date,
                entry_price = pos["entry_price"],
                exit_price  = price,
                shares      = pos["shares"],
                pnl         = pnl,
                ret         = ret,
                days_held   = days_held,
                reason      = reason,
            ))

        # ── エントリー判定 ──
        if len(positions) < p["max_positions"]:
            candidates = []
            for tkr, day_df in tkr_daily.items():
                if tkr in positions or date not in day_df.index:
                    continue
                row   = day_df.loc[date]
                close = float(row["Close"])
                atr   = float(row["atr"])
                if not (
                    bool(row["entry_signal"])
                    and close > float(row["ema_slow"])
                    and p["rsi_lo"] <= float(row["rsi"]) <= p["rsi_hi"]
                    and float(row["Volume"]) >= p["vol_mult"] * float(row["vol_ma"])
                    and atr / close <= p["max_atr_ratio"]
                    and close > 0
                    and atr > 0
                ):
                    continue
                # RSI を強さスコアとして使い、強いものを優先エントリー
                candidates.append((tkr, close, atr, float(row["rsi"])))

            # RSI 降順（強いモメンタム優先）
            candidates.sort(key=lambda x: -x[3])

            for tkr, close, atr, _ in candidates:
                if len(positions) >= p["max_positions"]:
                    break
                risk_amount    = nav * p["risk_per_trade"]
                risk_per_share = p["atr_risk_mult"] * atr
                shares = int(risk_amount / risk_per_share)
                cost   = shares * close

                if shares <= 0:
                    continue
                if cost > nav * p["max_alloc_pct"]:
                    shares = int(nav * p["max_alloc_pct"] / close)
                    cost   = shares * close
                if shares <= 0 or cost > cash:
                    continue

                cash -= cost
                positions[tkr] = dict(
                    shares      = shares,
                    entry_price = close,
                    entry_date  = date,
                    stop        = close - p["atr_stop_s1"] * atr,
                    stage       = 1,
                    highest     = close,
                )

    # 残ポジションを最終日に強制決済
    last_date = dates[-1]
    for tkr, pos in list(positions.items()):
        idx = tkr_daily[tkr]
        close = float(idx.at[last_date, "Close"]) if last_date in idx.index else pos["entry_price"]
        ret       = close / pos["entry_price"] - 1.0
        pnl       = pos["shares"] * (close - pos["entry_price"])
        days_held = (last_date - pos["entry_date"]).days
        cash     += pos["shares"] * close
        trades.append(dict(
            ticker      = tkr,
            entry_date  = pos["entry_date"],
            exit_date   = last_date,
            entry_price = pos["entry_price"],
            exit_price  = close,
            shares      = pos["shares"],
            pnl         = pnl,
            ret         = ret,
            days_held   = days_held,
            reason      = "end_of_data",
        ))

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity).set_index("Date")
    return trades_df, equity_df


# ─────────────────────────────────────────────
#  パフォーマンス評価
# ─────────────────────────────────────────────
def evaluate(trades_df: pd.DataFrame, equity_df: pd.DataFrame, p: dict) -> dict:
    if trades_df.empty:
        return {}

    wins  = trades_df[trades_df["pnl"] > 0]
    loses = trades_df[trades_df["pnl"] <= 0]

    gross_profit = wins["pnl"].sum()
    gross_loss   = loses["pnl"].abs().sum()
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    win_rate = len(wins) / len(trades_df) if len(trades_df) > 0 else 0
    avg_win  = wins["pnl"].mean()  if len(wins)  > 0 else 0
    avg_loss = loses["pnl"].mean() if len(loses) > 0 else 0
    payoff   = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    nav      = equity_df["NAV"]
    roll_max = nav.cummax()
    dd       = (nav - roll_max) / roll_max
    max_dd   = dd.min()

    total_ret = nav.iloc[-1] / p["initial_capital"] - 1
    n_years   = (equity_df.index[-1] - equity_df.index[0]).days / 365.25
    cagr      = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else 0

    daily_ret = nav.pct_change().dropna()
    sharpe    = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)
                 if daily_ret.std() > 0 else 0)

    n_quarters = n_years * 4
    n_tickers  = trades_df["ticker"].nunique()
    avg_entry_per_tkr_qtr = (
        len(trades_df) / n_tickers / n_quarters
        if n_tickers > 0 and n_quarters > 0 else 0
    )
    entries_per_quarter = len(trades_df) / n_quarters if n_quarters > 0 else 0

    stage_dist = trades_df["reason"].value_counts().to_dict()

    return dict(
        total_trades           = len(trades_df),
        win_rate               = win_rate,
        profit_factor          = pf,
        payoff_ratio           = payoff,
        avg_win                = avg_win,
        avg_loss               = avg_loss,
        gross_profit           = gross_profit,
        gross_loss             = gross_loss,
        max_drawdown           = max_dd,
        cagr                   = cagr,
        sharpe                 = sharpe,
        total_return           = total_ret,
        avg_hold_days          = trades_df["days_held"].mean(),
        n_tickers              = n_tickers,
        n_years                = n_years,
        avg_entry_per_tkr_qtr  = avg_entry_per_tkr_qtr,
        entries_per_quarter    = entries_per_quarter,
        exit_reasons           = stage_dist,
    )


def print_report(m: dict, mode: str = ""):
    sep = "─" * 58
    tag = f"  [{mode}]" if mode else ""
    print(f"\n{'═'*58}")
    print(f"  JPX スイング戦略  バックテスト結果{tag}")
    print(f"{'═'*58}")
    print(f"  期間                : {m['n_years']:.1f} 年")
    print(f"  対象銘柄数          : {m['n_tickers']}")
    print(f"  総トレード数        : {m['total_trades']}")
    print(f"  平均保有日数        : {m['avg_hold_days']:.1f} 日")
    print(f"  四半期エントリー数  : {m['entries_per_quarter']:.1f} 回/四半期（全体）")
    print(f"  Qtry/銘柄(取引銘柄): {m['avg_entry_per_tkr_qtr']:.2f} 回/銘柄"
          f"  {'✅ OK' if m['avg_entry_per_tkr_qtr'] >= 1.0 else '  ※ロング保有が長いほど低下'}")
    print(sep)
    print(f"  勝率                : {m['win_rate']*100:.1f} %")
    print(f"  ペイオフレシオ      : {m['payoff_ratio']:.2f}")
    pf_ok = m['profit_factor'] >= 1.6
    print(f"  プロフィットF       : {m['profit_factor']:.2f}"
          f"  {'✅ 目標達成 (≥1.6)' if pf_ok else '⚠ 目標未達 (<1.6)'}")
    print(sep)
    dd_ok = m['max_drawdown'] >= -0.12
    print(f"  最大 DD             : {m['max_drawdown']*100:.2f} %"
          f"  {'✅ 許容内 (>-12%)' if dd_ok else '⚠ 許容超過 (<-12%)'}")
    print(f"  CAGR                : {m['cagr']*100:.2f} %")
    print(f"  シャープレシオ      : {m['sharpe']:.2f}")
    print(f"  総利益              : ¥{m['gross_profit']:,.0f}")
    print(f"  総損失              : ¥{m['gross_loss']:,.0f}")
    print(sep)
    print("  イグジット内訳:")
    for reason, cnt in sorted(m["exit_reasons"].items(), key=lambda x: -x[1]):
        print(f"    {reason:<22} {cnt:>5} 回")
    print(f"{'═'*58}\n")


# ─────────────────────────────────────────────
#  メイン
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="JPX スイング戦略バックテスト")
    parser.add_argument("--no-fetch", action="store_true", help="キャッシュを再利用")
    parser.add_argument("--demo",     action="store_true", help="合成データで動作確認")
    parser.add_argument("--top",      type=int, default=200, help="流動性上位 N 銘柄")
    parser.add_argument("--stocks",   type=int, default=100, help="--demo 時の銘柄数")
    parser.add_argument("--years",    type=int, default=4,   help="--demo 時のデータ期間（年）")
    args = parser.parse_args()

    print(f"⏱  {datetime.now():%H:%M:%S}  開始")

    if args.demo:
        ohlcv_df = generate_synthetic_data(n_stocks=args.stocks, n_years=args.years)
        mode = "合成データ"
    else:
        tickers  = load_tickers(args.top)
        print(f"  対象ティッカー: {len(tickers)} 銘柄")
        ohlcv_df = load_or_fetch(tickers, args.no_fetch)
        mode = "実データ"

    print(f"⚙  バックテスト実行中 …")
    t0 = time.time()
    trades_df, equity_df = backtest(ohlcv_df, PARAMS)
    elapsed = time.time() - t0
    print(f"   完了 ({elapsed:.1f} 秒)  トレード数: {len(trades_df)}")

    metrics = evaluate(trades_df, equity_df, PARAMS)
    print_report(metrics, mode)

    out_prefix = "demo" if args.demo else "backtest"
    trades_df.to_csv(f"{out_prefix}_trades.csv", index=False)
    equity_df.to_csv(f"{out_prefix}_equity.csv")
    print(f"📄  {out_prefix}_trades.csv / {out_prefix}_equity.csv を保存しました\n")


if __name__ == "__main__":
    main()
