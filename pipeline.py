#!/usr/bin/env python3
"""
QuarterLevel Lab — deterministic data pipeline

Outputs 3 parquet datasets (partitioned by symbol/timeframe/date):
1) data/raw_bars
2) data/level_events_labeled
3) data/event_features (optional)

Key change (per request):
- Store bars_to_* for ALL ladder levels up to K_MAX as LIST columns:
    tp_hit_bars: list[int?]  # index 0 => TP1 (0.125), index 1 => TP2 (0.250), ...
    sl_hit_bars: list[int?]  # same but for adverse direction

Note:
- Event is 0.25 level interaction; reference price p0 = level.
- Direction-agnostic, but we define side = UP/DOWN by which excursion (up/down) is larger.
  Then SL ladder is measured on the opposite side (adverse).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl


# =========================
# Config
# =========================

@dataclass(frozen=True)
class LadderConfig:
    level_step: float = 0.25       # fixed levels
    tp_step: float = 0.125         # TP ladder step
    sl_step: float = 0.125         # SL ladder step
    eps: float = 1e-6              # float tolerance
    merge_gap_bars: int = 3        # merge hits within N bars into one event
    max_bars_forward: int = 80     # horizon after t0
    k_max: int = 32                # store bars_to for TP1..TPk_max and SL1..SLk_max


# =========================
# IO helpers
# =========================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def parse_timestamp_and_date(df: pl.DataFrame) -> pl.DataFrame:
    if df.schema.get("timestamp") != pl.Datetime:
        df = df.with_columns(
            pl.col("timestamp").str.strptime(pl.Datetime, strict=False).alias("timestamp")
        )
    df = df.with_columns(pl.col("timestamp").dt.date().alias("date"))
    return df

def add_symbol_tf(df: pl.DataFrame, symbol: str, timeframe: str) -> pl.DataFrame:
    return df.with_columns([
        pl.lit(symbol).alias("symbol"),
        pl.lit(timeframe).alias("timeframe"),
    ])

def write_partitioned_parquet(df: pl.DataFrame, base_dir: Path) -> None:
    ensure_dir(base_dir)
    df.write_parquet(
        str(base_dir),
        use_pyarrow=True,
        compression="zstd",
        statistics=True,
        row_group_size=200_000,
        partition_by=["symbol", "timeframe", "date"],
    )

def read_csv_ohlcv(csv_path: Path) -> pl.DataFrame:
    df = pl.read_csv(str(csv_path), try_parse_dates=False)
    lower = {c.lower(): c for c in df.columns}
    need = ["timestamp", "open", "high", "low", "close", "volume"]
    miss = [c for c in need if c not in lower]
    if miss:
        raise ValueError(f"Missing columns: {miss}. Found: {df.columns}")

    df = df.rename({
        lower["timestamp"]: "timestamp",
        lower["open"]: "open",
        lower["high"]: "high",
        lower["low"]: "low",
        lower["close"]: "close",
        lower["volume"]: "volume",
    })

    df = parse_timestamp_and_date(df).with_columns([
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64),
    ])

    df = df.sort("timestamp").unique(subset=["timestamp"], keep="last")
    return df


# =========================
# Level hits / events
# =========================

def round_down_to_step(x: float, step: float) -> float:
    return (x // step) * step

def round_up_to_step(x: float, step: float) -> float:
    import math
    return math.ceil(x / step) * step

def levels_between(low: float, high: float, step: float, eps: float) -> List[float]:
    if high < low:
        return []
    start = round_down_to_step(low + eps, step)
    end = round_up_to_step(high - eps, step)
    if end < start:
        return []
    n = int(round((end - start) / step))
    return [start + i * step for i in range(n + 1)]

def tf_to_minutes(tf: str) -> int:
    tf = tf.upper()
    if tf.startswith("M"):
        return int(tf[1:])
    if tf.startswith("H"):
        return int(tf[1:]) * 60
    raise ValueError(f"Unsupported timeframe: {tf}")

def detect_level_hits(bars: pl.DataFrame, cfg: LadderConfig) -> pl.DataFrame:
    hits = (
        bars.select(["timestamp", "date", "low", "high", "symbol", "timeframe"])
        .with_columns(
            pl.struct(["low", "high"]).map_elements(
                lambda s: levels_between(float(s["low"]), float(s["high"]), cfg.level_step, cfg.eps),
                return_dtype=pl.List(pl.Float64),
            ).alias("levels")
        )
        .explode("levels")
        .rename({"levels": "level"})
        .drop(["low", "high"])
        .sort(["level", "timestamp"])
    )
    return hits

def merge_hits_into_events(hits: pl.DataFrame, cfg: LadderConfig) -> pl.DataFrame:
    tf_minutes = tf_to_minutes(hits.select(pl.first("timeframe")).item())
    allowed_gap_min = cfg.merge_gap_bars * tf_minutes

    hits = hits.with_columns(
        pl.col("timestamp").shift(1).over("level").alias("prev_ts")
    ).with_columns(
        (
            pl.col("prev_ts").is_null() |
            ((pl.col("timestamp") - pl.col("prev_ts")) > pl.duration(minutes=allowed_gap_min))
        ).alias("is_new_event")
    ).with_columns(
        pl.col("is_new_event").cast(pl.Int64).cum_sum().over("level").alias("event_group")
    )

    events = (
        hits.group_by(["symbol", "timeframe", "date", "level", "event_group"])
        .agg([
            pl.min("timestamp").alias("t0"),
            pl.max("timestamp").alias("t_last"),
            pl.count().alias("n_hits"),
        ])
        .drop("event_group")
        .with_columns([
            pl.col("level").alias("p0"),
            (pl.col("symbol") + "_" + pl.col("timeframe") + "_" +
             pl.col("level").cast(pl.Utf8) + "_" +
             pl.col("t0").cast(pl.Utf8)).alias("event_id")
        ])
        .sort(["t0", "level"])
    )
    return events


# =========================
# Outcome tracking (ALL bars_to levels)
# =========================

def compute_event_outcomes(bars: pl.DataFrame, events: pl.DataFrame, cfg: LadderConfig) -> pl.DataFrame:
    bars = bars.sort("timestamp").with_row_count("bar_index")
    idx_map = bars.select(["timestamp", "bar_index"])

    ev = (
        events.join(idx_map, left_on="t0", right_on="timestamp", how="left")
        .rename({"bar_index": "t0_index"})
        .drop("timestamp")
        .drop_nulls(["t0_index"])
    )

    bars_py = bars.select(["high", "low"]).to_dicts()  # indexed by bar_index after row_count
    out_rows: List[Dict[str, Any]] = []

    for e in ev.to_dicts():
        t0i = int(e["t0_index"])
        p0 = float(e["p0"])
        end_i = min(t0i + cfg.max_bars_forward, len(bars_py) - 1)

        # ALL TP times (direction-agnostic): based on abs excursion max(up,down)
        tp_times: List[Optional[int]] = [None] * cfg.k_max

        up_max = 0.0
        down_max = 0.0

        for j in range(t0i, end_i + 1):
            b = bars_py[j]
            up = float(b["high"]) - p0
            dn = p0 - float(b["low"])
            if up > up_max:
                up_max = up
            if dn > down_max:
                down_max = dn

            abs_move = max(up_max, down_max)
            # fill TP1..TPk
            k_reached = int(abs_move // cfg.tp_step)
            k_reached = min(k_reached, cfg.k_max)
            for k in range(1, k_reached + 1):
                idx = k - 1
                if tp_times[idx] is None:
                    tp_times[idx] = j - t0i

        # decide side by bigger excursion
        if up_max >= down_max:
            side = "UP"
            mfe = up_max
            mae = down_max
            # SL ladder = adverse side => down excursion
            sl_times: List[Optional[int]] = [None] * cfg.k_max
            dmax = 0.0
            for j in range(t0i, end_i + 1):
                b = bars_py[j]
                dn = p0 - float(b["low"])
                if dn > dmax:
                    dmax = dn
                k_reached = int(dmax // cfg.sl_step)
                k_reached = min(k_reached, cfg.k_max)
                for k in range(1, k_reached + 1):
                    idx = k - 1
                    if sl_times[idx] is None:
                        sl_times[idx] = j - t0i
        else:
            side = "DOWN"
            mfe = down_max
            mae = up_max
            # SL ladder = adverse side => up excursion
            sl_times = [None] * cfg.k_max
            umax = 0.0
            for j in range(t0i, end_i + 1):
                b = bars_py[j]
                up = float(b["high"]) - p0
                if up > umax:
                    umax = up
                k_reached = int(umax // cfg.sl_step)
                k_reached = min(k_reached, cfg.k_max)
                for k in range(1, k_reached + 1):
                    idx = k - 1
                    if sl_times[idx] is None:
                        sl_times[idx] = j - t0i

        tp_level = min(int(mfe // cfg.tp_step), cfg.k_max)
        sl_level = min(int(mae // cfg.sl_step), cfg.k_max)

        row = {
            **e,
            "side": side,
            "mfe": float(mfe),
            "mae": float(mae),
            "tp_level": int(tp_level),
            "sl_level": int(sl_level),
            "tp_hit_bars": tp_times,  # list[nullable int]
            "sl_hit_bars": sl_times,  # list[nullable int]
            "time_in_event": int(end_i - t0i),
        }
        out_rows.append(row)

    out = pl.from_dicts(out_rows)

    # Baseline outcome class (same as before)
    out = out.with_columns(
        pl.when((pl.col("tp_level") >= 2) & (pl.col("sl_level") <= 1))
          .then(pl.lit("CONTINUATION"))
          .when((pl.col("sl_level") >= 2) & (pl.col("tp_level") <= 1))
          .then(pl.lit("FALSE_BREAK"))
          .when((pl.col("tp_level") <= 1) & (pl.col("sl_level") <= 1))
          .then(pl.lit("STALL"))
          .otherwise(pl.lit("NOISE"))
          .alias("outcome")
    )

    # Convenience: expose fast/normal/slow based on TP1 timing if available
    out = out.with_columns(
        pl.col("tp_hit_bars").list.get(0).alias("bars_to_tp1"),
        pl.col("tp_hit_bars").list.get(1).alias("bars_to_tp2"),
        pl.col("sl_hit_bars").list.get(0).alias("bars_to_sl1"),
    )

    return out


# =========================
# Optional features
# =========================

def compute_event_features(bars: pl.DataFrame, labeled_events: pl.DataFrame, lookback_bars: int = 20) -> pl.DataFrame:
    bars = bars.sort("timestamp").with_row_count("bar_index")
    idx_map = bars.select(["timestamp", "bar_index"])

    ev = (
        labeled_events.join(idx_map, left_on="t0", right_on="timestamp", how="left")
        .rename({"bar_index": "t0_index"})
        .drop("timestamp")
        .drop_nulls(["t0_index"])
    )

    bars_small = bars.select(["open", "high", "low", "close"]).to_dicts()

    out_rows: List[Dict[str, Any]] = []
    for e in ev.to_dicts():
        t0i = int(e["t0_index"])
        level = float(e["level"])
        start = max(0, t0i - lookback_bars)
        seg = bars_small[start:t0i + 1]
        if len(seg) < 2:
            continue

        b0 = seg[-1]
        o, h, l, c = float(b0["open"]), float(b0["high"]), float(b0["low"]), float(b0["close"])
        body = abs(c - o) if abs(c - o) > 1e-12 else 1e-12
        wick = (h - max(o, c)) + (min(o, c) - l)
        wick_body = wick / body

        absret = 0.0
        hl_avg = 0.0
        for i in range(1, len(seg)):
            absret += abs(float(seg[i]["close"]) - float(seg[i - 1]["close"]))
        for b in seg:
            hl_avg += float(b["high"]) - float(b["low"])
        hl_avg /= len(seg)

        out_rows.append({
            "event_id": e["event_id"],
            "symbol": e["symbol"],
            "timeframe": e["timeframe"],
            "date": e["date"],
            "t0": e["t0"],
            "level": level,
            "dist_close_to_level": abs(c - level),
            "wick_body_ratio": wick_body,
            "abs_return_sum_lb": absret,
            "hl_range_avg_lb": hl_avg,
        })

    return pl.from_dicts(out_rows)


# =========================
# CLI
# =========================

def cmd_ingest_raw(args: argparse.Namespace) -> None:
    df = read_csv_ohlcv(Path(args.csv))
    df = add_symbol_tf(df, args.symbol, args.timeframe)
    write_partitioned_parquet(df, Path(args.out_dir) / "raw_bars")

def cmd_build_events(args: argparse.Namespace) -> None:
    raw_dir = Path(args.data_dir) / "raw_bars"
    bars = (
        pl.scan_parquet(str(raw_dir))
        .filter((pl.col("symbol") == args.symbol) & (pl.col("timeframe") == args.timeframe))
        .collect()
        .sort("timestamp")
    )

    cfg = LadderConfig(
        level_step=float(args.level_step),
        tp_step=float(args.tp_step),
        sl_step=float(args.sl_step),
        merge_gap_bars=int(args.merge_gap_bars),
        max_bars_forward=int(args.max_bars_forward),
        k_max=int(args.k_max),
    )

    hits = detect_level_hits(bars, cfg)
    events = merge_hits_into_events(hits, cfg)
    labeled = compute_event_outcomes(bars, events, cfg)

    out_dir = Path(args.data_dir) / "level_events_labeled"
    write_partitioned_parquet(labeled, out_dir)

def cmd_build_features(args: argparse.Namespace) -> None:
    raw_dir = Path(args.data_dir) / "raw_bars"
    event_dir = Path(args.data_dir) / "level_events_labeled"

    bars = (
        pl.scan_parquet(str(raw_dir))
        .filter((pl.col("symbol") == args.symbol) & (pl.col("timeframe") == args.timeframe))
        .collect()
        .sort("timestamp")
    )

    labeled = (
        pl.scan_parquet(str(event_dir))
        .filter((pl.col("symbol") == args.symbol) & (pl.col("timeframe") == args.timeframe))
        .collect()
    )

    feats = compute_event_features(bars, labeled, lookback_bars=int(args.lookback_bars))
    out_dir = Path(args.data_dir) / "event_features"
    write_partitioned_parquet(feats, out_dir)

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("quarterlevel-lab pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("ingest-raw", help="CSV OHLCV -> partitioned parquet raw_bars")
    p1.add_argument("--csv", required=True)
    p1.add_argument("--symbol", required=True)
    p1.add_argument("--timeframe", required=True)  # e.g. M5
    p1.add_argument("--out-dir", default="data")
    p1.set_defaults(func=cmd_ingest_raw)

    p2 = sub.add_parser("build-events", help="Build labeled level events (includes ALL bars_to levels as list)")
    p2.add_argument("--data-dir", default="data")
    p2.add_argument("--symbol", required=True)
    p2.add_argument("--timeframe", required=True)
    p2.add_argument("--level-step", default="0.25")
    p2.add_argument("--tp-step", default="0.125")
    p2.add_argument("--sl-step", default="0.125")
    p2.add_argument("--merge-gap-bars", default="3")
    p2.add_argument("--max-bars-forward", default="80")
    p2.add_argument("--k-max", default="32")  # TP1..TP32, SL1..SL32
    p2.set_defaults(func=cmd_build_events)

    p3 = sub.add_parser("build-features", help="Build basic deterministic features around each event (optional)")
    p3.add_argument("--data-dir", default="data")
    p3.add_argument("--symbol", required=True)
    p3.add_argument("--timeframe", required=True)
    p3.add_argument("--lookback-bars", default="20")
    p3.set_defaults(func=cmd_build_features)

    return p

def main() -> None:
    args = build_arg_parser().parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
