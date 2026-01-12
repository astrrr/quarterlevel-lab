# QuarterLevel Lab (`quarterlevel-lab`)

Event-based trading research pipeline on retail OHLCV data using:

- Fixed price levels: **0.25** (`.00 .25 .50 .75`)
- TP/SL ladder step: **0.125**
- Deterministic labeling (no subjective pattern names)
- Partitioned Parquet datasets (`symbol/timeframe/date`) for append/backfill/research

---

## Why

ผมไม่ได้ทำเพื่อทาย Buy/Sell ให้แม่นขึ้น
แต่ทำเพื่อ “คัดไม้พัง” ลด DD และอยู่รอดใน prop firm

Key idea:
- Event = ราคา **interaction กับ fixed level**
- วัด outcome แบบ **TP/SL ladder**
- เก็บ MAE/MFE + เวลา + path information (`bars_to_*`) เพื่อใช้ทำ risk model / filter

---

## Datasets

### 1) `data/raw_bars`
OHLCV ดิบ (immutable)

Partition:
- `symbol=.../timeframe=.../date=YYYY-MM-DD/part-000.parquet`

Columns (ขั้นต่ำ):
- timestamp (Datetime)
- open, high, low, close (Float64)
- volume (Int64)
- symbol (Utf8)
- timeframe (Utf8)
- date (Date)

---

### 2) `data/level_events_labeled`
Event ที่ราคา hit level 0.25 แล้ว track ต่อไปข้างหน้า

สำคัญสุด: เก็บ `bars_to_*` “ครบทุกระดับ” เป็น list columns

Key columns:
- event_id
- t0, t_last
- level (Float64)
- p0 (= level)
- side: UP/DOWN (กำหนดจากฝั่งที่ MFE ใหญ่กว่า)
- mfe, mae
- tp_level, sl_level
- tp_hit_bars: **List[Int64?]**  (TP1..TPk_max)
- sl_hit_bars: **List[Int64?]**  (SL1..SLk_max, วัดฝั่ง adverse ต่อ side)
- bars_to_tp1, bars_to_tp2, bars_to_sl1 (convenience)
- time_in_event
- outcome: CONTINUATION / FALSE_BREAK / STALL / NOISE

#### Meaning of `tp_hit_bars` / `sl_hit_bars`
- index 0 = level 1 (0.125)
- index 1 = level 2 (0.250)
- ...
- value = จำนวน bars หลัง t0 ที่ “แตะครั้งแรก”
- null = ไม่เคยแตะภายใน horizon

---

### 3) `data/event_features` (optional)
Feature deterministic รอบ event (baseline)
- dist_close_to_level
- wick_body_ratio
- abs_return_sum_lb
- hl_range_avg_lb

---

## How it works (high-level)

1. Ingest raw OHLCV -> partitioned Parquet
2. Detect bars that cross any 0.25 level -> hits
3. Merge hits close in time into events
4. For each event:
   - track forward up to `max_bars_forward`
   - compute MFE/MAE
   - compute `tp_hit_bars` (ALL levels up to k_max)
   - compute `sl_hit_bars` (ALL levels up to k_max, adverse side)
   - derive `tp_level/sl_level` and `outcome`

---

## Install

```bash
pip install -r requirements.txt
````

`requirements.txt`:

* polars
* pyarrow

---

## Usage

### 1) Ingest raw

```bash
python pipeline.py ingest-raw \
  --csv mcl_m5.csv \
  --symbol MCL \
  --timeframe M5 \
  --out-dir data
```

### 2) Build labeled events (ALL bars_to levels)

```bash
python pipeline.py build-events \
  --data-dir data \
  --symbol MCL \
  --timeframe M5 \
  --level-step 0.25 \
  --tp-step 0.125 \
  --sl-step 0.125 \
  --merge-gap-bars 3 \
  --max-bars-forward 80 \
  --k-max 32
```

### 3) Build features (optional)

```bash
python pipeline.py build-features \
  --data-dir data \
  --symbol MCL \
  --timeframe M5 \
  --lookback-bars 20
```

---

## Notes / Defaults (M5)

* `max-bars-forward=80` ≈ 6h40m
* `k-max=32` => 0.125 * 32 = 4.0 dollars ladder coverage

ถ้าอยากละเอียดกว่า:

* เพิ่ม `k-max` (เช่น 64) แต่ list column จะยาวขึ้นตามนั้น

---
