# QuarterLevel Lab (`quarterlevel-lab`)

Event-based trading research pipeline for **fixed price levels (0.25)** with
**deterministic TP/SL ladder labeling** on OHLCV data.

> Focus: ลด drawdown, คัด trade ที่ “ไม่ควรเข้า”, และสร้าง rule ที่ใช้เทรดจริงได้  
> ไม่ใช่ระบบทำนาย Buy/Sell แบบ black-box

---

## Core Idea

- ใช้ **fixed price scale** (0.25 → `.00 .25 .50 .75`)
- มองตลาดเป็น **event-based** ไม่ใช่ bar-by-bar
- Event = ราคา *interaction* กับ price level
- วัดผลลัพธ์ด้วย **TP/SL ladder (0.125 ต่อ step)**
- ทุกอย่าง deterministic, reproducible, ไม่ subjective

ระบบนี้ถูกออกแบบมาเพื่อ:
- วิเคราะห์ behavior รอบ level
- แยก CONTINUATION / FALSE_BREAK / STALL / NOISE
- ใช้เป็นฐานสำหรับ risk filter, position sizing, และ rule-based execution

---

## What This Repo Does (Current State)

✅ Ingest CSV OHLCV (รองรับหลาย format)  
✅ รองรับ CSV แบบ `date,time,open,high,low,close,volume`  
✅ Windows-safe Parquet writer  
✅ Detect price-level events (0.25)  
✅ Label outcome ด้วย TP/SL ladder (0.125)  
✅ เก็บ **bars_to_TP / bars_to_SL ทุกระดับ** (list columns)  
✅ ใช้งานกับ data ขนาดหลายปีได้จริง

---

## Project Structure (Minimal)

```

quarterlevel-lab/
├─ pipeline.py
├─ README.md
├─ requirements.txt
├─ .gitignore
└─ data/                  # not committed
    ├─ raw_bars/
    ├─ level_events_labeled/
    └─ event_features/

````

---

## Installation

```bash
pip install -r requirements.txt
````

Dependencies (หลัก):

* polars
* pyarrow

---

## Input CSV Formats

### 1) CSV with header + timestamp

```csv
timestamp,open,high,low,close,volume
2020-01-02 01:00:00,61.391,61.466,61.346,61.406,125
```

### 2) CSV with header + date,time

```csv
date,time,open,high,low,close,volume
2020.01.02,01:00:00,61.391,61.466,61.346,61.406,125
```

### 3) CSV without header (date,time,...)

```csv
2020.01.02,01:00:00,61.391,61.466,61.346,61.406,125
```

ใช้ flag `--no-header`

---

## Usage

### 1) Ingest raw data

#### Case A: มี header

```bash
python pipeline.py ingest-raw \
  --csv path/to/MCL_M5.csv \
  --symbol MCL \
  --timeframe M5 \
  --out-dir data
```

#### Case B: ไม่มี header (date,time,...)

```bash
python pipeline.py ingest-raw \
  --csv "data/US_Light_Crude_Oil_GMT+2_US-DST_M5.csv" \
  --no-header \
  --symbol MCL \
  --timeframe M5 \
  --out-dir data
```

ผลลัพธ์:

```
data/raw_bars/symbol=MCL/timeframe=M5/date=YYYY-MM-DD/part-000.parquet
```

---

### 2) Build labeled events

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

ผลลัพธ์:

```
data/level_events_labeled/...
```

---

### 3) (Optional) Build features

```bash
python pipeline.py build-features \
  --data-dir data \
  --symbol MCL \
  --timeframe M5 \
  --lookback-bars 20
```

---

## Dataset Overview

### `raw_bars`

OHLCV ดิบ (immutable)

Columns:

* timestamp (Datetime)
* open, high, low, close (Float64)
* volume (Int64)
* date (Date)
* symbol, timeframe

---

### `level_events_labeled`

หัวใจของโปรเจค

Key columns:

* event_id
* t0, t_last
* level / p0
* side (UP / DOWN)
* mfe / mae
* tp_level / sl_level
* **tp_hit_bars**: List[Int] → TP1..TPk
* **sl_hit_bars**: List[Int] → SL1..SLk (adverse)
* bars_to_tp1 / bars_to_tp2 / bars_to_sl1
* time_in_event
* outcome:

  * CONTINUATION
  * FALSE_BREAK
  * STALL
  * NOISE

**ความหมายของ tp_hit_bars**

* index 0 = TP1 (0.125)
* index 1 = TP2 (0.25)
* value = จำนวน bars หลัง t0 ที่แตะครั้งแรก
* null = ไม่เคยแตะภายใน horizon

---

## Default Parameters (M5)

* level_step = 0.25
* tp_step / sl_step = 0.125
* max_bars_forward = 80  (~6h40m)
* k_max = 32  (coverage ~4.0 dollars)

---

## Philosophy

* ไม่ optimize เพื่อ win rate
* ไม่ fit model ให้สวย
* เน้น **อยู่รอด + ลด DD**
* ทุก rule ต้องอธิบายได้ และเอาไปใช้เทรดจริงได้

---

## Status

* v0.1.x: data pipeline + labeling stable
* Next:

  * outcome analysis
  * risk filter rules
  * walk-forward / stress test
  * integration กับ execution layer

---



