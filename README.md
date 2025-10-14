# 🚕 Ride Sharing Analytics Using Spark Streaming and Spark SQL

A real-time data analytics pipeline built with **Apache Spark Structured Streaming**, leveraging a live synthetic data feed generated over a socket (`localhost:9999`).

---

## Prerequisites

- **Python 3.x**
  ```bash
  python3 --version
  ```

- **PySpark**
  ```bash
  pip install pyspark==3.5.1
  ```

- **Faker** (for synthetic data generation)
  ```bash
  pip install faker
  ```

- **Java 8+** (required for running Spark)
  ```bash
  java -version
  ```

---

## Project Structure

The following structure represents the organization within your Codespace:

```
HandsOn9/
├── data_generator.py
├── task1.py
├── task2.py
├── task3.py
├── outputs/
│   ├── task1/
│   │   ├── row_0_<uuid>/part-00000-....csv
│   │   ├── row_1_<uuid>/part-00000-....csv
│   │   └── ...
│   ├── task2/
│   │   ├── batch_0/part-00000-....csv
│   │   ├── batch_1/part-00000-....csv
│   │   └── ...
│   └── task3/
│       └── batch_85/part-00000-....csv
└── README.md
```

> **Note:** The `checkpoints/` directory is used by Spark for maintaining stream state and should **not** be committed to version control.

---

## ▶Running the Project

Open **two terminals** simultaneously — one for generating the data stream and the other for executing a task.

### 1. Start the Data Generator (Terminal 1)
```bash
python data_generator.py
# Continuously streams JSON-formatted ride data to localhost:9999
```

### 2. Run One Task at a Time (Terminal 2)
```bash
# Task 1: Stream ingestion & parsing (writes one CSV per row)
python task1.py

# Task 2: Real-time aggregations (writes one CSV per micro-batch)
python task2.py

# Task 3: Windowed aggregations (5-min window, 1-min slide, 1-min watermark)
python task3.py
```

> **Tip:** Task 3 needs a few minutes to accumulate enough data for the sliding windows to appear in outputs.

---

## Project Goals

This project demonstrates how to build a streaming analytics pipeline for a ride-sharing platform.

**Objectives by Task:**

- **Task 1:** Ingest JSON events and extract structured columns.
- **Task 2:** Perform real-time aggregations grouped by driver.
- **Task 3:** Compute time-windowed metrics with watermarking for late data handling.

---

## Task 1 — Streaming Ingestion and Parsing

**Purpose:**  
Read streaming data from `localhost:9999`, parse JSON, and store each record in an individual CSV file.

**Key Steps:**
- Use `spark.readStream.format("socket")` to consume live data.
- Parse JSON into columns:
  ```
  (trip_id, driver_id, distance_km, fare_amount, timestamp)
  ```
- Each record is written to a unique folder under `outputs/task1/row_*`.

**Example Output:**

From `outputs/task1/row_1_<uuid>/part-00000-....csv`:
```
distance_km,driver_id,fare_amount,timestamp,trip_id
31.78,55,116.49,2025-10-14 23:26:39,d738e0d8-b46a-4350-b1ef-090fcfd079be
```

---

## Task 2 — Real-Time Aggregations (Driver-Level)

**Goal:**  
Aggregate rides per driver in real time.

**Aggregations Performed:**
- `SUM(fare_amount)` → `total_fare`
- `AVG(distance_km)` → `avg_distance`
- Grouped by `driver_id`

**Implementation Details:**
- Convert timestamps to `TimestampType`.
- Use `outputMode("complete")` for full driver summaries.
- Write each micro-batch as a separate CSV in `outputs/task2/batch_*`.

**Example Output (from batch_1):**
```
driver_id,total_fare,avg_distance
26,46.52,36.82
22,27.47,10.46
16,20.17,23.75
94,130.82,6.89
64,125.7,23.17
43,188.76999999999998,8.585
61,58.18,24.62
88,99.42,18.44
17,13.68,29.57
59,43.81,33.54
4,17.17,35.86
7,190.7,17.755
84,87.26,6.45
97,139.85,12.85
45,112.8,42.67
82,124.53,39.15
32,108.42,30.89
58,115.7,48.2
11,58.04,26.9
2,108.34,27.46
66,105.71,4.84
74,139.31,24.6
```

---

## Task 3 — Time-Based Windowed Analytics

**Purpose:**  
Analyze total fare trends using event-time windows.

**Window Configuration:**
- **Watermark:** 1 minute
- **Window Duration:** 5 minutes
- **Slide Interval:** 1 minute
- Aggregation: `SUM(fare_amount)` → `sum_fare`

**Implementation Highlights:**
- Convert timestamps to event-time.
- Use `groupBy(window(event_time, "5 minutes", "1 minute"))`.
- Output window start and end times with total fare per window.

**Example Output (from batch_85):**
```
window_start,window_end,sum_fare
2025-10-14T23:31:00.000Z,2025-10-14T23:36:00.000Z,1713.8000000000002
```

---

## Checking Outputs

List all generated files:
```bash
ls -R outputs
```

Preview content samples:
```bash
head outputs/task1/*/part-*.csv | sed -n '1,5p'
head outputs/task2/batch_*/part-*.csv | sed -n '1,10p'
head outputs/task3/batch_*/part-*.csv | sed -n '1,5p'
```

---

## Cleaning Up

If you modify schemas or output modes and encounter checkpoint errors, stop your job and clear the affected task state:

```bash
rm -rf checkpoints/task1 checkpoints/task2 checkpoints/task3
```

To fully regenerate results:
```bash
rm -rf outputs/task1 outputs/task2 outputs/task3
```

---
