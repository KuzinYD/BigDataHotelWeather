# Big Data Restaurant-Weather ETL

## Overview

This project implements an **ETL pipeline using PySpark** that enriches restaurant data with weather information. The main steps include:

1. Reading restaurant and weather datasets from local storage.
2. Detecting missing latitude and longitude values in the restaurant dataset.
3. Resolving missing coordinates via the OpenCage Geocoding API.
4. Generating 4-character geohashes for restaurants and weather points.
5. Left-joining restaurant and weather data on geohash, avoiding row multiplication.
6. Writing the enriched data in **Parquet format**, partitioned by geohash.

---

## Prerequisites

- Python 3.10+
- PySpark installed (`pip install pyspark`)
- `pygeohash` installed (`pip install pygeohash`)
- OpenCage API key (replace in `main.py`)
- Local datasets:
  - `restaurant_csv/` (CSV files)
  - `artifacts/weather` (Parquet partitions)

---

## How to Run

```bash
# Activate your virtual environment if needed
source .venv/bin/activate

# Run the ETL
python main.py
```

## Screenshot

![Results](/docs/output.png)
