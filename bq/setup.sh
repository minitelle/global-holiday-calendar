#!/usr/bin/env bash
# bq/setup.sh
# ─────────────────────────────────────────────────────────────────────────────
# BigQuery setup for global-holiday-calendar holiday calendar.
# Run once per environment (dev / prod). Safe to re-run — uses --replace.
#
# Prerequisites:
#   gcloud auth login (or service account key in GOOGLE_APPLICATION_CREDENTIALS)
#   bq CLI installed
#   Parquet files already generated: cd scripts && python generate_holidays.py
#
# Usage:
#   PROJECT=my-gcp-project BUCKET=my-ref-data-bucket bash bq/setup.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

PROJECT="${PROJECT:-your-gcp-project}"
BUCKET="${BUCKET:-your-gcs-bucket}"
DATASET="${DATASET:-ref_data_raw}"
TABLE="${TABLE:-holidays}"
LOCATION="${LOCATION:-US}"                  # change to australia-southeast1 for AU
PARQUET_DIR="${PARQUET_DIR:-../data/holidays}"
GCS_PREFIX="gs://${BUCKET}/ref_data/holidays"

echo "── Step 1: Create dataset if it does not exist ──────────────────────────"
bq --project_id="${PROJECT}" mk \
   --dataset \
   --location="${LOCATION}" \
   --description="Global holiday calendar — holiday + commerce calendar" \
   --default_table_expiration=0 \
   "${PROJECT}:${DATASET}" 2>/dev/null || echo "Dataset already exists, continuing."

echo "── Step 2: Upload Parquet files to GCS ──────────────────────────────────"
gsutil -m rsync -r -d "${PARQUET_DIR}/" "${GCS_PREFIX}/"

echo "── Step 3: Load into BigQuery native table ───────────────────────────────"
# Using a native table (not external) for best Sigma and dbt performance.
# Hive-partitioned source (year=YYYY/data.parquet).
bq --project_id="${PROJECT}" load \
   --replace \
   --source_format=PARQUET \
   --hive_partitioning_mode=AUTO \
   --hive_partitioning_source_uri_prefix="${GCS_PREFIX}/{year:INTEGER}" \
   "${DATASET}.${TABLE}" \
   "${GCS_PREFIX}/year=*/data.parquet"

echo "── Step 4: Verify row count ──────────────────────────────────────────────"
bq --project_id="${PROJECT}" query \
   --use_legacy_sql=false \
   "SELECT COUNT(*) as total_rows,
           COUNT(DISTINCT country_code) as countries,
           MIN(calendar_date) as min_date,
           MAX(calendar_date) as max_date
    FROM \`${PROJECT}.${DATASET}.${TABLE}\`"

echo ""
echo "Done. Set dbt var 'ref_data_raw_schema' to '${DATASET}' and run dbt build."
echo "Table: ${PROJECT}.${DATASET}.${TABLE}"
