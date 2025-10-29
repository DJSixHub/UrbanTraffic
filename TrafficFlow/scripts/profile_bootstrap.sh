#!/bin/bash
set -euo pipefail

RAW_DIR="${RAW_DATA_DIR:-/opt/raw}"
OUTPUT_PATH="${PROFILE_OUTPUT_PATH:-/opt/profiles/distributions.json}"
JOB_PATH="${PROFILE_JOB_PATH:-/opt/spark-apps/build_producer_profiles.py}"
MIN_OBS="${PROFILE_MIN_OBSERVATIONS:-50}"
SPARK_BIN="${SPARK_SUBMIT_BIN:-/spark/bin/spark-submit}"

log() {
	printf '[profile-bootstrap] %s\n' "$*"
}

if [ ! -d "$RAW_DIR" ]; then
	log "Raw directory $RAW_DIR not found; skipping profile build"
	exit 0
fi

dataset=$(find "$RAW_DIR" -maxdepth 1 -type f -name '*.csv' | sort | head -n 1 || true)
if [ -z "$dataset" ]; then
	log "No CSV dataset detected under $RAW_DIR; keeping existing profiles"
	exit 0
fi

log "Discovered dataset $dataset; generating producer profiles"

tmp_output="${OUTPUT_PATH}.tmp"
rm -f "$tmp_output"

set +e
"$SPARK_BIN" \
	--master "${PROFILE_SPARK_MASTER:-local[*]}" \
	--deploy-mode client \
	"$JOB_PATH" \
	--input-path "file://$dataset" \
	--output-path "$tmp_output" \
	--min-observations "$MIN_OBS"
status=$?
set -e

if [ $status -ne 0 ]; then
	log "spark-submit failed with exit code $status; removing temporary output"
	rm -f "$tmp_output"
	exit 0
fi

if [ ! -s "$tmp_output" ]; then
	log "Profile generation produced empty output; skipping override"
	rm -f "$tmp_output"
	exit 0
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
mv "$tmp_output" "$OUTPUT_PATH"
log "Profile override stored at $OUTPUT_PATH"
exit 0
