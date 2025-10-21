#!/bin/bash
set -euo pipefail

echo "[bootstrap] Waiting for NameNode RPC endpoint..."
ready=0
for attempt in $(seq 1 30); do
	if hdfs dfsadmin -safemode get >/tmp/safemode-status 2>&1; then
		ready=1
		cat /tmp/safemode-status
		break
	fi
	echo "[bootstrap] Attempt ${attempt}: namenode not ready yet, retrying..." >&2
	sleep 2
done

if [ "$ready" -ne 1 ]; then
	echo "[bootstrap] Timed out waiting for namenode" >&2
	cat /tmp/safemode-status >&2 || true
	exit 1
fi

echo "[bootstrap] Waiting for safe mode to disengage..."
hdfs dfsadmin -safemode wait

echo "[bootstrap] Ensuring /data/gold/synthetic exists"
hdfs dfs -mkdir -p /data/gold/synthetic

echo "[bootstrap] Setting ownership on /data"
hdfs dfs -chown -R hdfs:hdfs /data

echo "[bootstrap] Done"
