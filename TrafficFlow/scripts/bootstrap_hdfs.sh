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

echo "[bootstrap] Ensuring pipeline directories exist"
hdfs dfs -mkdir -p /data/bronze/staging
hdfs dfs -mkdir -p /data/silver/regions
hdfs dfs -mkdir -p /data/gold/regions/groups
hdfs dfs -mkdir -p /data/gold/management
hdfs dfs -mkdir -p /data/checkpoints
hdfs dfs -mkdir -p /data/gold/synthetic
hdfs dfs -mkdir -p /data/trafficflow/spool
hdfs dfs -mkdir -p /data/trafficflow/spool_backup
hdfs dfs -mkdir -p /data/silver/regions_failover_primary
hdfs dfs -mkdir -p /data/silver/regions_failover_backup
hdfs dfs -mkdir -p /data/gold/management/primary_failover
hdfs dfs -mkdir -p /data/gold/management/backup_failover

echo "[bootstrap] Setting ownership on /data"
hdfs dfs -chown -R hdfs:hdfs /data
hdfs dfs -chown -R hdfs:hdfs /data/trafficflow

echo "[bootstrap] Done"
