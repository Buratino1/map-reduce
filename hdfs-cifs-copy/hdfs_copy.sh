#!/usr/bin/env bash
set -euo pipefail

PID=${1:?Usage: hdfs_copy.sh <pid> [threads] [retries]}
THREADS=${2:-35}
RETRIES=${3:-3}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="${SCRIPT_DIR}/target/hdfs-cifs-copy-1.0.0-fat.jar"
LOG="/Catalyst_archive_data/backup_test/hdfs_copy_${PID}.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') Starting copy for PID=${PID} threads=${THREADS} retries=${RETRIES}" >> "${LOG}"

hadoop jar "${JAR}" --pid "${PID}" --threads "${THREADS}" --retries "${RETRIES}" >> "${LOG}" 2>&1
