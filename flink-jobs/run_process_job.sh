#!/usr/bin/env bash
set -euo pipefail

JOB_PY="${JOB_PY:-/workspace/flink-jobs/kafka_consumer.py}"
PYFS_PATH="${PYFS_PATH:-/workspace/flink-jobs}"
CHECKPOINT_STORAGE_DIR="${CHECKPOINT_STORAGE_DIR:-file:///workspace/flink-checkpoints}"
FORCE_FRESH_START="${FORCE_FRESH_START:-false}"

checkpoint_path="${CHECKPOINT_STORAGE_DIR}"
if [[ "${checkpoint_path}" == file://* ]]; then
  checkpoint_path="${checkpoint_path#file://}"
fi

find_latest_checkpoint_metadata() {
  if [[ ! -d "${checkpoint_path}" ]]; then
    return 1
  fi
  find "${checkpoint_path}" -type f -path '*/chk-*/_metadata' -printf '%T@ %p\n' \
    | sort -nr \
    | head -n1 \
    | cut -d' ' -f2-
}

run_with_restore() {
  local restore_dir="$1"
  echo "Starting Flink job from checkpoint: ${restore_dir}"
  flink run \
    -s "${restore_dir}" \
    -py "${JOB_PY}" \
    -pyfs "${PYFS_PATH}"
}

run_fresh() {
  echo "Starting Flink job without restore checkpoint."
  flink run \
    -py "${JOB_PY}" \
    -pyfs "${PYFS_PATH}"
}

if [[ "${FORCE_FRESH_START}" == "true" ]]; then
  run_fresh
  exit 0
fi

latest_metadata="$(find_latest_checkpoint_metadata || true)"
if [[ -n "${latest_metadata}" ]]; then
  restore_dir="${latest_metadata%/_metadata}"
  run_with_restore "${restore_dir}"
else
  run_fresh
fi
