#!/usr/bin/env bash
# Daily low-peak bucket maintenance: health-check first, then run
# maintain_table_indexes.py only on flagged (hot) buckets, with dldb debug
# progress logs. Intended to be driven by cron:
#
#   0 4 * * * /home/heshan/wt-data-platform-sdk/scripts/ops/scheduled_maintain.sh \
#     >> /home/heshan/wt-data-platform-sdk/maintain_logs/cron.log 2>&1
#
# Usage: scheduled_maintain.sh [table ...]   (default: wind_tunnel_landing)
set -u

REPO_DIR="${REPO_DIR:-$HOME/wt-data-platform-sdk}"
PYTHON_BIN="${PYTHON_BIN:-/root/miniconda3/envs/wt-dldb-v1/bin/python}"
LOG_DIR="${LOG_DIR:-$REPO_DIR/maintain_logs}"
HEALTH_CHECK="$REPO_DIR/scripts/ops/bucket_health_check.py"
MAINTAIN="$REPO_DIR/scripts/ops/maintain_table_indexes.py"

if [ $# -eq 0 ]; then
    set -- wind_tunnel_landing
fi

mkdir -p "$LOG_DIR"

# Load WT_SDK_* environment (db uri, s3 credentials).
if [ -f "$REPO_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$REPO_DIR/.env"
    set +a
fi

# Nightly maintenance tolerates slow S3: huge version-dir LISTs on XSKY can
# exceed lance's per-request default (observed 120s failures on 2026-10-25).
# Only this job gets the raised timeout; interactive writers keep defaults.
export WT_SDK_S3_REQUEST_TIMEOUT="${WT_SDK_S3_REQUEST_TIMEOUT:-300}"

for table in "$@"; do
    ts=$(date +%Y%m%d_%H%M%S)
    log_file="$LOG_DIR/maintain_${table}_${ts}.log"
    echo "=== $(date -Is) health check table=$table ===" | tee -a "$log_file"

    flagged="$("$PYTHON_BIN" "$HEALTH_CHECK" --table "$table" --print-partitions 2>>"$log_file")"
    if [ -z "$flagged" ]; then
        echo "=== $(date -Is) no flagged buckets for $table, nothing to do ===" | tee -a "$log_file"
        continue
    fi
    echo "=== $(date -Is) flagged buckets for $table: $flagged ===" | tee -a "$log_file"

    partition_args=()
    for bucket in $flagged; do
        case "$bucket" in
            ''|*[!0-9]*) ;;  # skip non-numeric noise
            *) partition_args+=(--partition "$bucket") ;;
        esac
    done
    if [ ${#partition_args[@]} -eq 0 ]; then
        echo "=== $(date -Is) flagged output for $table had no valid bucket ids, skipping ===" | tee -a "$log_file"
        continue
    fi

    "$PYTHON_BIN" "$MAINTAIN" \
        --table "$table" \
        "${partition_args[@]}" \
        --dldb-model debug \
        --cleanup-older-than-hours 24 \
        >>"$log_file" 2>&1 &
    maintain_pid=$!

    # Sample peak RSS (VmHWM, kB) while maintain runs; monotonic, so the last
    # sample is the peak. /proc values vanish once the process exits.
    rss_file="$LOG_DIR/.maintain_${table}_${ts}.rss"
    (
        while kill -0 "$maintain_pid" 2>/dev/null; do
            awk '/^VmHWM/ {print $2}' "/proc/$maintain_pid/status" >>"$rss_file" 2>/dev/null
            sleep 30
        done
    ) &
    sampler_pid=$!

    wait "$maintain_pid"
    status=$?
    kill "$sampler_pid" 2>/dev/null
    wait "$sampler_pid" 2>/dev/null

    peak_kb=$(tail -1 "$rss_file" 2>/dev/null)
    rm -f "$rss_file"
    if [ -n "$peak_kb" ]; then
        peak_mib=$((peak_kb / 1024))
        echo "=== $(date -Is) maintain for $table exited with $status, peak RSS: ${peak_mib} MiB, log: $log_file ===" | tee -a "$log_file"
        echo "$(date -Is) table=$table peak_rss_mib=$peak_mib" >>"$LOG_DIR/rss_history.log"
    else
        echo "=== $(date -Is) maintain for $table exited with $status, log: $log_file ===" | tee -a "$log_file"
    fi
done

# Env-config table maintenance (unpartitioned SimpleTable; runs serially after
# the landing loop). Override the profile with ENV_CONFIG_PROFILE=test.
ENV_CONFIG_MAINTAIN="$REPO_DIR/scripts/ops/maintain_env_config_indexes.py"
ENV_CONFIG_PROFILE="${ENV_CONFIG_PROFILE:-production}"
ts=$(date +%Y%m%d_%H%M%S)
env_log_file="$LOG_DIR/maintain_env_config_${ENV_CONFIG_PROFILE}_${ts}.log"
echo "=== $(date -Is) env-config maintain profile=$ENV_CONFIG_PROFILE ===" | tee -a "$env_log_file"
"$PYTHON_BIN" "$ENV_CONFIG_MAINTAIN" \
    --profile "$ENV_CONFIG_PROFILE" \
    --cleanup-older-than-hours 24 \
    >>"$env_log_file" 2>&1
env_status=$?
echo "=== $(date -Is) env-config maintain exited with $env_status, log: $env_log_file ===" | tee -a "$env_log_file"
