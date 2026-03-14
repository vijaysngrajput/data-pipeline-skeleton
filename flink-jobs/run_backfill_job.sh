#!/bin/bash
set -e

# Backfill Job Launcher
# Usage: ./run_backfill_job.sh --backfill-from YYYY-MM-DD --backfill-to YYYY-MM-DD [--strategy all|smart_filtered]

BACKFILL_FROM=""
BACKFILL_TO=""
STRATEGY="smart_filtered"
FLINK_HOME="${FLINK_HOME:-/opt/flink}"
FLINK_JOBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --backfill-from)
            BACKFILL_FROM="$2"
            shift 2
            ;;
        --backfill-to)
            BACKFILL_TO="$2"
            shift 2
            ;;
        --strategy)
            STRATEGY="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$BACKFILL_FROM" ]]; then
    echo "Error: --backfill-from is required"
    echo "Usage: $0 --backfill-from YYYY-MM-DD --backfill-to YYYY-MM-DD [--strategy all|smart_filtered]"
    exit 1
fi

if [[ -z "$BACKFILL_TO" ]]; then
    echo "Error: --backfill-to is required"
    echo "Usage: $0 --backfill-from YYYY-MM-DD --backfill-to YYYY-MM-DD [--strategy all|smart_filtered]"
    exit 1
fi

# Validate date format (YYYY-MM-DD)
if ! [[ "$BACKFILL_FROM" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format for --backfill-from: $BACKFILL_FROM (expected YYYY-MM-DD)"
    exit 1
fi

if ! [[ "$BACKFILL_TO" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format for --backfill-to: $BACKFILL_TO (expected YYYY-MM-DD)"
    exit 1
fi

# Validate strategy
if [[ ! "$STRATEGY" =~ ^(all|smart_filtered)$ ]]; then
    echo "Error: Invalid strategy: $STRATEGY (expected 'all' or 'smart_filtered')"
    exit 1
fi

echo "Starting backfill job..."
echo "  From: $BACKFILL_FROM"
echo "  To: $BACKFILL_TO"
echo "  Strategy: $STRATEGY"
echo ""

# Set environment variables for configuration
export BACKFILL_FROM="$BACKFILL_FROM"
export BACKFILL_TO="$BACKFILL_TO"
export BACKFILL_STRATEGY="$STRATEGY"

# Run backfill job directly with Python (not Flink - this is just a batch processor)
cd "$FLINK_JOBS_DIR"
python3 backfill_job.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Backfill job completed successfully"
    exit 0
else
    echo ""
    echo "✗ Backfill job failed"
    exit 1
fi
