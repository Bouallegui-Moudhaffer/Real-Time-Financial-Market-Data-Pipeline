#!/usr/bin/env bash
set -euo pipefail
# Step 5 will publish these events into Kafka. For now, print a count.
DATA="tests/data/trades.jsonl"
echo "Sample trades lines: $(wc -l < ${DATA})"
