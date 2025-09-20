#!/usr/bin/env bash
#
# This script builds and runs a 5-node sboxdb cluster
# listening on ports 8811-8815(raft) and 8911-8915(sql).
#
# To connect a sboxsql client to node 5 on port 8915, run:
#
# cargo run --release --bin sboxsql -- -a localhost:8915

set -euo pipefail

# Change into the script directory.
cd "$(dirname $0)"

# Build sboxdb using release optimizations.
cargo build --release --bin sboxdb

# Start nodes 1-5 in the background, prefixing their output with the node ID.
echo "Starting 5 nodes on ports 8811-8815. To connect to node 5, run:"
echo "cargo run --release --bin sboxsql -- -a localhost:8911"
echo ""

for ID in 1 2 3 4 5; do
    (cargo run -q --release --bin sboxdb -- -c sboxdb$ID/sboxdb.yaml 2>&1 | sed -e "s/\\(.*\\)/sboxdb$ID \\1/g") &
done

# Wait for the background processes to exit. Kill all sboxdb processes when the
# script exits (e.g. via Ctrl-C).
trap 'kill $(jobs -p)' EXIT
wait < <(jobs -p)