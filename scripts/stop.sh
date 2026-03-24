#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/utils.sh"

log_info "Repository root resolved to: $REPO_ROOT"

cd "$REPO_ROOT"

log_info "Stopping Airflow and monitoring stacks"

docker compose -f airflow/docker-compose.yaml down
docker compose -f monitoring/compose.yml down

log_success "All stacks stopped"