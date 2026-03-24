#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/utils.sh"

log_info "Repository root resolved to: $REPO_ROOT"

cd "$REPO_ROOT"

log_info "Checking prerequisites(Docker and Docker Compose)"

check_command docker
check_command docker-compose || check_command docker compose

log_success "All prerequisites satisfied"


log_info "Creating custom docker network for observability stack"

docker network inspect observability-network >/dev/null 2>&1 || docker network create observability-network


# Start Monitoring Stack (LGTM)

log_info "Starting monitoring stack (Prometheus, Loki, Tempo, Grafana)"

docker compose -f monitoring/compose.yml up -d

log_success "Monitoring stack containers started"


# Start Airflow Stack

log_info "Starting Airflow stack (Builds custom image from Dockerfile)"

docker compose -f airflow/docker-compose.yaml up -d --build

log_success "Airflow stack containers started"

cat <<EOF

${GREEN} All stacks started successfully, verify with 'docker ps' command${NC}

Access points:
- Airflow UI:  http://localhost:8080
- Prometheus:  http://localhost:9090
- Grafana:     http://localhost:3000

EOF