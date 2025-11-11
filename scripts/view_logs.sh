#!/bin/bash
# View logs from services

SERVICE=${1:-all}

echo "=========================================="
echo "Viewing logs for: $SERVICE"
echo "=========================================="
echo ""

if [ "$SERVICE" = "all" ]; then
    docker-compose logs -f
else
    docker-compose logs -f $SERVICE
fi
