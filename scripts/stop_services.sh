#!/bin/bash
# Stop all services

set -e

echo "=========================================="
echo "Stopping Services"
echo "=========================================="
echo ""

docker-compose down

echo ""
echo "✓ All services stopped"
echo ""
echo "To remove all data, run: docker-compose down -v"
