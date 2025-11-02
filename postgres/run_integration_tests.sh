#!/bin/bash
# Script to run integration tests with testcontainers
# Requires Docker to be running

set -e

echo "Running integration tests with PostgreSQL 18 container..."
echo "Note: This requires Docker to be running"
echo ""

go test -tags=integration -v ./postgres/...

echo ""
echo "Integration tests completed successfully!"
