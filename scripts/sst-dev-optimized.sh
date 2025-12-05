#!/bin/bash
# Auto-optimize SST dev mode based on system resources
# Usage: source scripts/sst-dev-optimized.sh && sst dev
#    or: ./scripts/sst-dev-optimized.sh sst dev

set -e

# Detect OS
OS="$(uname -s)"

# Get total RAM in GB
get_ram_gb() {
  case "$OS" in
    Darwin)
      # macOS
      sysctl -n hw.memsize | awk '{print int($1/1024/1024/1024)}'
      ;;
    Linux)
      # Linux
      grep MemTotal /proc/meminfo | awk '{print int($2/1024/1024)}'
      ;;
    *)
      echo "16" # Default fallback
      ;;
  esac
}

# Get CPU core count
get_cpu_cores() {
  case "$OS" in
    Darwin)
      # macOS - get performance cores for M1/M2/M3
      sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || sysctl -n hw.physicalcpu
      ;;
    Linux)
      nproc
      ;;
    *)
      echo "4" # Default fallback
      ;;
  esac
}

RAM_GB=$(get_ram_gb)
CPU_CORES=$(get_cpu_cores)

# Calculate optimal settings based on RAM
if [ "$RAM_GB" -ge 64 ]; then
  # 64GB+ RAM (M1 Max, M2 Max, etc.)
  POOL_SIZE=20
  IDLE_TIMEOUT=120000
elif [ "$RAM_GB" -ge 32 ]; then
  # 32-64GB RAM (M1 Pro, M2 Pro, etc.)
  POOL_SIZE=15
  IDLE_TIMEOUT=90000
elif [ "$RAM_GB" -ge 16 ]; then
  # 16-32GB RAM (M1, M2, etc.)
  POOL_SIZE=10
  IDLE_TIMEOUT=60000
else
  # <16GB RAM
  POOL_SIZE=5
  IDLE_TIMEOUT=30000
fi

# Build concurrency based on CPU cores (leave 2 cores for system)
BUILD_CONCURRENCY=$((CPU_CORES > 2 ? CPU_CORES - 2 : CPU_CORES))

# Export environment variables
export SST_WORKER_POOL_SIZE="$POOL_SIZE"
export SST_WORKER_IDLE_TIMEOUT="$IDLE_TIMEOUT"
export SST_BUILD_CONCURRENCY="$BUILD_CONCURRENCY"

echo "🚀 SST Dev Mode Optimized for your system:"
echo "   RAM: ${RAM_GB}GB | CPU Cores: ${CPU_CORES}"
echo "   SST_WORKER_POOL_SIZE=$POOL_SIZE"
echo "   SST_WORKER_IDLE_TIMEOUT=${IDLE_TIMEOUT}ms"
echo "   SST_BUILD_CONCURRENCY=$BUILD_CONCURRENCY"
echo ""

# If arguments passed, run them with the env vars
if [ $# -gt 0 ]; then
  exec "$@"
fi
