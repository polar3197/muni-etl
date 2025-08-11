#!/bin/bash

WORKERS=(quentin)
LOCAL_PROJECT_DIR="$HOME/transit-etl/spark-cluster"
REMOTE_PROJECT_DIR="~/transit-etl/spark-cluster"

ACTION="$1"

if [[ "$ACTION" == "-up" ]]; then
  
  for WORKER in "${WORKERS[@]}"; do
    echo "Starting Spark worker on $WORKER..."
    ssh "$WORKER" "cd $REMOTE_PROJECT_DIR && docker compose up -d --build spark-worker"
  done

  echo "Starting Spark master on Caddy..."
  cd "$LOCAL_PROJECT_DIR" || exit 1
  docker compose up -d --build spark-master

  echo "Spark cluster started."

elif [[ "$ACTION" == "-down" ]]; then
  echo "Stopping Spark master on Caddy..."
  cd "$LOCAL_PROJECT_DIR" || exit 1
  docker compose down

  for WORKER in "${WORKERS[@]}"; do
    echo "Stopping Spark worker on $WORKER..."
    ssh "$WORKER" "cd $REMOTE_PROJECT_DIR && docker compose down"
  done

  echo "Spark cluster stopped."

else
  echo "Usage: $0 -up | -down"
  exit 1
fi