#!/bin/sh
set -e

while true; do
  echo "[$(date)] pharma-raw runner start"
  npm start || echo "[$(date)] pharma-raw runner failed"
  echo "[$(date)] sleeping 300s"
  sleep 300
done

