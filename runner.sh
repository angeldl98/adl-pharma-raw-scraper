#!/bin/sh
set -e

echo "[$(date)] pharma-raw runner start"
npm start || echo "[$(date)] pharma-raw runner failed"
echo "[$(date)] pharma-raw runner end"

