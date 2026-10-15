#!/usr/bin/env bash
# Stop all running Glue streaming job runs (the S3 checkpoint lets the next run resume).
set -euo pipefail
PROJECT="${PROJECT:-ticketpulse}"; REGION="${AWS_REGION:-ap-south-1}"
JOB="$PROJECT-streaming-etl"
RUNS=$(aws glue get-job-runs --region "$REGION" --job-name "$JOB" \
  --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING'].Id" --output text)
if [ -z "$RUNS" ]; then echo "no running runs"; exit 0; fi
aws glue batch-stop-job-run --region "$REGION" --job-name "$JOB" --job-run-ids $RUNS
