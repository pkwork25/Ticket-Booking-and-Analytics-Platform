#!/usr/bin/env bash
# Start the Glue streaming job (billed per hour while running - stop it when you're done).
set -euo pipefail
PROJECT="${PROJECT:-ticketpulse}"; REGION="${AWS_REGION:-ap-south-1}"
aws glue start-job-run --region "$REGION" --job-name "$PROJECT-streaming-etl" --query JobRunId --output text
echo "Logs: CloudWatch -> /aws-glue/jobs/output (continuous logging enabled)"
