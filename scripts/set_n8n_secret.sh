#!/usr/bin/env bash
# Point the SQS->n8n forwarder at your n8n instance and print the token to put in n8n.
# usage: ./scripts/set_n8n_secret.sh https://your-n8n.example.com
set -euo pipefail
BASE="${1:?usage: set_n8n_secret.sh <n8n base url, e.g. https://abc.trycloudflare.com>}"
PROJECT="${PROJECT:-ticketpulse}"; REGION="${AWS_REGION:-ap-south-1}"
SECRET_ID="$PROJECT/n8n/webhook"
CURRENT=$(aws secretsmanager get-secret-value --region "$REGION" --secret-id "$SECRET_ID" --query SecretString --output text)
NEW=$(python3 -c '
import json, sys
cfg = json.loads(sys.argv[1]); base = sys.argv[2].rstrip("/")
cfg["alerts_url"] = base + "/webhook/ticketpulse-alerts"
cfg["insights_url"] = base + "/webhook/ticketpulse-insights"
print(json.dumps(cfg))' "$CURRENT" "$BASE")
aws secretsmanager put-secret-value --region "$REGION" --secret-id "$SECRET_ID" --secret-string "$NEW" >/dev/null
python3 -c '
import json, sys
c = json.loads(sys.argv[1])
print("alerts_url  :", c["alerts_url"]); print("insights_url:", c["insights_url"])
print("\nIn n8n create a *Header Auth* credential named \"TicketPulse webhook token\":")
print("  Name :", c["header_name"]); print("  Value:", c["token"])' "$NEW"
echo "(the forwarder caches the secret for up to 5 minutes)"
