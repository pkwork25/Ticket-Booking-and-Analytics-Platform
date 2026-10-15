#!/usr/bin/env bash
# Deploy the whole stack: network -> data -> (upload code) -> glue -> alerting -> Redshift DDL.
# usage: PROJECT=ticketpulse AWS_REGION=ap-south-1 EGRESS_MODE=NAT OPS_EMAIL=you@example.com ./scripts/deploy.sh
set -euo pipefail

PROJECT="${PROJECT:-ticketpulse}"
REGION="${AWS_REGION:-ap-south-1}"
EGRESS_MODE="${EGRESS_MODE:-NAT}"
OPS_EMAIL="${OPS_EMAIL:-}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"

cfn_deploy() {  # stack-suffix template [param overrides...]
  local stack="$PROJECT-$1" template="$2"; shift 2
  echo ">> deploying $stack"
  aws cloudformation deploy --region "$REGION" --stack-name "$stack" \
    --template-file "$ROOT/infra/$template" --capabilities CAPABILITY_NAMED_IAM \
    --no-fail-on-empty-changeset --parameter-overrides ProjectName="$PROJECT" "$@"
}
output() {  # stack-suffix output-key
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$PROJECT-$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" --output text
}

cfn_deploy network 01-network.yaml EgressMode="$EGRESS_MODE"
cfn_deploy data 02-data.yaml

ARTIFACTS="$(output data ArtifactsBucketName)"
echo ">> packaging code -> s3://$ARTIFACTS"
"$ROOT/scripts/package.sh"
aws s3 cp --region "$REGION" "$ROOT/glue/streaming_job.py" "s3://$ARTIFACTS/glue/streaming_job.py"
aws s3 cp --region "$REGION" "$BUILD/ticket_etl.zip" "s3://$ARTIFACTS/glue/ticket_etl.zip"
aws s3 cp --region "$REGION" "$BUILD/sqs_to_n8n.zip" "s3://$ARTIFACTS/lambda/sqs_to_n8n.zip"
aws s3 cp --region "$REGION" "$BUILD/kpi_snapshot.zip" "s3://$ARTIFACTS/lambda/kpi_snapshot.zip"

cfn_deploy glue 03-glue-streaming.yaml
cfn_deploy alerting 04-alerting.yaml OpsEmail="$OPS_EMAIL"

# Lambda code lives in S3: force a refresh on redeploys (CloudFormation only sees the same key).
for fn in sqs-to-n8n:sqs_to_n8n kpi-snapshot:kpi_snapshot; do
  aws lambda update-function-code --region "$REGION" --function-name "$PROJECT-${fn%%:*}" \
    --s3-bucket "$ARTIFACTS" --s3-key "lambda/${fn##*:}.zip" >/dev/null
done

echo ">> creating Redshift schema + views (Data API)"
python3 "$ROOT/scripts/init_redshift.py" --region "$REGION" \
  --workgroup "$(output data RedshiftWorkgroupName)" \
  --database "$(output data RedshiftDbName)" \
  --secret-arn "$(output data RedshiftSecretArn)"

cat <<EOF

Deployed. Next:
  1. ./scripts/start_pipeline.sh                         # start the Glue streaming job
  2. python producer/send_to_kinesis.py --stream $(output data StreamName) --region $REGION --rate 5
  3. Import n8n/workflows/*.json, then ./scripts/set_n8n_secret.sh <public-n8n-base-url>
EOF
