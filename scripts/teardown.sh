#!/usr/bin/env bash
# Delete everything except the raw archive bucket (DeletionPolicy: Retain - delete it manually if wanted).
set -euo pipefail
PROJECT="${PROJECT:-ticketpulse}"; REGION="${AWS_REGION:-ap-south-1}"
"$(dirname "$0")/stop_pipeline.sh" || true
ARTIFACTS=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$PROJECT-data" \
  --query "Stacks[0].Outputs[?OutputKey=='ArtifactsBucketName'].OutputValue" --output text 2>/dev/null || true)
for s in alerting glue; do
  echo ">> deleting $PROJECT-$s"
  aws cloudformation delete-stack --region "$REGION" --stack-name "$PROJECT-$s"
  aws cloudformation wait stack-delete-complete --region "$REGION" --stack-name "$PROJECT-$s"
done
if [ -n "${ARTIFACTS:-}" ] && [ "$ARTIFACTS" != "None" ]; then
  aws s3 rm --region "$REGION" "s3://$ARTIFACTS" --recursive --quiet || true
fi
for s in data network; do
  echo ">> deleting $PROJECT-$s"
  aws cloudformation delete-stack --region "$REGION" --stack-name "$PROJECT-$s"
  aws cloudformation wait stack-delete-complete --region "$REGION" --stack-name "$PROJECT-$s"
done
echo "Done. Secrets are scheduled for deletion (7-30 day recovery window) by Secrets Manager."
