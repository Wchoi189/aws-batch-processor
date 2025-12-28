#!/bin/bash
# Quick verification script for AWS Batch setup

set -e

source aws/config.env

echo "========================================="
echo "AWS Batch Setup Verification"
echo "========================================="
echo ""

# Check job definition
echo "1. Job Definition:"
JOB_DEF=$(aws batch describe-job-definitions \
    --job-definition-name "$BATCH_JOB_DEFINITION" \
    --region "$AWS_REGION" \
    --status ACTIVE \
    --query 'jobDefinitions[0].jobDefinitionName' \
    --output text 2>/dev/null || echo "NOT FOUND")
echo "   $JOB_DEF"

# Check job queue
echo ""
echo "2. Job Queue:"
QUEUE=$(aws batch describe-job-queues \
    --job-queues "$BATCH_JOB_QUEUE" \
    --region "$AWS_REGION" \
    --query 'jobQueues[0].jobQueueName' \
    --output text 2>/dev/null || echo "NOT FOUND")
echo "   $QUEUE"

# Check compute environment
echo ""
echo "3. Compute Environment:"
CE=$(aws batch describe-compute-environments \
    --compute-environments "$BATCH_COMPUTE_ENV" \
    --region "$AWS_REGION" \
    --query 'computeEnvironments[0].computeEnvironmentName' \
    --output text 2>/dev/null || echo "NOT FOUND")
echo "   $CE"

# Check S3 bucket
echo ""
echo "4. S3 Bucket:"
if aws s3 ls "s3://$S3_BUCKET/" &>/dev/null; then
    echo "   ✓ EXISTS: s3://$S3_BUCKET"
else
    echo "   ✗ NOT FOUND"
fi

# Check ECR image
echo ""
echo "5. ECR Image:"
IMAGE=$(aws ecr describe-images \
    --repository-name "$ECR_REPOSITORY" \
    --region "$AWS_REGION" \
    --query 'imageDetails[0].imageTags[0]' \
    --output text 2>/dev/null || echo "NOT FOUND")
echo "   $IMAGE"

echo ""
echo "========================================="
if [ "$JOB_DEF" != "NOT FOUND" ] && [ "$QUEUE" != "NOT FOUND" ] && [ "$CE" != "NOT FOUND" ] && [ "$IMAGE" != "NOT FOUND" ]; then
    echo "✅ All components ready!"
    echo ""
    echo "Next steps:"
    echo "1. Upload sample data to S3:"
    echo "   aws s3 cp data/input/sample_10.parquet s3://$S3_BUCKET/data/processed/sample_10.parquet"
    echo ""
    echo "2. Run a test job via GitHub Actions:"
    echo "   - Go to: https://github.com/Wchoi189/aws-batch-processor/actions"
    echo "   - Click 'Run Batch Job' workflow"
    echo "   - Select dataset: sample_10"
    echo "   - Run workflow"
    echo ""
    echo "Or via AWS CLI:"
    echo "   aws batch submit-job \\"
    echo "     --job-name \"test-sample-10-\$(date +%s)\" \\"
    echo "     --job-definition $BATCH_JOB_DEFINITION \\"
    echo "     --job-queue $BATCH_JOB_QUEUE \\"
    echo "     --region $AWS_REGION \\"
    echo "     --container-overrides '{\"environment\": [{\"name\": \"DATASET_NAME\", \"value\": \"sample_10\"}, {\"name\": \"S3_BUCKET\", \"value\": \"$S3_BUCKET\"}]}'"
else
    echo "⚠️  Some components missing. Run ./setup-aws.sh to create them."
fi
echo "========================================="
