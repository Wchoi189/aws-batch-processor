#!/bin/bash
# Submit AWS Batch jobs to reprocess datasets with Prebuilt Extraction API

set -e

# Load config
if [ -f "aws/config.env" ]; then
    source aws/config.env
else
    echo "ERROR: aws/config.env not found. Please run setup-aws.sh first."
    exit 1
fi

# Default values
BATCH_SIZE=${BATCH_SIZE:-500}
CONCURRENCY=${CONCURRENCY:-3}
JOB_QUEUE=${JOB_QUEUE:-"batch-processor-queue"}
JOB_DEFINITION=${JOB_DEFINITION:-"pseudo-label-processor"}

# Datasets to process
DATASETS=("baseline_val" "baseline_test" "baseline_train")

# Function to submit a job
submit_job() {
    local dataset_name=$1
    local job_name="prebuilt-extraction-${dataset_name}-$(date +%Y%m%d-%H%M%S)"
    
    echo "=========================================="
    echo "Submitting job for: $dataset_name"
    echo "Job name: $job_name"
    echo "=========================================="
    
    aws batch submit-job \
        --job-name "$job_name" \
        --job-definition "$JOB_DEFINITION" \
        --job-queue "$JOB_QUEUE" \
        --container-overrides "{
            \"command\": [
                \"python\", \"-m\", \"src.processor\",
                \"--dataset-name\", \"$dataset_name\",
                \"--api-type\", \"prebuilt-extraction\",
                \"--batch-size\", \"$BATCH_SIZE\",
                \"--concurrency\", \"$CONCURRENCY\"
            ],
            \"environment\": [
                {\"name\": \"S3_BUCKET\", \"value\": \"$S3_BUCKET\"},
                {\"name\": \"DATASET_NAME\", \"value\": \"$dataset_name\"}
            ]
        }" \
        --output json | jq -r '.jobId'
}

# Main execution
echo "=========================================="
echo "AWS Batch Job Submission"
echo "Reprocessing with Prebuilt Extraction API"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  S3 Bucket: $S3_BUCKET"
echo "  Job Queue: $JOB_QUEUE"
echo "  Job Definition: $JOB_DEFINITION"
echo "  Batch Size: $BATCH_SIZE"
echo "  Concurrency: $CONCURRENCY"
echo ""

# Check if specific dataset provided
if [ $# -gt 0 ]; then
    DATASETS=("$@")
fi

# Submit jobs
JOB_IDS=()
for dataset in "${DATASETS[@]}"; do
    job_id=$(submit_job "$dataset")
    JOB_IDS+=("$job_id")
    echo "✓ Submitted job: $job_id for $dataset"
    echo ""
done

echo "=========================================="
echo "All jobs submitted!"
echo "=========================================="
echo ""
echo "Job IDs:"
for job_id in "${JOB_IDS[@]}"; do
    echo "  - $job_id"
done
echo ""
echo "Monitor jobs with:"
echo "  aws batch describe-jobs --jobs ${JOB_IDS[0]} ${JOB_IDS[1]} ${JOB_IDS[2]}"
echo ""
