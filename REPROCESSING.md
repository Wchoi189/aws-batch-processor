# Reprocessing with Prebuilt Extraction API

## Overview

This document describes how to reprocess datasets using the Prebuilt Extraction API, which provides better quality results for receipt/invoice documents.

## Output Compatibility

**Important**: The outputs from Document Parse and Prebuilt Extraction are **NOT fully interchangeable**:

- **Document Parse**: Provides polygon coordinates for text bounding boxes
- **Prebuilt Extraction**: Does NOT provide polygon coordinates (only structured field data)

Both produce compatible `texts` and `labels` arrays, but Prebuilt Extraction will have empty polygons.

## Backup

Existing output files have been backed up to `data/backup/`:
- `baseline_train_pseudo_labels.parquet`
- `baseline_val_pseudo_labels.parquet`
- `baseline_test_pseudo_labels.parquet`
- `pseudo_labels_worst_performers_pseudo_labels.parquet`

## Deployment Options

### Option 1: AWS Batch (Recommended for Production)

AWS Batch has S3 access and can process large datasets efficiently.

#### 1. Deploy Updated Code

```bash
# Commit and push changes
git add .
git commit -m "Add Prebuilt Extraction API support"
git push origin main
```

GitHub Actions will automatically:
- Build Docker image
- Push to ECR
- Update job definition

#### 2. Submit Batch Jobs

```bash
# Load AWS config
source aws/config.env

# Submit jobs for all datasets
./scripts/submit_prebuilt_extraction_jobs.sh

# Or submit for specific dataset
./scripts/submit_prebuilt_extraction_jobs.sh baseline_val
```

#### 3. Monitor Jobs

```bash
# List running jobs
aws batch list-jobs --job-queue batch-processor-queue --job-status RUNNING

# Check specific job
aws batch describe-jobs --jobs JOB_ID

# View logs
aws logs tail /aws/batch/job --log-stream-name LOG_STREAM_NAME --follow
```

### Option 2: Local Processing (Requires S3 Access)

If you have AWS credentials configured locally:

```bash
# Set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export S3_BUCKET=ocr-batch-processing
export UPSTAGE_API_KEY=your_api_key

# Reprocess with uv
/opt/uv/bin/uv run python3 scripts/reprocess_with_prebuilt_extraction.py baseline_val
```

**Note**: Local processing requires S3 access to download images.

## Datasets to Reprocess

1. **baseline_val** (404 images) - Validation set
2. **baseline_test** (413 images) - Test set  
3. **baseline_train** (3,272 images) - Training set (largest)

## Expected Output

Prebuilt Extraction output will have:
- **polygons**: Empty arrays (no coordinates available)
- **texts**: Extracted text from receipt fields
- **labels**: Field names (e.g., "store_name", "charged_price")
- **metadata**: `{"source": "upstage_api_prebuilt_extraction", "api_type": "prebuilt-extraction"}`

## Verification

After reprocessing, verify the output:

```bash
python3 << 'EOF'
import pandas as pd

df = pd.read_parquet('data/output/baseline_val_pseudo_labels.parquet')
print(f"Rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

sample = df.iloc[0]
print(f"\nSample metadata: {sample['metadata']}")
print(f"Texts count: {len(sample['texts'])}")
print(f"Labels count: {len(sample['labels'])}")
print(f"Polygons count: {len(sample['polygons'])}")
print(f"Empty polygons: {sum(1 for p in sample['polygons'] if not p)}")
EOF
```

## Comparison

To compare old vs new outputs:

```bash
# Compare Document Parse vs Prebuilt Extraction
python3 << 'EOF'
import pandas as pd

old = pd.read_parquet('data/backup/baseline_val_pseudo_labels.parquet')
new = pd.read_parquet('data/output/baseline_val_pseudo_labels.parquet')

print("Document Parse (old):")
print(f"  Source: {old.iloc[0]['metadata'].get('source')}")
print(f"  Has polygons: {any(len(p) > 0 for p in old.iloc[0]['polygons'])}")

print("\nPrebuilt Extraction (new):")
print(f"  Source: {new.iloc[0]['metadata'].get('source')}")
print(f"  Has polygons: {any(len(p) > 0 for p in new.iloc[0]['polygons'])}")
EOF
```

## Troubleshooting

### S3 Access Issues
- Ensure AWS credentials are configured
- Verify S3 bucket exists and is accessible
- Check IAM roles have S3 read permissions

### API Rate Limits
- Prebuilt Extraction is synchronous (faster than async Document Parse)
- Current concurrency: 3 (can be adjusted)
- Rate limits are handled automatically with retries

### Job Failures
- Check CloudWatch logs for errors
- Verify API key is in Secrets Manager
- Ensure job definition is updated with latest image

## Next Steps

After reprocessing:
1. Verify output quality
2. Compare with backup data
3. Update training pipelines if needed
4. Archive old outputs if satisfied with new results
