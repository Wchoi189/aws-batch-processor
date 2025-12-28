# AWS Batch OCR Pseudo-Label Processor

Standalone service for cloud-based OCR pseudo-label generation using Upstage Document Parse API or Prebuilt Extraction API.

## Quick Reference

### Upstage API Endpoints

**Document Parsing (default)** - General documents:
```
https://api.upstage.ai/v1/document-digitization/async
```

**Prebuilt Extraction** - Receipts/invoices (better quality for structured documents):
```
https://api.upstage.ai/v1/information-extraction/prebuilt-extraction
```

**Authentication**: Bearer token (API key in header)

**Request**: Multipart form-data with image file

**Response**: JSON with text polygons and content

**Usage**: Select API type via `--api-type` argument (default: `document-parse`)

### Output Format
Parquet file with OCRStorageItem schema:
- `polygons`: List of bounding boxes `[[[x,y], [x,y], [x,y], [x,y]], ...]`
- `texts`: Extracted text strings `["Invoice", "Total", ...]`
- `labels`: Classification labels `["text", "text", ...]`
- `metadata`: Processing info `{"source": "upstage_api", "enhanced": false}`

---

## Setup from Scratch

### 1. Install AWS CLI
```bash
# Already done if you see this file
aws --version  # Should show: aws-cli/2.x.x
```

### 2. Configure AWS Credentials
```bash
aws configure
# Enter: Access Key ID, Secret Access Key, Region (us-east-1), Format (json)
```

### 3. Set API Key
```bash
export UPSTAGE_API_KEY="your-upstage-api-key-here"
```

### 4. Run AWS Infrastructure Setup
```bash
cd aws-batch-processor
./setup-aws.sh
```

**Creates**:
- S3 bucket (`ocr-batch-processing`)
- ECR repository (`batch-processor`)
- Secrets Manager (stores API key)
- IAM roles (Job, Execution, Service)
- AWS Batch compute environment (Fargate Spot)
- AWS Batch job queue
- AWS Batch job definition

**Output**: Configuration saved to `config.env`

### 5. Upload Sample Data to S3
```bash
# Source config
source config.env

# Upload sample data
python scripts/prepare_dataset.py

# Or upload your own:
aws s3 cp data/input/your_data.parquet s3://$S3_BUCKET/data/processed/
aws s3 sync your_images/ s3://$S3_BUCKET/images/
```

### 6. Configure GitHub Secrets

**Required secrets** (Settings → Secrets and variables → Actions):

| Secret Name | Value Source |
|-------------|--------------|
| `AWS_ACCESS_KEY_ID` | AWS IAM Console → Security credentials |
| `AWS_SECRET_ACCESS_KEY` | Created with access key (shown once) |
| `AWS_REGION` | `us-east-1` (or your region) |
| `ECR_REPOSITORY_URI` | From `config.env` → `$ECR_URI` |
| `S3_BUCKET` | From `config.env` → `$S3_BUCKET` |
| `JOB_ROLE_ARN` | From `config.env` → `$JOB_ROLE_ARN` |
| `EXEC_ROLE_ARN` | From `config.env` → `$EXEC_ROLE_ARN` |
| `SECRET_ARN` | From `config.env` → Construct as shown below |

**Get values**:
```bash
source config.env
echo "AWS_REGION: $AWS_REGION"
echo "ECR_REPOSITORY_URI: $ECR_URI"
echo "S3_BUCKET: $S3_BUCKET"
echo "JOB_ROLE_ARN: $JOB_ROLE_ARN"
echo "EXEC_ROLE_ARN: $EXEC_ROLE_ARN"
echo "SECRET_ARN: arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:${SECRET_NAME}"
```

### 7. Deploy
```bash
git add .
git commit -m "Deploy AWS Batch processor"
git push origin main
```

**GitHub Actions** automatically:
1. Builds Docker image
2. Pushes to ECR
3. Updates Batch job definition

**Monitor**: https://github.com/YOUR_USER/YOUR_REPO/actions

---

## Running Jobs

### Option A: GitHub Actions (Recommended)
1. Go to: **Actions** → "Run Batch Job"
2. Click **"Run workflow"**
3. Select dataset: `baseline_train` or `baseline_val`
4. Configure:
   - Resume: `false` (first run) or `true` (resume from checkpoint)
   - Batch size: `500` (checkpoint interval)
   - Concurrency: `3` (API requests)
5. Click **"Run workflow"**

### Option B: AWS CLI
```bash
aws batch submit-job \
  --job-name "pseudo-labels-$(date +%Y%m%d-%H%M%S)" \
  --job-definition pseudo-label-processor \
  --job-queue batch-processor-queue \
  --container-overrides '{
    "environment": [
      {"name": "DATASET_NAME", "value": "baseline_train"},
      {"name": "S3_BUCKET", "value": "ocr-batch-processing"}
    ]
  }'
```

### Option C: Local Testing
```bash
export AWS_ACCESS_KEY_ID=youraccesskey
export AWS_SECRET_ACCESS_KEY=yoursecret
export S3_BUCKET=ocr-batch-processing
export UPSTAGE_API_KEY=your-api-key
export DATASET_NAME=sample_10

# Use Document Parsing (default, general documents)
python -m src.processor \
  --dataset-name sample_10 \
  --batch-size 5 \
  --concurrency 2

# Use Prebuilt Extraction (receipts/invoices)
python -m src.processor \
  --dataset-name sample_10 \
  --batch-size 5 \
  --concurrency 2 \
  --api-type prebuilt-extraction
```

---

## Data Requirements

### Input Parquet
Must contain:
- `image_path`: S3 URI (`s3://bucket/images/file.jpg`)
- `image_filename`: File name
- `width`, `height`: Image dimensions

### Images in S3
```bash
# Upload images before processing
aws s3 sync local_images/ s3://YOUR_BUCKET/images/
```

### Output Location
- **Final**: `s3://YOUR_BUCKET/data/processed/{dataset_name}_pseudo_labels.parquet`
- **Checkpoints**: `s3://YOUR_BUCKET/checkpoints/{dataset_name}_batch_*.parquet`

---

## Monitoring & Troubleshooting

### Check Job Status
```bash
# List running jobs
aws batch list-jobs --job-queue batch-processor-queue --job-status RUNNING

# Check specific job
aws batch describe-jobs --jobs JOB_ID

# View logs
aws logs tail /aws/batch/job --log-stream-name LOG_STREAM_NAME --follow
```

### Download Results
```bash
aws s3 cp s3://$S3_BUCKET/data/processed/baseline_train_pseudo_labels.parquet ./results/
```

### Resume Failed Job
```bash
aws batch submit-job \
  --job-name "pseudo-labels-resume" \
  --job-definition pseudo-label-processor \
  --job-queue batch-processor-queue \
  --container-overrides '{
    "command": ["python", "-m", "src.processor", "--resume"]
  }'
```

### Common Issues

**Job immediately fails**:
- Check ECR image exists: `aws ecr describe-images --repository-name batch-processor`
- Verify job definition: `aws batch describe-job-definitions --job-definition-name pseudo-label-processor`

**Rate limits (429 errors)**:
- Already handled (3 retries, exponential backoff)
- Can reduce concurrency: `--concurrency 2`

**No output file**:
- Check S3: `aws s3 ls s3://YOUR_BUCKET/data/processed/`
- View CloudWatch logs for errors

---

## Cost Estimation

**Per run** (3,000 images, ~90 minutes):
- Fargate Spot (2 vCPU, 4GB): ~$0.03
- S3 storage (temporary): ~$0.01
- **Total**: ~$0.04

**Monthly** (10 runs):
- Compute: $0.40
- ECR storage: $0.10
- **Total**: ~$0.50/month

---

## Architecture

```
GitHub Push
  ↓
GitHub Actions (Build)
  ↓
Docker Image → ECR
  ↓
AWS Batch Job Queue
  ↓
Fargate Container
  ├→ Download: S3 (dataset + images)
  ├→ Process: Upstage API (3 concurrent)
  ├→ Checkpoint: S3 (every 500 images)
  └→ Upload: S3 (final results)
```

---

## API Selection

The processor supports two Upstage API types:

1. **Document Parsing** (`--api-type document-parse`, default)
   - Best for: General documents, mixed content
   - Endpoint: `v1/document-digitization/async`

2. **Prebuilt Extraction** (`--api-type prebuilt-extraction`)
   - Best for: Receipts, invoices, structured documents
   - Endpoint: `v1/information-extraction/prebuilt-extraction`
   - Provides higher quality results for receipt/invoice documents

**When to use each:**
- Use **Document Parsing** for general OCR tasks
- Use **Prebuilt Extraction** for receipt/invoice datasets to improve extraction quality

**Testing APIs locally:**
```bash
# Test Document Parsing
python scripts/test_api_local.py image.jpg --api-type document-parse

# Test Prebuilt Extraction
python scripts/test_api_local.py receipt.jpg --api-type prebuilt-extraction
```

## Files Reference

- `src/processor.py` - Main batch processor (S3-enabled)
- `src/batch_processor_base.py` - Core processing logic with API selection
- `src/schemas.py` - Data models (OCRStorageItem)
- `config/Dockerfile` - Container definition
- `config/requirements.txt` - Dependencies (9 minimal)
- `config/batch-job-def.json` - AWS Batch job template
- `scripts/prepare_dataset.py` - Upload data to S3
- `scripts/test_api_local.py` - Local API testing tool (supports both APIs)
- `setup-aws.sh` - One-command AWS setup
- `docs/DATA_CATALOG.md` - Schema documentation

---

## Re-creating AWS Resources

If you deleted AWS resources and need to recreate:

```bash
# 1. Ensure API key is set
export UPSTAGE_API_KEY="your-key"

# 2. Run setup script
./setup-aws.sh

# 3. Note new values from config.env

# 4. Update GitHub secrets with new ARNs

# 5. Deploy
git push origin main
```

**Important**: ECR URI and IAM role ARNs will be the same if using same account/region. Only S3 bucket name might change if not available.

---

## Support

**Documentation**:
- `docs/DATA_CATALOG.md` - Data schemas and API details
- `AI_PROJECT_BRIEF.md` - AI agent instructions

**Debugging**:
- CloudWatch Logs: `/aws/batch/job`
- S3 checkpoints: `s3://bucket/checkpoints/`
- Job status: AWS Batch console

**Contact**: Check parent repository for maintainer info
