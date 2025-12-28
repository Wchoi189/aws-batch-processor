# Standalone AWS Batch Processor - AI Project Brief

## Overview
Self-contained OCR pseudo-label generation service using AWS Batch + S3.

## Key Constraints
1. **No ML dependencies** - Only data processing (pandas, pydantic, boto3)
2. **S3-first design** - All I/O via S3, no local filesystem assumptions
3. **Minimal container** - ~500MB (vs ~3GB with PyTorch)
4. **Independent deployment** - No parent project dependencies

## Structure
```
aws-batch-processor/          # Standalone project root
├── src/
│   ├── processor.py          # Main: S3-enabled batch processor
│   └── schemas.py            # Data models (OCRStorageItem)
├── config/
│   ├── Dockerfile            # Container (Python 3.11 slim)
│   ├── requirements.txt      # 9 minimal deps
│   └── batch-job-def.json    # AWS Batch job template
├── data/
│   └── input/sample_10.parquet  # Test data (10 images)
├── docs/DATA_CATALOG.md      # Schema documentation
├── setup-aws.sh              # One-command AWS setup
└── README.md                 # User & AI instructions
```

## Critical Files

### `src/processor.py`
Main batch processor logic:
- **Entry point**: `main()` async function
- **Key methods**:
  - `download_input_dataset()` - S3 → local
  - `process_single_image()` - API call + retry logic
  - `save_checkpoint()` - Local → S3 (every 500 images)
  - `upload_final_output()` - Final parquet → S3
- **Rate limiting**: 3 concurrent, 100ms delay, exponential backoff
- **Resumable**: Reads checkpoints from S3 on `--resume` flag

### `config/requirements.txt` (9 deps only)
```
aiohttp, boto3, aioboto3      # API + S3
pandas, pyarrow, pydantic     # Data
Pillow, numpy                 # Basic image ops
tqdm                          # Progress
```

### `config/Dockerfile`
- Base: `python:3.11-slim`
- Deps: apt packages (libgl1, etc.) + uv + requirements.txt
- Context: Project root (copies src/, config/)
- CMD: `python -m src.processor`

## Common AI Tasks

### Add Feature to Processor
1. Edit `src/processor.py`
2. Test locally: `python src/processor.py --help`
3. Commit: `git add src/ && git commit -m "feat: ..." && git push`
4. GitHub Actions auto-deploys to ECR

### Change API Integration
1. Modify `src/processor.py` L90-180 (API call section)
2. Update `docs/DATA_CATALOG.md` if schema changes
3. Test with sample data: `python -m src.processor --dataset sample_10`

### Optimize Container
1. Check `config/requirements.txt` - remove unused deps
2. Review `config/Dockerfile` - optimize layer caching
3. Build locally: `docker build -t test -f config/Dockerfile .`

## Deployment

### Initial Setup
```bash
./setup-aws.sh  # Creates S3, ECR IAM, Batch (5 min)
# Configure GitHub secrets from output
git push  # GitHub Actions builds + deploys
```

### Run Job
```bash
# Via GitHub Actions UI
# Or AWS CLI:
aws batch submit-job --job-name "job-$(date +%s)" \
  --job-definition pseudo-label-processor \
  --job-queue batch-processor-queue
```

## Data Flow
```
S3 Input Parquet
  ↓ download
Local /tmp/input.parquet
  ↓ process (async, 3 concurrent)
Upstage API → polygons/texts
  ↓ checkpoint (every 500)
S3 Checkpoints (resumable)
  ↓ final
S3 Output Parquet
```

## Testing Locally
```bash
# Set AWS creds
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3_BUCKET=your-bucket
export UPSTAGE_API_KEY=your-key

# Run processor
python -m src.processor \
  --dataset-name sample_10 \
  --batch-size 5 \
  --concurrency 2
```

## Troubleshooting

**Import errors**: Check `src/` imports only reference `src.schemas`, not parent project
**Missing deps**: Add to `config/requirements.txt`, rebuild Docker
**S3 access denied**: Verify IAM role has S3 + Secrets Manager permissions
**Rate limits**: Already handled (3 retries, exponential backoff)

## Cost
- ~$0.03/1000 images (Fargate Spot)
- S3 storage: ~$0.01/GB/month
- ECR: ~$0.10/GB/month

## Key Design Decisions
1. **Standalone** - Can export as separate GitHub repo
2. **Minimal** - No training deps (PyTorch, Wandb, transformers)
3. **Cloud-native** - S3 for everything, no local filesystem
4. **AI-friendly** - Concise docs, clear structure, focused scope
