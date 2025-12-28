#!/usr/bin/env python3
"""AWS Batch-compatible pseudo-label processor with S3 checkpoint storage.

Features:
- S3-based checkpoint storage (resume on spot interruptions)
- Download input datasets from S3
- Upload results and checkpoints to S3
- Progress tracking via S3 markers
- Secrets Manager integration for API keys

Environment Variables:
    UPSTAGE_API_KEY: API key (or use AWS_SECRET_ARN)
    AWS_SECRET_ARN: ARN of secret containing API key
    S3_BUCKET: S3 bucket for data storage
    DATASET_NAME: Name of dataset to process (e.g., baseline_train)
    CHECKPOINT_PREFIX: S3 prefix for checkpoints (default: checkpoints/)
    INPUT_S3_KEY: S3 key of input parquet file
    OUTPUT_S3_KEY: S3 key for output parquet file

Usage:
    # Local testing with S3
    export S3_BUCKET=your-bucket
    export DATASET_NAME=baseline_train
    python runners/batch_pseudo_labels_aws.py

    # AWS Batch job (environment variables set in job definition)
    python -m runners.batch_pseudo_labels_aws
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import aioboto3
import boto3
import pandas as pd

# Import base processor
from src.batch_processor_base import ResumableBatchProcessor
from src.schemas import OCRStorageItem

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3ResumableBatchProcessor(ResumableBatchProcessor):
    """AWS Batch-compatible processor with S3 checkpoint storage."""

    def __init__(
        self,
        s3_bucket: str,
        checkpoint_prefix: str = "checkpoints/",
        *args,
        **kwargs
    ):
        # Create S3 client first
        s3_client = boto3.client('s3')
        
        # Pass S3 client to base class for image downloads
        super().__init__(*args, s3_client=s3_client, **kwargs)
        
        self.s3_bucket = s3_bucket
        self.checkpoint_prefix = checkpoint_prefix
        self.s3_client = s3_client  # Also store for S3 operations in this class

        logger.info(f"S3 storage: s3://{s3_bucket}/{checkpoint_prefix}")

    def save_checkpoint(self, checkpoint_name: str, results: list[OCRStorageItem]):
        """Save checkpoint locally and upload to S3."""
        # Save locally first
        super().save_checkpoint(checkpoint_name, results)

        # Upload to S3
        if self.checkpoint_dir:
            local_path = self.checkpoint_dir / f"{checkpoint_name}.parquet"
            s3_key = f"{self.checkpoint_prefix}{checkpoint_name}.parquet"

            try:
                self.s3_client.upload_file(
                    str(local_path),
                    self.s3_bucket,
                    s3_key
                )
                logger.info(f"✓ Checkpoint uploaded: s3://{self.s3_bucket}/{s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload checkpoint to S3: {e}")
                raise

    def load_checkpoints(self, checkpoint_pattern: str) -> pd.DataFrame | None:
        """Download checkpoints from S3 and load."""
        try:
            # List checkpoints in S3
            prefix = f"{self.checkpoint_prefix}{checkpoint_pattern}"
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                logger.info("No checkpoints found in S3")
                return None

            # Ensure checkpoint directory exists
            if self.checkpoint_dir:
                self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

            # Download all checkpoints
            for obj in response['Contents']:
                key = obj['Key']
                if not key.endswith('.parquet'):
                    continue

                local_path = self.checkpoint_dir / Path(key).name
                logger.info(f"Downloading checkpoint: s3://{self.s3_bucket}/{key}")
                self.s3_client.download_file(
                    self.s3_bucket,
                    key,
                    str(local_path)
                )

            # Load using parent class method
            return super().load_checkpoints(checkpoint_pattern)

        except Exception as e:
            logger.error(f"Failed to load checkpoints from S3: {e}")
            return None

    def upload_final_output(self, local_path: Path, s3_key: str):
        """Upload final output parquet to S3."""
        try:
            logger.info(f"Uploading final output to s3://{self.s3_bucket}/{s3_key}")
            self.s3_client.upload_file(
                str(local_path),
                self.s3_bucket,
                s3_key
            )
            logger.info(f"✓ Upload complete: s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload output to S3: {e}")
            raise

    def download_input_dataset(self, s3_key: str, local_path: Path):
        """Download input dataset from S3."""
        try:
            logger.info(f"Downloading dataset from s3://{self.s3_bucket}/{s3_key}")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(
                self.s3_bucket,
                s3_key,
                str(local_path)
            )
            logger.info(f"✓ Downloaded to {local_path}")
        except Exception as e:
            logger.error(f"Failed to download dataset from S3: {e}")
            raise


def get_api_key_from_secrets_manager(secret_arn: str) -> str:
    """Retrieve API key from AWS Secrets Manager."""
    try:
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=secret_arn)

        # Handle both string and JSON secrets
        secret = response['SecretString']
        if secret.startswith('{'):
            import json
            secret_dict = json.loads(secret)
            return secret_dict.get('UPSTAGE_API_KEY', secret_dict.get('api_key', ''))
        return secret
    except Exception as e:
        logger.error(f"Failed to retrieve secret from Secrets Manager: {e}")
        raise


async def main():
    parser = argparse.ArgumentParser(description="AWS Batch pseudo-label processor")
    parser.add_argument("--s3-bucket", type=str, help="S3 bucket name")
    parser.add_argument("--dataset-name", type=str, help="Dataset name")
    parser.add_argument("--input-s3-key", type=str, help="Input S3 key")
    parser.add_argument("--output-s3-key", type=str, help="Output S3 key")
    parser.add_argument("--checkpoint-prefix", type=str, default="checkpoints/", help="S3 checkpoint prefix")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent requests")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoints")
    parser.add_argument("--api-type", type=str, default="document-parse", choices=["document-parse", "prebuilt-extraction"],
                        help="API type to use: 'document-parse' (default, general documents) or 'prebuilt-extraction' (receipts/invoices)")

    args = parser.parse_args()

    # Get configuration from environment or args
    s3_bucket = args.s3_bucket or os.getenv("S3_BUCKET")
    dataset_name = args.dataset_name or os.getenv("DATASET_NAME", "baseline_train")
    input_s3_key = args.input_s3_key or os.getenv("INPUT_S3_KEY", f"data/processed/{dataset_name}.parquet")
    output_s3_key = args.output_s3_key or os.getenv("OUTPUT_S3_KEY", f"data/processed/{dataset_name}_pseudo_labels.parquet")
    checkpoint_prefix = args.checkpoint_prefix or os.getenv("CHECKPOINT_PREFIX", "checkpoints/")

    if not s3_bucket:
        logger.error("S3_BUCKET must be specified via --s3-bucket or S3_BUCKET environment variable")
        return 1

    # Get API key from environment or Secrets Manager
    api_key = os.getenv("UPSTAGE_API_KEY")
    secret_arn = os.getenv("AWS_SECRET_ARN")

    if not api_key and secret_arn:
        logger.info(f"Retrieving API key from Secrets Manager: {secret_arn}")
        api_key = get_api_key_from_secrets_manager(secret_arn)

    if not api_key:
        logger.error("UPSTAGE_API_KEY or AWS_SECRET_ARN must be set")
        return 1

    # Create processor
    local_checkpoint_dir = Path("/tmp/checkpoints")
    processor = S3ResumableBatchProcessor(
        api_key=api_key,
        s3_bucket=s3_bucket,
        checkpoint_prefix=checkpoint_prefix,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        checkpoint_dir=local_checkpoint_dir,
        api_type=args.api_type,
    )

    # Download input dataset
    local_input = Path(f"/tmp/{dataset_name}.parquet")
    processor.download_input_dataset(input_s3_key, local_input)

    # Process dataset
    local_output = Path(f"/tmp/{dataset_name}_pseudo_labels.parquet")

    logger.info(f"\n{'='*80}")
    logger.info(f"Starting batch processing: {dataset_name}")
    logger.info(f"  Input: s3://{s3_bucket}/{input_s3_key}")
    logger.info(f"  Output: s3://{s3_bucket}/{output_s3_key}")
    logger.info(f"  Checkpoints: s3://{s3_bucket}/{checkpoint_prefix}")
    logger.info(f"  API Type: {args.api_type}")
    logger.info(f"  Resume: {args.resume}")
    logger.info(f"{'='*80}\n")

    await processor.process_dataset(
        parquet_file=local_input,
        output_file=local_output,
        dataset_name=dataset_name,
        resume=args.resume,
    )

    # Upload final output
    processor.upload_final_output(local_output, output_s3_key)

    logger.info(f"\n{'='*80}")
    logger.info("✓ Batch processing complete!")
    logger.info(f"{'='*80}\n")

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
