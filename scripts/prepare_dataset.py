#!/usr/bin/env python3
"""Create a small test dataset and upload to S3 for AWS Batch testing.

This creates a 50-image subset of the training data and uploads both
the images and metadata to S3 for quick validation of the AWS Batch pipeline.
"""

import os
import sys
from pathlib import Path

import boto3
import pandas as pd
from tqdm import tqdm


def create_test_subset(input_parquet: Path, output_parquet: Path, n_samples: int = 50):
    """Create a small test subset of the dataset."""
    print(f"Loading {input_parquet}...")
    df = pd.read_parquet(input_parquet)

    print(f"Total images: {len(df)}")
    test_df = df.head(n_samples)

    print(f"Creating test subset: {len(test_df)} images")
    test_df.to_parquet(output_parquet, engine='pyarrow', index=False)

    print(f"✓ Saved test dataset to {output_parquet}")
    return test_df


def upload_images_to_s3(df: pd.DataFrame, s3_bucket: str, local_base: Path):
    """Upload images referenced in dataframe to S3."""
    s3_client = boto3.client('s3')

    print(f"\nUploading {len(df)} images to s3://{s3_bucket}/images/...")

    uploaded = 0
    skipped = 0
    failed = 0

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        local_path = Path(row['image_path'])
        image_name = local_path.name
        s3_key = f"images/{image_name}"

        # Check if already exists
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            skipped += 1
            continue
        except:
            pass  # Object doesn't exist, need to upload

        # Upload
        try:
            if not local_path.exists():
                print(f"⚠️  Local file not found: {local_path}")
                failed += 1
                continue

            s3_client.upload_file(
                str(local_path),
                s3_bucket,
                s3_key,
                ExtraArgs={'StorageClass': 'INTELLIGENT_TIERING'}
            )
            uploaded += 1
        except Exception as e:
            print(f"❌ Failed to upload {image_name}: {e}")
            failed += 1

    print(f"\n✓ Upload complete:")
    print(f"  - Uploaded: {uploaded}")
    print(f"  - Skipped (already exists): {skipped}")
    print(f"  - Failed: {failed}")

    return uploaded + skipped == len(df)


def upload_parquet_to_s3(local_file: Path, s3_bucket: str, s3_key: str):
    """Upload parquet metadata file to S3."""
    s3_client = boto3.client('s3')

    print(f"\nUploading {local_file} to s3://{s3_bucket}/{s3_key}...")
    s3_client.upload_file(str(local_file), s3_bucket, s3_key)
    print(f"✓ Upload complete")


def main():
    # Configuration
    input_parquet = Path("data/processed/baseline_train.parquet")
    output_parquet = Path("data/processed/test_50.parquet")
    n_samples = 50

    # Get S3 bucket from environment or config file
    s3_bucket = os.getenv("S3_BUCKET")
    if not s3_bucket:
        # Try reading from aws/config.env
        config_file = Path("aws/config.env")
        if config_file.exists():
            with open(config_file) as f:
                for line in f:
                    if line.startswith("S3_BUCKET="):
                        s3_bucket = line.split("=", 1)[1].strip()
                        break

        if not s3_bucket:
            print("❌ S3_BUCKET not found in environment or aws/config.env")
            print("Run: export S3_BUCKET=your-bucket-name")
            return 1

    print("="*60)
    print("AWS Batch Test Dataset Creator")
    print("="*60)
    print(f"S3 Bucket: {s3_bucket}")
    print(f"Test samples: {n_samples}")
    print("="*60)

    # Step 1: Create test subset
    test_df = create_test_subset(input_parquet, output_parquet, n_samples)

    # Step 2: Upload images to S3
    success = upload_images_to_s3(test_df, s3_bucket, Path("data/raw_images"))

    if not success:
        print("⚠️  Some images failed to upload")

    # Step 3: Upload parquet to S3
    upload_parquet_to_s3(
        output_parquet,
        s3_bucket,
        "data/processed/test_50.parquet"
    )

    print("\n" + "="*60)
    print("✅ Test dataset ready!")
    print("="*60)
    print(f"\nS3 Locations:")
    print(f"  Images:  s3://{s3_bucket}/images/ ({len(test_df)} files)")
    print(f"  Parquet: s3://{s3_bucket}/data/processed/test_50.parquet")
    print(f"\nNext steps:")
    print(f"  1. Build and push Docker image (see docs/aws-batch-quickref.md)")
    print(f"  2. Run test job with dataset: test_50")
    print("="*60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
