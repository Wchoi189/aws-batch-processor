#!/usr/bin/env python3
"""Convert local image paths in parquet to S3 URIs and upload images."""

import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

try:
    import pandas as pd
    import boto3
    from tqdm import tqdm
except ImportError as e:
    print(f"Error: {e}")
    print("Install required packages: pip install pandas pyarrow boto3 tqdm")
    sys.exit(1)


def fix_parquet_s3_paths(input_parquet: Path, output_parquet: Path, s3_bucket: str, s3_prefix: str = "images/"):
    """Convert local paths to S3 URIs in parquet file."""
    print(f"Reading {input_parquet}...")
    df = pd.read_parquet(input_parquet)
    
    print(f"Found {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    if 'image_path' not in df.columns:
        print("Error: 'image_path' column not found")
        return False
    
    # Count current S3 vs local paths
    s3_count = df['image_path'].str.startswith('s3://').sum()
    local_count = (~df['image_path'].str.startswith('s3://')).sum()
    
    print(f"\nCurrent paths:")
    print(f"  S3 URIs: {s3_count}")
    print(f"  Local paths: {local_count}")
    
    if local_count == 0:
        print("✓ All paths are already S3 URIs")
        return True
    
    # Prepare upload tasks
    workspace_root = Path("/workspaces/upstageailab-ocr-recsys-competition-ocr-2")
    upload_tasks = []
    
    print(f"\nPreparing upload tasks...")
    for idx, row in df.iterrows():
        old_path = row['image_path']
        
        if old_path.startswith('s3://'):
            continue
        
        # Extract filename from path
        filename = Path(old_path).name
        s3_key = f"{s3_prefix}{filename}"
        s3_uri = f"s3://{s3_bucket}/{s3_key}"
        
        # Find local file - try multiple locations
        local_file = Path(old_path)
        if not local_file.exists():
            # Try workspace root + path
            alt_path = workspace_root / old_path.lstrip("/")
            if alt_path.exists():
                local_file = alt_path
            else:
                # Try in train/ subdirectory
                alt_path2 = workspace_root / "data/datasets/images/train" / filename
                if alt_path2.exists():
                    local_file = alt_path2
                else:
                    # Try in val/ subdirectory
                    alt_path3 = workspace_root / "data/datasets/images/val" / filename
                    if alt_path3.exists():
                        local_file = alt_path3
                    else:
                        # Try in test/ subdirectory
                        alt_path4 = workspace_root / "data/datasets/images/test" / filename
                        if alt_path4.exists():
                            local_file = alt_path4
                        else:
                            # Try searching all subdirectories
                            images_dir = workspace_root / "data/datasets/images"
                            if images_dir.exists():
                                found = list(images_dir.rglob(filename))
                                if found:
                                    local_file = found[0]
        
        if local_file.exists():
            upload_tasks.append((idx, local_file, s3_key, s3_uri, filename))
        else:
            # Still update path even if file doesn't exist (user can upload later)
            df.at[idx, 'image_path'] = s3_uri
    
    print(f"Found {len(upload_tasks)} files to upload")
    
    # Upload files concurrently
    s3_client = boto3.client('s3')
    
    def upload_file(task):
        idx, local_file, s3_key, s3_uri, filename = task
        try:
            s3_client.upload_file(str(local_file), s3_bucket, s3_key)
            df.at[idx, 'image_path'] = s3_uri
            return (True, filename, None)
        except Exception as e:
            return (False, filename, str(e))
    
    print(f"\nUploading {len(upload_tasks)} files with 20 concurrent workers...")
    uploaded = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(upload_file, task): task for task in upload_tasks}
        
        for future in tqdm(as_completed(futures), total=len(upload_tasks), desc="Uploading"):
            success, filename, error = future.result()
            if success:
                uploaded += 1
            else:
                failed += 1
                if failed <= 10:  # Show first 10 errors
                    print(f"  ⚠️  Failed to upload {filename}: {error}")
    
    # Update paths for skipped S3 URIs
    skipped = df['image_path'].str.startswith('s3://').sum() - uploaded
    
    print(f"\n✓ Upload complete:")
    print(f"  Uploaded: {uploaded}")
    print(f"  Failed: {failed}")
    print(f"  Already S3 URIs: {skipped}")
    
    # Save updated parquet
    print(f"\nSaving to {output_parquet}...")
    df.to_parquet(output_parquet, engine='pyarrow', index=False)
    print(f"✓ Saved {len(df)} rows")
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert local image paths to S3 URIs")
    parser.add_argument("--input", type=Path, required=True, help="Input parquet file")
    parser.add_argument("--output", type=Path, required=True, help="Output parquet file")
    parser.add_argument("--s3-bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--s3-prefix", type=str, default="images/", help="S3 prefix for images")
    
    args = parser.parse_args()
    
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)
    
    success = fix_parquet_s3_paths(args.input, args.output, args.s3_bucket, args.s3_prefix)
    sys.exit(0 if success else 1)
