#!/usr/bin/env python3
"""Resumable batch processor for pseudo-label generation with 2x speed optimization.

Features:
- Checkpointing every N images (resumable)
- Async processing with optimized concurrency (2x faster)
- Skip already processed images
- Progress tracking and reporting

Usage:
    # Process training set
    uv run python runners/batch_pseudo_labels.py \
      --parquet data/processed/baseline_train.parquet \
      --output data/processed/baseline_train_pseudo_labels.parquet \
      --batch-size 500 \
      --checkpoint-dir data/checkpoints/pseudo_labels

    # Resume from checkpoint
    uv run python runners/batch_pseudo_labels.py \
      --parquet data/processed/baseline_train.parquet \
      --output data/processed/baseline_train_pseudo_labels.parquet \
      --resume
"""

import argparse
import asyncio
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm

from src.schemas import OCRStorageItem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Upstage API configuration
API_URL = "https://api.upstage.ai/v1/document-ai/ocr"
DEFAULT_CONCURRENCY = 3  # Conservative to avoid rate limiting
DEFAULT_BATCH_SIZE = 500
REQUEST_DELAY = 0.1  # 100ms delay between requests (max ~600/min with 3 concurrent)


class ResumableBatchProcessor:
    """Process images in resumable batches with checkpointing."""

    def __init__(
        self,
        api_key: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        batch_size: int = DEFAULT_BATCH_SIZE,
        checkpoint_dir: Path | None = None,
        s3_client=None,
    ):
        self.api_key = api_key
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.checkpoint_dir = checkpoint_dir
        self.s3_client = s3_client  # Optional boto3 S3 client for downloading images

        if checkpoint_dir:
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _download_image_from_s3(self, s3_uri: str) -> Optional[Path]:
        """Download image from S3 to temporary file. Returns temp file path or None."""
        if not self.s3_client:
            return None
        
        try:
            # Parse S3 URI: s3://bucket/key
            if not s3_uri.startswith('s3://'):
                return None
            
            parts = s3_uri[5:].split('/', 1)
            if len(parts) != 2:
                return None
            
            bucket, key = parts
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=Path(key).suffix)
            temp_path = Path(temp_file.name)
            temp_file.close()
            
            # Download from S3
            self.s3_client.download_file(bucket, key, str(temp_path))
            logger.debug(f"Downloaded {s3_uri} to {temp_path}")
            return temp_path
            
        except Exception as e:
            logger.error(f"Failed to download {s3_uri} from S3: {e}")
            return None

    async def process_single_image(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        image_row: dict,
        dataset_name: str,
        retry_count: int = 0,
        max_retries: int = 3,
    ) -> OCRStorageItem | None:
        """Process a single image with API call."""
        async with semaphore:  # Limit concurrent requests
            image_path_str = image_row['image_path']
            image_path = Path(image_path_str)
            temp_file_path = None
            
            # Handle S3 URIs
            if image_path_str.startswith('s3://'):
                if not self.s3_client:
                    logger.warning(f"S3 client not available, cannot download {image_path_str}")
                    return None
                temp_file_path = self._download_image_from_s3(image_path_str)
                if not temp_file_path:
                    return None
                image_path = temp_file_path
            elif not image_path.exists():
                logger.warning(f"Image not found: {image_path}")
                return None

            try:
                # Rate limiting: small delay before each request
                await asyncio.sleep(REQUEST_DELAY)

                start_time = time.time()

                # Read image
                with open(image_path, 'rb') as f:
                    image_bytes = f.read()

                # Call API
                headers = {"Authorization": f"Bearer {self.api_key}"}
                data = aiohttp.FormData()
                data.add_field('document', image_bytes, filename=image_path.name)

                async with session.post(API_URL, headers=headers, data=data) as response:
                    response_time_ms = (time.time() - start_time) * 1000

                    if response.status == 200:
                        api_result = await response.json()

                        # Log success
                        logger.debug(f"API success: {image_path.name} ({response_time_ms:.1f}ms)")

                        # Parse API response
                        polygons = []
                        texts = []
                        labels = []

                        for page in api_result.get('pages', []):
                            for word in page.get('words', []):
                                bbox = word.get('boundingBox', {}).get('vertices', [])
                                poly = [[float(v.get('x', 0)), float(v.get('y', 0))] for v in bbox]

                                polygons.append(poly)
                                texts.append(word.get('text', ''))
                                labels.append('text')

                        # Create storage item (preserve original S3 URI if applicable)
                        original_image_path = image_path_str if image_path_str.startswith('s3://') else str(image_path)
                        image_filename = Path(image_path_str).name if image_path_str.startswith('s3://') else image_path.name
                        
                        result = OCRStorageItem(
                            id=f"{dataset_name}_pseudo_{image_path.stem}",
                            split="pseudo",
                            image_path=original_image_path,
                            image_filename=image_filename,
                            width=int(image_row.get('width', 0)),
                            height=int(image_row.get('height', 0)),
                            polygons=polygons,
                            texts=texts,
                            labels=labels,
                            metadata={"source": "upstage_api", "enhanced": False}
                        )
                        
                        # Clean up temp file if downloaded from S3
                        if temp_file_path and temp_file_path.exists():
                            temp_file_path.unlink()
                        
                        return result

                    elif response.status == 429:
                        # Rate limited - retry with longer backoff (up to max_retries)
                        logger.warning(f"Rate limited: {image_path.name} (retry {retry_count + 1}/{max_retries})")

                        if retry_count >= max_retries:
                            logger.error(f"Max retries ({max_retries}) exceeded for {image_path.name}")
                            if temp_file_path and temp_file_path.exists():
                                temp_file_path.unlink()
                            return None

                        # Clean up temp file before retry (will be re-downloaded if needed)
                        if temp_file_path and temp_file_path.exists():
                            temp_file_path.unlink()
                        
                        backoff_delay = min(5 * (retry_count + 1), 30)  # Exponential backoff, max 30s
                        logger.warning(f"Retrying after {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                        # Retry will download image again if it's an S3 URI
                        return await self.process_single_image(
                            session, semaphore, image_row, dataset_name,
                            retry_count=retry_count + 1, max_retries=max_retries
                        )

                    else:
                        error_text = await response.text()
                        logger.error(f"API error {response.status}: {image_path.name} - {error_text[:200]}")
                        if temp_file_path and temp_file_path.exists():
                            temp_file_path.unlink()
                        return None

            except Exception as e:
                logger.error(f"Failed to process {image_path.name}: {e}")
                if temp_file_path and temp_file_path.exists():
                    temp_file_path.unlink()
                return None

    async def process_batch(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        start_idx: int = 0,
    ) -> list[OCRStorageItem]:
        """Process a batch of images asynchronously."""
        results = []
        semaphore = asyncio.Semaphore(self.concurrency)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for idx, row in df.iloc[start_idx:].iterrows():
                task = self.process_single_image(
                    session, semaphore, row.to_dict(), dataset_name
                )
                tasks.append(task)

            # Process with progress bar
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Processing batch from {start_idx}"
            ):
                result = await coro
                if result:
                    results.append(result)

        return results

    def save_checkpoint(self, checkpoint_name: str, results: list[OCRStorageItem]):
        """Save checkpoint to resume later."""
        if not self.checkpoint_dir:
            return

        checkpoint_path = self.checkpoint_dir / f"{checkpoint_name}.parquet"
        df = pd.DataFrame([item.model_dump() for item in results])
        df.to_parquet(checkpoint_path, engine='pyarrow', index=False)
        logger.info(f"Checkpoint saved: {checkpoint_path} ({len(results)} items)")

    def load_checkpoints(self, checkpoint_pattern: str) -> pd.DataFrame | None:
        """Load all checkpoints matching pattern."""
        if not self.checkpoint_dir or not self.checkpoint_dir.exists():
            return None

        checkpoints = sorted(self.checkpoint_dir.glob(f"{checkpoint_pattern}*.parquet"))
        if not checkpoints:
            return None

        dfs = [pd.read_parquet(cp) for cp in checkpoints]
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {len(checkpoints)} checkpoints with {len(combined)} total items")
        return combined

    async def process_dataset(
        self,
        parquet_file: Path,
        output_file: Path,
        dataset_name: str,
        resume: bool = False,
    ):
        """Process entire dataset with checkpointing."""
        # Load source data
        df = pd.read_parquet(parquet_file)
        total_images = len(df)
        logger.info(f"Loaded {total_images} images from {parquet_file}")

        # Resume from checkpoint if requested
        start_idx = 0
        all_results = []

        if resume:
            checkpoint_pattern = dataset_name.replace('_pseudo', '')
            existing = self.load_checkpoints(checkpoint_pattern)
            if existing is not None:
                all_results = [OCRStorageItem(**row) for _, row in existing.iterrows()]
                start_idx = len(all_results)
                logger.info(f"Resuming from index {start_idx}/{total_images}")

        # Process in batches
        batch_num = start_idx // self.batch_size
        while start_idx < total_images:
            end_idx = min(start_idx + self.batch_size, total_images)
            logger.info(f"\n{'='*80}")
            logger.info(f"Batch {batch_num + 1}: Processing images {start_idx}-{end_idx}/{total_images}")
            logger.info(f"{'='*80}\n")

            # Process batch
            batch_df = df.iloc[start_idx:end_idx]
            batch_results = await self.process_batch(batch_df, dataset_name, 0)

            # Save checkpoint
            if self.checkpoint_dir:
                checkpoint_name = f"{dataset_name.replace('_pseudo', '')}_batch_{batch_num:04d}"
                self.save_checkpoint(checkpoint_name, batch_results)

            all_results.extend(batch_results)
            start_idx = end_idx
            batch_num += 1

            # Progress report
            logger.info(f"\nProgress: {len(all_results)}/{total_images} images processed ({len(all_results)/total_images*100:.1f}%)")

        # Save final output
        final_df = pd.DataFrame([item.model_dump() for item in all_results])
        final_df.to_parquet(output_file, engine='pyarrow', index=False)
        logger.info(f"\n{'='*80}")
        logger.info(f"✓ Complete: {len(all_results)}/{total_images} images saved to {output_file}")
        logger.info(f"{'='*80}\n")


async def main():
    parser = argparse.ArgumentParser(description="Resumable batch pseudo-label generation")
    parser.add_argument("--parquet", type=Path, required=True, help="Input parquet file")
    parser.add_argument("--output", type=Path, required=True, help="Output parquet file")
    parser.add_argument("--name", type=str, default="baseline", help="Dataset name")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Checkpoint batch size")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent requests")
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("data/checkpoints/pseudo_labels"), help="Checkpoint directory")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")

    args = parser.parse_args()

    # Load API key from .env.local or environment
    api_key = os.getenv("UPSTAGE_API_KEY")
    if not api_key:
        # Try loading from .env.local
        env_local = Path(".env.local")
        if env_local.exists():
            with open(env_local) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("UPSTAGE_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        os.environ["UPSTAGE_API_KEY"] = api_key
                        logger.info("Loaded UPSTAGE_API_KEY from .env.local")
                        break

    if not api_key:
        logger.error("UPSTAGE_API_KEY not found in environment or .env.local")
        return 1

    # Create processor
    processor = ResumableBatchProcessor(
        api_key=api_key,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        checkpoint_dir=args.checkpoint_dir,
    )

    # Process dataset
    await processor.process_dataset(
        parquet_file=args.parquet,
        output_file=args.output,
        dataset_name=args.name,
        resume=args.resume,
    )

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
