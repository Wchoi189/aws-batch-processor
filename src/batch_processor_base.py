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

# Upstage API configuration - Using async endpoint for better rate limit handling
API_URL_SUBMIT = "https://api.upstage.ai/v1/document-digitization/async"
API_URL_STATUS = "https://api.upstage.ai/v1/document-digitization/requests"
DEFAULT_CONCURRENCY = 5  # Higher concurrency for async submission (no waiting)
DEFAULT_BATCH_SIZE = 500
REQUEST_DELAY = 0.05  # 50ms delay for submission (faster since we're not waiting)
POLL_DELAY = 2.0  # 2s delay between polling checks
POLL_MAX_WAIT = 300  # Max 5 minutes wait per request


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

    async def submit_image_async(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        image_row: dict,
        retry_count: int = 0,
        max_retries: int = 3,
    ) -> str | None:
        """Submit image to async API and return request_id."""
        async with semaphore:
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
                await asyncio.sleep(REQUEST_DELAY)

                # Read image
                with open(image_path, 'rb') as f:
                    image_bytes = f.read()

                # Submit to async API
                headers = {"Authorization": f"Bearer {self.api_key}"}
                data = aiohttp.FormData()
                data.add_field('document', image_bytes, filename=image_path.name)
                data.add_field('model', 'document-parse')

                async with session.post(API_URL_SUBMIT, headers=headers, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        request_id = result.get('request_id')
                        if request_id:
                            logger.debug(f"Submitted: {image_path.name} → {request_id}")
                            if temp_file_path and temp_file_path.exists():
                                temp_file_path.unlink()
                            return request_id
                        else:
                            logger.error(f"No request_id in response for {image_path.name}")
                            if temp_file_path and temp_file_path.exists():
                                temp_file_path.unlink()
                            return None

                    elif response.status == 429:
                        logger.warning(f"Rate limited (submit): {image_path.name} (retry {retry_count + 1}/{max_retries})")
                        if retry_count >= max_retries:
                            logger.error(f"Max retries exceeded for {image_path.name}")
                            if temp_file_path and temp_file_path.exists():
                                temp_file_path.unlink()
                            return None
                        
                        backoff_delay = min(5 * (retry_count + 1), 30)
                        await asyncio.sleep(backoff_delay)
                        return await self.submit_image_async(
                            session, semaphore, image_row, retry_count + 1, max_retries
                        )

                    else:
                        error_text = await response.text()
                        logger.error(f"API error {response.status}: {image_path.name} - {error_text[:200]}")
                        if temp_file_path and temp_file_path.exists():
                            temp_file_path.unlink()
                        return None

            except Exception as e:
                logger.error(f"Failed to submit {image_path.name}: {e}")
                if temp_file_path and temp_file_path.exists():
                    temp_file_path.unlink()
                return None

    async def poll_and_get_result(
        self,
        session: aiohttp.ClientSession,
        request_id: str,
        image_row: dict,
        dataset_name: str,
    ) -> OCRStorageItem | None:
        """Poll for async result and download when ready."""
        start_time = time.time()
        headers = {"Authorization": f"Bearer {self.api_key}"}
        
        while time.time() - start_time < POLL_MAX_WAIT:
            await asyncio.sleep(POLL_DELAY)
            
            try:
                async with session.get(f"{API_URL_STATUS}/{request_id}", headers=headers) as response:
                    if response.status == 200:
                        status_data = await response.json()
                        status = status_data.get('status')
                        
                        if status == 'completed':
                            # Get download_url from batches
                            batches = status_data.get('batches', [])
                            if batches and batches[0].get('download_url'):
                                download_url = batches[0]['download_url']
                                
                                # Download result
                                async with session.get(download_url) as result_response:
                                    if result_response.status == 200:
                                        api_result = await result_response.json()
                                        
                                        # Parse API response - async API may have different structure
                                        polygons = []
                                        texts = []
                                        labels = []

                                        # Try different response formats
                                        # Format 1: Direct pages array (sync API format)
                                        pages = api_result.get('pages', [])
                                        
                                        # Format 2: Nested structure (async API might wrap it)
                                        if not pages and 'result' in api_result:
                                            pages = api_result['result'].get('pages', [])
                                        
                                        # Format 3: Check if it's a list directly
                                        if not pages and isinstance(api_result, list):
                                            pages = api_result
                                        
                                        for page in pages:
                                            # Handle both dict and direct word lists
                                            words = page.get('words', []) if isinstance(page, dict) else page
                                            
                                            for word in words:
                                                if isinstance(word, dict):
                                                    bbox = word.get('boundingBox', {}).get('vertices', [])
                                                    if bbox:
                                                        poly = [[float(v.get('x', 0)), float(v.get('y', 0))] for v in bbox]
                                                        polygons.append(poly)
                                                        texts.append(word.get('text', ''))
                                                        labels.append('text')
                                        
                                        logger.debug(f"Parsed {len(polygons)} polygons from async result")

                                        # Create storage item
                                        image_path_str = image_row['image_path']
                                        original_image_path = image_path_str if image_path_str.startswith('s3://') else str(image_path_str)
                                        image_filename = Path(image_path_str).name if image_path_str.startswith('s3://') else Path(image_path_str).name
                                        
                                        result = OCRStorageItem(
                                            id=f"{dataset_name}_pseudo_{Path(image_path_str).stem}",
                                            split="pseudo",
                                            image_path=original_image_path,
                                            image_filename=image_filename,
                                            width=int(image_row.get('width', 0)),
                                            height=int(image_row.get('height', 0)),
                                            polygons=polygons,
                                            texts=texts,
                                            labels=labels,
                                            metadata={"source": "upstage_api_async", "enhanced": False}
                                        )
                                        
                                        logger.debug(f"Completed: {request_id}")
                                        return result
                                    else:
                                        logger.error(f"Failed to download result for {request_id}: {result_response.status}")
                                        return None
                            else:
                                logger.error(f"No download_url in completed request {request_id}")
                                return None
                                
                        elif status == 'failed':
                            failure_msg = status_data.get('failure_message', 'Unknown error')
                            logger.error(f"Request {request_id} failed: {failure_msg}")
                            return None
                        # else: still processing, continue polling
                        
                    elif response.status == 429:
                        logger.warning(f"Rate limited (poll): {request_id}")
                        await asyncio.sleep(POLL_DELAY * 2)
                        continue
                    else:
                        error_text = await response.text()
                        logger.error(f"Poll error {response.status} for {request_id}: {error_text[:200]}")
                        return None
                        
            except Exception as e:
                logger.error(f"Error polling {request_id}: {e}")
                await asyncio.sleep(POLL_DELAY)
                continue
        
        logger.error(f"Timeout waiting for {request_id}")
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
        """Process a single image using async API (submit + poll)."""
        # Phase 1: Submit
        request_id = await self.submit_image_async(session, semaphore, image_row, retry_count, max_retries)
        if not request_id:
            return None
        
        # Phase 2: Poll and get result
        return await self.poll_and_get_result(session, request_id, image_row, dataset_name)

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
