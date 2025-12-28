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
import re
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
API_URL_SUBMIT_DOCUMENT_PARSE = "https://api.upstage.ai/v1/document-digitization/async"
API_URL_STATUS_DOCUMENT_PARSE = "https://api.upstage.ai/v1/document-digitization/requests"
# Prebuilt Extraction uses synchronous API (no async/polling needed)
API_URL_PREBUILT_EXTRACTION = "https://api.upstage.ai/v1/information-extraction"
DEFAULT_CONCURRENCY = 3  # Reduced concurrency to better respect rate limits
DEFAULT_BATCH_SIZE = 500
REQUEST_DELAY = 0.05  # 50ms delay for submission (faster since we're not waiting)
POLL_DELAY = 5.0  # 5s delay between polling checks (conservative to avoid rate limits)
POLL_CONCURRENCY = 2  # Limit concurrent polling requests to avoid overwhelming API
POLL_MAX_WAIT = 300  # Max 5 minutes wait per request

# Backward compatibility
API_URL_SUBMIT = API_URL_SUBMIT_DOCUMENT_PARSE
API_URL_STATUS = API_URL_STATUS_DOCUMENT_PARSE


class ResumableBatchProcessor:
    """Process images in resumable batches with checkpointing."""

    def __init__(
        self,
        api_key: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        batch_size: int = DEFAULT_BATCH_SIZE,
        checkpoint_dir: Path | None = None,
        s3_client=None,
        api_type: str = "document-parse",
    ):
        self.api_key = api_key
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.checkpoint_dir = checkpoint_dir
        self.s3_client = s3_client  # Optional boto3 S3 client for downloading images
        self.api_type = api_type  # "document-parse" or "prebuilt-extraction"

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

                # Select API endpoint and method based on api_type
                headers = {"Authorization": f"Bearer {self.api_key}"}
                data = aiohttp.FormData()
                data.add_field('document', image_bytes, filename=image_path.name)
                
                if self.api_type == "prebuilt-extraction":
                    # Prebuilt Extraction uses synchronous API
                    submit_url = API_URL_PREBUILT_EXTRACTION
                    data.add_field('model', 'receipt-extraction')
                else:
                    # Document Parse uses async API
                    submit_url = API_URL_SUBMIT_DOCUMENT_PARSE
                    data.add_field('model', 'document-parse')

                async with session.post(submit_url, headers=headers, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        # Prebuilt Extraction returns results directly (synchronous)
                        if self.api_type == "prebuilt-extraction":
                            # Store result for direct processing (no polling needed)
                            # We'll return a special marker and process immediately
                            logger.info(f"✓ Prebuilt Extraction completed: {image_path.name}")
                            if temp_file_path and temp_file_path.exists():
                                temp_file_path.unlink()
                            # Return result dict wrapped in a way we can identify it
                            return {"type": "prebuilt-extraction", "result": result}
                        
                        # Document Parse returns request_id (async)
                        request_id = result.get('request_id')
                        if request_id:
                            logger.info(f"✓ Submitted: {image_path.name} → {request_id}")
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
        poll_semaphore: asyncio.Semaphore | None = None,
    ) -> OCRStorageItem | None:
        """Poll for async result and download when ready."""
        start_time = time.time()
        headers = {"Authorization": f"Bearer {self.api_key}"}
        retry_count = 0  # Track consecutive rate limits for exponential backoff
        
        while time.time() - start_time < POLL_MAX_WAIT:
            await asyncio.sleep(POLL_DELAY)
            
            # Use semaphore to limit concurrent polling requests
            if poll_semaphore:
                await poll_semaphore.acquire()
            
            try:
                # Prebuilt Extraction doesn't use polling (synchronous API)
                # This should not be called for prebuilt-extraction, but handle it gracefully
                if self.api_type == "prebuilt-extraction":
                    logger.warning(f"Polling called for prebuilt-extraction (should not happen)")
                    if poll_semaphore:
                        poll_semaphore.release()
                    return None
                
                # Document Parse uses async polling
                status_url = f"{API_URL_STATUS_DOCUMENT_PARSE}/{request_id}"
                async with session.get(status_url, headers=headers) as response:
                    if response.status == 200:
                        # Reset retry count on successful response
                        retry_count = 0
                        status_data = await response.json()
                        status = status_data.get('status')
                        
                        if status == 'completed':
                            # Get download_url from batches (async API structure)
                            batches = status_data.get('batches', [])
                            if batches and isinstance(batches, list) and len(batches) > 0:
                                first_batch = batches[0]
                                if isinstance(first_batch, dict):
                                    download_url = first_batch.get('download_url')
                            else:
                                download_url = status_data.get('download_url')
                            
                            if download_url:
                                
                                # Download result
                                async with session.get(download_url) as result_response:
                                    if result_response.status == 200:
                                        api_result = await result_response.json()
                                        
                                        # Parse API response - async API may have different structure
                                        polygons = []
                                        texts = []
                                        labels = []

                                        # Log actual response structure for debugging
                                        logger.debug(f"API response type: {type(api_result)} (API type: {self.api_type})")
                                        if isinstance(api_result, dict):
                                            logger.debug(f"API response keys: {list(api_result.keys())}")
                                        
                                        # Upstage async API returns structure: {elements: [...], content: {...}, ocr: bool}
                                        # Elements have: coordinates (normalized 0-1), content.text/html, category
                                        # Prebuilt Extraction may have similar or different structure
                                        
                                        elements = []
                                        
                                        if isinstance(api_result, dict):
                                            # Format 1: elements array (async API format)
                                            elements = api_result.get('elements', [])
                                            
                                            # Format 2: Try pages (sync API format) for backward compatibility
                                            if not elements:
                                                pages = api_result.get('pages', [])
                                                if pages:
                                                    # Convert pages format to elements format
                                                    for page in pages:
                                                        if isinstance(page, dict):
                                                            words = page.get('words', [])
                                                            elements.extend(words)
                                            
                                            # Format 3: Nested structure
                                            if not elements and 'result' in api_result:
                                                result_data = api_result['result']
                                                if isinstance(result_data, dict):
                                                    elements = result_data.get('elements', result_data.get('pages', []))
                                                elif isinstance(result_data, list):
                                                    elements = result_data
                                        
                                        # Format 4: Check if it's a list directly
                                        if not elements and isinstance(api_result, list):
                                            elements = api_result
                                        
                                        logger.debug(f"Found {len(elements)} elements")
                                        
                                        # Get image dimensions for coordinate conversion
                                        width = int(image_row.get('width', 0))
                                        height = int(image_row.get('height', 0))
                                        
                                        # Parse elements
                                        for element in elements:
                                            if not isinstance(element, dict):
                                                continue
                                            
                                            # Get coordinates (normalized 0-1 in async API)
                                            coords = element.get('coordinates', [])
                                            
                                            # Also try other formats for backward compatibility
                                            if not coords:
                                                if 'boundingBox' in element:
                                                    bbox_obj = element['boundingBox']
                                                    if isinstance(bbox_obj, dict):
                                                        coords = bbox_obj.get('vertices', [])
                                                    elif isinstance(bbox_obj, list):
                                                        coords = bbox_obj
                                            
                                            if not coords:
                                                coords = element.get('bbox', element.get('polygon', []))
                                            
                                            if coords and isinstance(coords, list) and len(coords) >= 3:
                                                try:
                                                    # Convert coordinates to polygon
                                                    if isinstance(coords[0], dict):
                                                        # Format: [{"x": 0.2447, "y": 0.0714}, ...] (normalized)
                                                        poly = [[float(v.get('x', 0)), float(v.get('y', 0))] for v in coords]
                                                    elif isinstance(coords[0], (list, tuple)):
                                                        # Format: [[x, y], ...]
                                                        poly = [[float(v[0]), float(v[1])] for v in coords]
                                                    else:
                                                        continue
                                                    
                                                    # Convert normalized coordinates (0-1) to pixel coordinates if needed
                                                    # Check if coordinates are normalized (all values < 1.0)
                                                    is_normalized = all(0 <= p[0] <= 1.0 and 0 <= p[1] <= 1.0 for p in poly)
                                                    
                                                    if is_normalized and width > 0 and height > 0:
                                                        # Convert to pixel coordinates
                                                        poly = [[p[0] * width, p[1] * height] for p in poly]
                                                    
                                                    if len(poly) >= 3:
                                                        polygons.append(poly)
                                                        
                                                        # Get text from content
                                                        text = ''
                                                        content = element.get('content', {})
                                                        if isinstance(content, dict):
                                                            text = content.get('text', '')
                                                            # If text is empty, try to extract from HTML
                                                            if not text:
                                                                html = content.get('html', '')
                                                                if html:
                                                                    # Simple HTML text extraction (remove tags)
                                                                    import re
                                                                    text = re.sub(r'<[^>]+>', ' ', html)
                                                                    text = ' '.join(text.split())
                                                        elif isinstance(content, str):
                                                            text = content
                                                        
                                                        # Fallback to direct text field
                                                        if not text:
                                                            text = element.get('text', element.get('content', ''))
                                                        
                                                        texts.append(text)
                                                        
                                                        # Get label/category
                                                        label = element.get('category', element.get('label', 'text'))
                                                        labels.append(label)
                                                        
                                                except (ValueError, IndexError, TypeError) as e:
                                                    logger.warning(f"Error parsing element coordinates: {e}")
                                                    continue
                                        
                                        logger.info(f"Parsed {len(polygons)} polygons from async result for {request_id}")
                                        
                                        # Extract image dimensions from API response if available
                                        width = int(image_row.get('width', 0))
                                        height = int(image_row.get('height', 0))
                                        
                                        if width == 0 or height == 0:
                                            # Try to get from API response
                                            if isinstance(api_result, dict):
                                                if 'width' in api_result:
                                                    width = int(api_result.get('width', 0))
                                                if 'height' in api_result:
                                                    height = int(api_result.get('height', 0))
                                                
                                                # Check pages for dimensions
                                                if (width == 0 or height == 0) and 'pages' in api_result and len(api_result['pages']) > 0:
                                                    first_page = api_result['pages'][0]
                                                    if isinstance(first_page, dict):
                                                        if 'width' in first_page:
                                                            width = int(first_page.get('width', 0))
                                                        if 'height' in first_page:
                                                            height = int(first_page.get('height', 0))
                                        
                                        # CRITICAL: Log warning if no polygons found
                                        if len(polygons) == 0:
                                            logger.error(f"⚠️  NO POLYGONS PARSED for {request_id}!")
                                            logger.error(f"   Image: {image_row.get('image_filename', 'unknown')}")
                                            logger.error(f"   Response type: {type(api_result)}")
                                            if isinstance(api_result, dict):
                                                logger.error(f"   Response keys: {list(api_result.keys())}")
                                                # Log sample of response structure
                                                import json
                                                try:
                                                    response_sample = json.dumps(api_result, indent=2)[:2000]
                                                    logger.error(f"   Response sample:\n{response_sample}")
                                                except:
                                                    logger.error(f"   Response (str): {str(api_result)[:1000]}")
                                            
                                            # Still return the result (with empty polygons) so we can track failures
                                            # But log it as an error

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
                                            metadata={"source": f"upstage_api_async_{self.api_type}", "enhanced": False, "api_type": self.api_type}
                                        )
                                        
                                        logger.info(f"✓ Completed: {request_id}")
                                        # Release semaphore before returning
                                        if poll_semaphore:
                                            poll_semaphore.release()
                                        return result
                                    else:
                                        logger.error(f"Failed to download result for {request_id}: {result_response.status}")
                                        if poll_semaphore:
                                            poll_semaphore.release()
                                        return None
                            else:
                                logger.error(f"No download_url in completed request {request_id}")
                                if poll_semaphore:
                                    poll_semaphore.release()
                                return None
                                
                        elif status == 'failed':
                            failure_msg = status_data.get('failure_message', 'Unknown error')
                            logger.error(f"Request {request_id} failed: {failure_msg}")
                            if poll_semaphore:
                                poll_semaphore.release()
                            return None
                        # else: still processing, continue polling
                        # Semaphore will be released after this iteration completes
                        
                    elif response.status == 429:
                        # Check for Retry-After header (in seconds)
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            try:
                                wait_time = int(retry_after)
                            except ValueError:
                                wait_time = POLL_DELAY * 4  # Default to 8s if header invalid
                        else:
                            # Exponential backoff: 4s, 8s, 16s, 32s, max 60s
                            wait_time = min(POLL_DELAY * (2 ** min(5, retry_count + 1)), 60)
                        
                        retry_count += 1
                        logger.warning(f"Rate limited (poll): {request_id}, waiting {wait_time}s (attempt {retry_count})")
                        # Release semaphore before waiting
                        if poll_semaphore:
                            poll_semaphore.release()
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        error_text = await response.text()
                        logger.error(f"Poll error {response.status} for {request_id}: {error_text[:200]}")
                        if poll_semaphore:
                            poll_semaphore.release()
                        return None
                    
                    # Release semaphore after each polling attempt (for "still processing" case)
                    if poll_semaphore:
                        poll_semaphore.release()
                        
            except Exception as e:
                logger.error(f"Error polling {request_id}: {e}")
                if poll_semaphore:
                    poll_semaphore.release()
                await asyncio.sleep(POLL_DELAY)
                continue
        
        logger.error(f"Timeout waiting for {request_id}")
        return None

    def _parse_prebuilt_extraction_result(self, api_result: dict, image_row: dict) -> tuple[list, list, list]:
        """Parse Prebuilt Extraction API response (fields structure) into polygons, texts, labels."""
        polygons = []
        texts = []
        labels = []
        
        fields = api_result.get('fields', [])
        
        # Get image dimensions
        width = int(image_row.get('width', 0))
        height = int(image_row.get('height', 0))
        
        # Try to get dimensions from metadata
        if (width == 0 or height == 0) and 'metadata' in api_result:
            pages = api_result.get('metadata', {}).get('pages', [])
            if pages and len(pages) > 0:
                width = int(pages[0].get('width', 0))
                height = int(pages[0].get('height', 0))
        
        # Extract text from fields
        for field in fields:
            if not isinstance(field, dict):
                continue
            
            field_type = field.get('type', '')
            field_value = field.get('value', '')
            refined_value = field.get('refinedValue', '')
            field_key = field.get('key', '')
            
            # Use refined value if available, otherwise use value
            text = refined_value if refined_value else field_value
            
            if text:
                texts.append(text)
                
                # Create label from key (e.g., "store.store_name" -> "store_name")
                if field_key:
                    label_parts = field_key.split('.')
                    label = label_parts[-1] if len(label_parts) > 1 else field_key
                else:
                    label = field_type if field_type else 'text'
                labels.append(label)
                
                # For Prebuilt Extraction, we don't have coordinate information
                # Create a placeholder polygon (full image or empty)
                # In practice, you might want to use OCR coordinates if available
                # For now, we'll use empty polygons since coordinates aren't in the response
                polygons.append([])
            
            # Also process properties if it's a group
            if field_type == 'group' and 'properties' in field:
                properties = field.get('properties', [])
                for prop in properties:
                    if isinstance(prop, dict):
                        prop_value = prop.get('refinedValue', prop.get('value', ''))
                        if prop_value:
                            texts.append(prop_value)
                            prop_key = prop.get('key', '')
                            if prop_key:
                                label_parts = prop_key.split('.')
                                label = label_parts[-1] if len(label_parts) > 1 else prop_key
                            else:
                                label = prop.get('type', 'text')
                            labels.append(label)
                            polygons.append([])
        
        return polygons, texts, labels

    async def process_single_image(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        image_row: dict,
        dataset_name: str,
        poll_semaphore: asyncio.Semaphore | None = None,
        retry_count: int = 0,
        max_retries: int = 3,
    ) -> OCRStorageItem | None:
        """Process a single image using async API (submit + poll) or sync API (prebuilt extraction)."""
        # Phase 1: Submit
        submit_result = await self.submit_image_async(session, semaphore, image_row, retry_count, max_retries)
        if not submit_result:
            return None
        
        # Prebuilt Extraction returns results directly (synchronous)
        if isinstance(submit_result, dict) and submit_result.get('type') == 'prebuilt-extraction':
            api_result = submit_result.get('result', {})
            
            # Parse Prebuilt Extraction response
            polygons, texts, labels = self._parse_prebuilt_extraction_result(api_result, image_row)
            
            # Get image dimensions
            width = int(image_row.get('width', 0))
            height = int(image_row.get('height', 0))
            if (width == 0 or height == 0) and 'metadata' in api_result:
                pages = api_result.get('metadata', {}).get('pages', [])
                if pages and len(pages) > 0:
                    width = int(pages[0].get('width', 0))
                    height = int(pages[0].get('height', 0))
            
            # Create storage item
            image_path_str = image_row['image_path']
            original_image_path = image_path_str if image_path_str.startswith('s3://') else str(image_path_str)
            image_filename = Path(image_path_str).name if image_path_str.startswith('s3://') else Path(image_path_str).name
            
            result = OCRStorageItem(
                id=f"{dataset_name}_pseudo_{Path(image_path_str).stem}",
                split="pseudo",
                image_path=original_image_path,
                image_filename=image_filename,
                width=width,
                height=height,
                polygons=polygons,
                texts=texts,
                labels=labels,
                metadata={"source": f"upstage_api_prebuilt_extraction", "enhanced": False, "api_type": self.api_type}
            )
            
            logger.info(f"✓ Prebuilt Extraction completed: {image_filename} ({len(texts)} fields extracted)")
            return result
        
        # Document Parse uses async API - poll for result
        request_id = submit_result
        return await self.poll_and_get_result(session, request_id, image_row, dataset_name, poll_semaphore)

    async def process_batch(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        start_idx: int = 0,
    ) -> list[OCRStorageItem]:
        """Process a batch of images asynchronously."""
        results = []
        semaphore = asyncio.Semaphore(self.concurrency)
        # Separate semaphore for polling to limit concurrent polling requests
        poll_semaphore = asyncio.Semaphore(POLL_CONCURRENCY)

        async with aiohttp.ClientSession() as session:
            # Create tasks with image_row tracking
            tasks = []
            image_rows = []
            for idx, row in df.iloc[start_idx:].iterrows():
                row_dict = row.to_dict()
                image_rows.append((idx, row_dict))
                task = self.process_single_image(
                    session, semaphore, row_dict, dataset_name, poll_semaphore
                )
                tasks.append(task)

            # Process with progress bar and track which image each result belongs to
            completed_tasks = {}
            for coro in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Processing batch from {start_idx}"
            ):
                result = await coro
                # Find which image this result belongs to by matching request
                # We'll handle this differently - create a mapping
                if result:
                    results.append(result)
                else:
                    # Find the corresponding image_row for this failed task
                    # Since as_completed doesn't preserve order, we need to track differently
                    pass
            
            # Create entries for failed images
            # Get all image IDs that succeeded
            successful_ids = {result.id for result in results}
            
            # Create empty entries for failed images
            for idx, row_dict in image_rows:
                image_path_str = row_dict.get('image_path', '')
                image_id = f"{dataset_name}_pseudo_{Path(image_path_str).stem if image_path_str else idx}"
                
                if image_id not in successful_ids:
                    # Create empty result for failed image
                    original_image_path = image_path_str if image_path_str.startswith('s3://') else str(image_path_str)
                    image_filename = Path(image_path_str).name if image_path_str.startswith('s3://') else Path(image_path_str).name if image_path_str else f"image_{idx}"
                    
                    failed_result = OCRStorageItem(
                        id=image_id,
                        split="pseudo",
                        image_path=original_image_path,
                        image_filename=image_filename,
                        width=int(row_dict.get('width', 0)),
                        height=int(row_dict.get('height', 0)),
                        polygons=[],
                        texts=[],
                        labels=[],
                        metadata={"source": f"upstage_api_async_{self.api_type}", "enhanced": False, "status": "failed", "api_type": self.api_type}
                    )
                    results.append(failed_result)

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
