#!/usr/bin/env python3
"""Test Upstage async API locally to see actual response structure."""

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp

# Get API key
API_KEY = os.getenv("UPSTAGE_API_KEY", "").strip()
if not API_KEY:
    # Try loading from .env.local
    env_local = Path(".env.local")
    if env_local.exists():
        with open(env_local) as f:
            for line in f:
                line = line.strip()
                if line.startswith("UPSTAGE_API_KEY="):
                    API_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                    print("Loaded UPSTAGE_API_KEY from .env.local")
                    break

if not API_KEY:
    print("ERROR: UPSTAGE_API_KEY not set")
    sys.exit(1)

API_URL_SUBMIT_DOCUMENT_PARSE = "https://api.upstage.ai/v1/document-digitization/async"
API_URL_STATUS_DOCUMENT_PARSE = "https://api.upstage.ai/v1/document-digitization/requests"
# Prebuilt Extraction uses synchronous API (no async/polling)
API_URL_PREBUILT_EXTRACTION = "https://api.upstage.ai/v1/information-extraction"

async def test_api(test_image, api_type="document-parse"):
    """Test the async API with a sample image."""
    # Select API endpoints based on type
    if api_type == "prebuilt-extraction":
        API_URL_SUBMIT = API_URL_PREBUILT_EXTRACTION
        API_URL_STATUS = None  # Synchronous API, no polling needed
        is_sync = True
    else:
        API_URL_SUBMIT = API_URL_SUBMIT_DOCUMENT_PARSE
        API_URL_STATUS = API_URL_STATUS_DOCUMENT_PARSE
        is_sync = False
    
    print(f"Testing API type: {api_type}")
    print(f"Submit URL: {API_URL_SUBMIT}")
    if API_URL_STATUS:
        print(f"Status URL: {API_URL_STATUS}")
    else:
        print("API Type: Synchronous (no polling needed)")

    # Find a test image if not provided
    if not test_image:
        test_paths = [
            "../data/datasets/images/train",
            "../data/datasets/images/val",
            "../data/datasets/images/test",
            "data/input",
        ]

        for base_path in test_paths:
            p = Path(base_path)
            if p.exists():
                if p.is_file() and p.suffix in [".jpg", ".png", ".jpeg"]:
                    test_image = str(p)
                    break
                elif p.is_dir():
                    for ext in ["*.jpg", "*.png", "*.jpeg"]:
                        images = list(p.glob(ext))
                        if images:
                            test_image = str(images[0])
                            break
                    if test_image:
                        break

    if not test_image:
        print("ERROR: No test image found. Please provide an image path.")
        print("Usage: python scripts/test_api_local.py [image_path] [--api-type document-parse|prebuilt-extraction]")
        sys.exit(1)

    print(f"Using test image: {test_image}")

    # Read image
    image_path = Path(test_image)
    if not image_path.exists():
        print(f"ERROR: Image not found: {image_path}")
        sys.exit(1)

    async with aiohttp.ClientSession() as session:
        # Step 1: Submit
        print("\n=== Step 1: Submitting image ===")
        headers = {"Authorization": f"Bearer {API_KEY}"}
        with open(image_path, 'rb') as f:
            image_bytes = f.read()

        print(f"Image size: {len(image_bytes)} bytes")

        data = aiohttp.FormData()
        data.add_field('document', image_bytes, filename=image_path.name)
        # Add model field based on API type
        if api_type == "document-parse":
            data.add_field('model', 'document-parse')
        elif api_type == "prebuilt-extraction":
            data.add_field('model', 'receipt-extraction')

        async with session.post(API_URL_SUBMIT, headers=headers, data=data) as response:
            if response.status != 200:
                print(f"ERROR: Submit failed with status {response.status}")
                print(await response.text())
                sys.exit(1)

            result = await response.json()
            
            # Prebuilt Extraction returns results directly (synchronous)
            if is_sync:
                print(f"✓ Prebuilt Extraction completed (synchronous)")
                api_result = result
                # Skip to result processing
                download_url = None  # Not needed for sync API
            else:
                # Document Parse returns request_id (async)
                request_id = result.get('request_id')
                print(f"✓ Submitted. Request ID: {request_id}")
                api_result = None
                download_url = None

        # Step 2: Poll for result (only for async API)
        if not is_sync:
            print("\n=== Step 2: Polling for result ===")
            max_wait = 300  # 5 minutes
            start_time = asyncio.get_event_loop().time()
            poll_count = 0

            while (asyncio.get_event_loop().time() - start_time) < max_wait:
                await asyncio.sleep(5)
                poll_count += 1

                async with session.get(f"{API_URL_STATUS}/{request_id}", headers=headers) as status_response:
                    if status_response.status != 200:
                        print(f"ERROR: Status check failed with status {status_response.status}")
                        print(await status_response.text())
                        continue

                    status_data = await status_response.json()
                    status = status_data.get('status')
                    print(f"Poll {poll_count}: Status = {status}")

                    if status == 'completed':
                        print(f"\nStatus response keys: {list(status_data.keys())}")

                        # Check for download_url in batches
                        download_url = status_data.get('download_url')
                        if not download_url and 'batches' in status_data:
                            batches = status_data['batches']
                            if isinstance(batches, list) and len(batches) > 0:
                                first_batch = batches[0]
                                if isinstance(first_batch, dict):
                                    download_url = first_batch.get('download_url')
                                    print(f"Found download_url in batches[0]")

                        if download_url:
                            print(f"\n=== Step 3: Downloading result ===")
                            print(f"Download URL: {download_url}")

                            async with session.get(download_url) as result_response:
                                if result_response.status == 200:
                                    api_result = await result_response.json()
                                    
                                    # Process result
                                    if api_result:
                                        download_url = "processed"  # Mark as processed
                    
                    elif status == 'failed':
                        print(f"ERROR: Request failed: {status_data.get('failure_message', 'Unknown')}")
                        return None
                    
                    # else: still processing, continue polling
            
            if not api_result:
                print("ERROR: Timeout waiting for result")
                return None
        
        # Step 3: Process result (for both sync and async)
        if api_result:
            print(f"\n=== Step 3: Processing result ===")

            print("\n" + "="*80)
            print(f"ACTUAL API RESPONSE STRUCTURE (API Type: {api_type})")
            print("="*80)

            # Save full response to file
            output_file = Path("api_response_sample.json")
            with open(output_file, 'w') as f:
                json.dump(api_result, f, indent=2)
            print(f"\n✓ Full response saved to: {output_file}")

            # Analyze structure
            print("\n=== RESPONSE ANALYSIS ===")
            print(f"Type: {type(api_result)}")

            if isinstance(api_result, dict):
                print(f"Top-level keys: {list(api_result.keys())}")
                
                # Check for Prebuilt Extraction structure
                if 'fields' in api_result:
                    print(f"\n✓ Found 'fields' key (Prebuilt Extraction format)")
                    fields = api_result['fields']
                    print(f"  Type: {type(fields)}, Length: {len(fields) if isinstance(fields, list) else 'N/A'}")
                    if isinstance(fields, list) and len(fields) > 0:
                        print(f"  First field keys: {list(fields[0].keys()) if isinstance(fields[0], dict) else 'N/A'}")
                        print(f"\n  First field structure:")
                        print(json.dumps(fields[0], indent=4))
                
                # Check for Document Parse structure
                if 'pages' in api_result:

                    pages = api_result.get('pages', [])
                    print(f"\n✓ Found 'pages' key (Document Parse format)")
                    print(f"  Type: {type(pages)}")
                    if isinstance(pages, list):
                        print(f"  Length: {len(pages)}")
                        if len(pages) > 0:
                            print(f"  First page type: {type(pages[0])}")
                            if isinstance(pages[0], dict):
                                print(f"  First page keys: {list(pages[0].keys())}")
                                if 'words' in pages[0]:
                                    words = pages[0]['words']
                                    print(f"\n  ✓ Found 'words' in first page")
                                    print(f"    Type: {type(words)}, Length: {len(words) if isinstance(words, list) else 'N/A'}")
                                    if isinstance(words, list) and len(words) > 0:
                                        print(f"    First word type: {type(words[0])}")
                                        if isinstance(words[0], dict):
                                            print(f"    First word keys: {list(words[0].keys())}")
                                            print(f"\n    First word structure:")
                                            print(json.dumps(words[0], indent=4))
                                else:
                                    print(f"  ⚠️  No 'words' key in first page")
                            else:
                                print(f"  First page is not a dict: {pages[0]}")
                    elif isinstance(pages, dict):
                        print(f"  Pages is a dict with keys: {list(pages.keys())}")

                # Check for elements (Document Parse async format)
                if 'elements' in api_result:
                    elements = api_result['elements']
                    print(f"\n✓ Found 'elements' key (Document Parse async format)")
                    print(f"  Type: {type(elements)}, Length: {len(elements) if isinstance(elements, list) else 'N/A'}")

                # Check for other possible structures
                for key in ['result', 'data', 'output', 'document', 'response']:
                    if key in api_result:
                        print(f"\n✓ Found '{key}' key: {type(api_result[key])}")
                        if isinstance(api_result[key], dict):
                            print(f"  Keys: {list(api_result[key].keys())}")

            elif isinstance(api_result, list):
                print(f"\n⚠️  Response is a list (length: {len(api_result)})")
                if len(api_result) > 0:
                    print(f"  First item type: {type(api_result[0])}")
                    if isinstance(api_result[0], dict):
                        print(f"  First item keys: {list(api_result[0].keys())}")

            print("\n" + "="*80)
            print("PARSING TEST")
            print("="*80)

            # Parse based on API type
            if api_type == "prebuilt-extraction":
                # Parse Prebuilt Extraction fields structure
                fields = api_result.get('fields', [])
                print(f"Extracted fields: {len(fields)}")
                
                texts = []
                labels = []
                
                for field in fields:
                    if isinstance(field, dict):
                        value = field.get('refinedValue', field.get('value', ''))
                        if value:
                            texts.append(value)
                            key = field.get('key', '')
                            if key:
                                label_parts = key.split('.')
                                label = label_parts[-1] if len(label_parts) > 1 else key
                            else:
                                label = field.get('type', 'text')
                            labels.append(label)
                
                print(f"\n✓ Parsed {len(texts)} texts from fields")
                if len(texts) > 0:
                    print(f"  Sample texts: {texts[:5]}")
                    print(f"  Sample labels: {labels[:5]}")
                
                print("\n✅ SUCCESS: Prebuilt Extraction parsing works!")
            else:
                # Parse Document Parse structure (existing logic)
                polygons = []
                texts = []
                labels = []
                elements = []

                if isinstance(api_result, dict):
                    elements = api_result.get('elements', [])
                    if not elements:
                        pages = api_result.get('pages', [])
                        if pages:
                            for page in pages:
                                if isinstance(page, dict):
                                    words = page.get('words', [])
                                    elements.extend(words)

                if not elements and isinstance(api_result, list):
                    elements = api_result

                print(f"Extracted elements: {len(elements)}")
                # ... (rest of Document Parse parsing logic would go here)

            return api_result
        else:
            print("ERROR: No result to process")
            return None

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Test Upstage API locally")
    parser.add_argument("image_path", nargs="?", help="Path to test image")
    parser.add_argument("--api-type", type=str, default="document-parse",
                        choices=["document-parse", "prebuilt-extraction"],
                        help="API type to test: 'document-parse' (default) or 'prebuilt-extraction'")
    args = parser.parse_args()
    
    result = asyncio.run(test_api(test_image=args.image_path, api_type=args.api_type))
    if result:
        print("\n✓ Test completed successfully")
    else:
        print("\n✗ Test failed")
        sys.exit(1)
