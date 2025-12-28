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

API_URL_SUBMIT = "https://api.upstage.ai/v1/document-digitization/async"
API_URL_STATUS = "https://api.upstage.ai/v1/document-digitization/requests"

async def test_api():
    """Test the async API with a sample image."""
    # Check command line argument first
    if len(sys.argv) > 1:
        test_image = sys.argv[1]

    # Otherwise find a test image
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
        print("Usage: python scripts/test_api_local.py [image_path]")
        if len(sys.argv) > 1:
            test_image = sys.argv[1]
        else:
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
        data.add_field('model', 'document-parse')

        async with session.post(API_URL_SUBMIT, headers=headers, data=data) as response:
            if response.status != 200:
                print(f"ERROR: Submit failed with status {response.status}")
                print(await response.text())
                sys.exit(1)

            result = await response.json()
            request_id = result.get('request_id')
            print(f"✓ Submitted. Request ID: {request_id}")

        # Step 2: Poll for result
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

                                print("\n" + "="*80)
                                print("ACTUAL API RESPONSE STRUCTURE")
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

                                    # Check for pages
                                    if 'pages' in api_result:
                                        pages = api_result['pages']
                                        print(f"\n✓ Found 'pages' key")
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

                                # Test the NEW parsing logic (elements-based)
                                polygons = []
                                texts = []
                                labels = []

                                elements = []

                                if isinstance(api_result, dict):
                                    # Format 1: elements array (async API format)
                                    elements = api_result.get('elements', [])

                                    # Format 2: Try pages (sync API format) for backward compatibility
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

                                # Get image dimensions (we'll use a test value if not available)
                                test_width = 1000
                                test_height = 1000

                                for element in elements:
                                    if not isinstance(element, dict):
                                        continue

                                    # Get coordinates (normalized 0-1 in async API)
                                    coords = element.get('coordinates', [])

                                    if not coords and 'boundingBox' in element:
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
                                                poly = [[float(v.get('x', 0)), float(v.get('y', 0))] for v in coords]
                                            elif isinstance(coords[0], (list, tuple)):
                                                poly = [[float(v[0]), float(v[1])] for v in coords]
                                            else:
                                                continue

                                            # Convert normalized coordinates (0-1) to pixel coordinates
                                            is_normalized = all(0 <= p[0] <= 1.0 and 0 <= p[1] <= 1.0 for p in poly)

                                            if is_normalized and test_width > 0 and test_height > 0:
                                                poly = [[p[0] * test_width, p[1] * test_height] for p in poly]

                                            if len(poly) >= 3:
                                                polygons.append(poly)

                                                # Get text from content
                                                text = ''
                                                content = element.get('content', {})
                                                if isinstance(content, dict):
                                                    text = content.get('text', '')
                                                    if not text:
                                                        html = content.get('html', '')
                                                        if html:
                                                            import re
                                                            text = re.sub(r'<[^>]+>', ' ', html)
                                                            text = ' '.join(text.split())
                                                elif isinstance(content, str):
                                                    text = content

                                                if not text:
                                                    text = element.get('text', element.get('content', ''))

                                                texts.append(text)

                                                # Get label/category
                                                label = element.get('category', element.get('label', 'text'))
                                                labels.append(label)

                                        except Exception as e:
                                            print(f"    Error parsing element: {e}")
                                            continue

                                print(f"\n✓ Parsed {len(polygons)} polygons")
                                print(f"✓ Parsed {len(texts)} texts")
                                if len(texts) > 0:
                                    print(f"  Sample texts: {texts[:5]}")

                                if len(polygons) == 0:
                                    print("\n⚠️  WARNING: No polygons parsed!")
                                    print("This indicates the parsing logic needs to be fixed.")
                                else:
                                    print("\n✅ SUCCESS: Parsing works correctly!")

                                return api_result
                            else:
                                print(f"ERROR: Download failed with status {result_response.status}")
                                print(await result_response.text())
                                return None
                    else:
                        print("ERROR: No download_url in completed status")
                        return None

                elif status == 'failed':
                    print(f"ERROR: Request failed: {status_data.get('failure_message', 'Unknown')}")
                    return None

                # else: still processing

        print("ERROR: Timeout waiting for result")
        return None

if __name__ == "__main__":
    result = asyncio.run(test_api())
    if result:
        print("\n✓ Test completed successfully")
    else:
        print("\n✗ Test failed")
        sys.exit(1)
