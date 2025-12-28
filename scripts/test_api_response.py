#!/usr/bin/env python3
"""Test Upstage async API to see actual response structure."""

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp

# Get API key from environment
API_KEY = os.getenv("UPSTAGE_API_KEY")
if not API_KEY:
    print("ERROR: UPSTAGE_API_KEY not set")
    sys.exit(1)

API_URL_SUBMIT = "https://api.upstage.ai/v1/document-digitization/async"
API_URL_STATUS = "https://api.upstage.ai/v1/document-digitization/requests"

async def test_api():
    """Test the async API with a sample image."""
    # Find a test image
    test_image = None
    for path in [
        "data/input/sample_10.parquet",
        "../data/datasets/images/train",
        "../data/datasets/images/val",
    ]:
        p = Path(path)
        if p.exists():
            if p.is_file() and p.suffix == ".parquet":
                import pandas as pd
                df = pd.read_parquet(p)
                if len(df) > 0 and 'image_path' in df.columns:
                    test_image = df.iloc[0]['image_path']
                    break
            elif p.is_dir():
                images = list(p.glob("*.jpg")) + list(p.glob("*.png"))
                if images:
                    test_image = str(images[0])
                    break
    
    if not test_image:
        print("ERROR: No test image found")
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
            print(f"Response: {json.dumps(result, indent=2)}")
        
        # Step 2: Poll for result
        print("\n=== Step 2: Polling for result ===")
        max_wait = 300  # 5 minutes
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < max_wait:
            await asyncio.sleep(5)
            
            async with session.get(f"{API_URL_STATUS}/{request_id}", headers=headers) as status_response:
                if status_response.status != 200:
                    print(f"ERROR: Status check failed with status {status_response.status}")
                    print(await status_response.text())
                    continue
                
                status_data = await status_response.json()
                status = status_data.get('status')
                print(f"Status: {status}")
                
                if status == 'completed':
                    download_url = status_data.get('download_url')
                    if download_url:
                        print(f"\n=== Step 3: Downloading result ===")
                        print(f"Download URL: {download_url}")
                        
                        async with session.get(download_url) as result_response:
                            if result_response.status == 200:
                                api_result = await result_response.json()
                                
                                print("\n=== ACTUAL API RESPONSE STRUCTURE ===")
                                print(json.dumps(api_result, indent=2)[:2000])  # First 2000 chars
                                
                                # Analyze structure
                                print("\n=== RESPONSE ANALYSIS ===")
                                print(f"Type: {type(api_result)}")
                                if isinstance(api_result, dict):
                                    print(f"Top-level keys: {list(api_result.keys())}")
                                    
                                    # Check for pages
                                    if 'pages' in api_result:
                                        pages = api_result['pages']
                                        print(f"Pages type: {type(pages)}, length: {len(pages) if isinstance(pages, (list, dict)) else 'N/A'}")
                                        if isinstance(pages, list) and len(pages) > 0:
                                            print(f"First page keys: {list(pages[0].keys()) if isinstance(pages[0], dict) else 'Not a dict'}")
                                            if isinstance(pages[0], dict) and 'words' in pages[0]:
                                                words = pages[0]['words']
                                                print(f"Words type: {type(words)}, length: {len(words) if isinstance(words, list) else 'N/A'}")
                                                if isinstance(words, list) and len(words) > 0:
                                                    print(f"First word keys: {list(words[0].keys()) if isinstance(words[0], dict) else 'Not a dict'}")
                                                    if isinstance(words[0], dict):
                                                        print(f"First word: {json.dumps(words[0], indent=2)}")
                                    
                                    # Check for other possible structures
                                    for key in ['result', 'data', 'output', 'document']:
                                        if key in api_result:
                                            print(f"\nFound '{key}' key: {type(api_result[key])}")
                                
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
