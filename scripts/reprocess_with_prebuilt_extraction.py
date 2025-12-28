#!/usr/bin/env python3
"""Reprocess datasets with Prebuilt Extraction API."""
import sys
import asyncio
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.batch_processor_base import ResumableBatchProcessor
import os

async def reprocess_dataset(dataset_name, batch_size=500, concurrency=3):
    """Reprocess a dataset with Prebuilt Extraction."""
    print(f"\n{'='*80}")
    print(f"Reprocessing {dataset_name} with Prebuilt Extraction API")
    print(f"{'='*80}\n")
    
    input_file = Path(f"data/input/{dataset_name}.parquet")
    output_file = Path(f"data/output/{dataset_name}_pseudo_labels.parquet")
    
    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        return False
    
    # Get API key
    api_key = os.getenv("UPSTAGE_API_KEY")
    if not api_key:
        env_local = Path(".env.local")
        if env_local.exists():
            with open(env_local) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("UPSTAGE_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    
    if not api_key:
        print("ERROR: UPSTAGE_API_KEY not found")
        return False
    
    # Create processor with Prebuilt Extraction
    checkpoint_dir = Path(f"data/checkpoints/{dataset_name}_prebuilt")
    processor = ResumableBatchProcessor(
        api_key=api_key,
        api_type="prebuilt-extraction",
        concurrency=concurrency,
        batch_size=batch_size,
        checkpoint_dir=checkpoint_dir,
    )
    
    # Process dataset
    try:
        await processor.process_dataset(
            parquet_file=input_file,
            output_file=output_file,
            dataset_name=dataset_name,
            resume=False,  # Start fresh
        )
        print(f"\n✓ Successfully reprocessed {dataset_name}")
        return True
    except Exception as e:
        print(f"\n✗ Error reprocessing {dataset_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Main reprocessing function."""
    datasets = ["baseline_val", "baseline_test", "baseline_train"]
    
    if len(sys.argv) > 1:
        datasets = [sys.argv[1]]
    
    for dataset in datasets:
        success = await reprocess_dataset(dataset, batch_size=500, concurrency=3)
        if not success:
            print(f"\n✗ Failed to reprocess {dataset}")
            sys.exit(1)
    
    print("\n" + "="*80)
    print("All datasets reprocessed successfully!")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())
