#!/usr/bin/env python3
"""Inspect pseudo-label output files to check for text and labels."""

import pandas as pd
from pathlib import Path

def inspect_file(filepath: Path):
    """Inspect a parquet file and report its contents."""
    print(f"\n{'='*80}")
    print(f"Inspecting: {filepath.name}")
    print(f"{'='*80}")
    
    df = pd.read_parquet(filepath)
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Check for empty arrays
    if 'polygons' in df.columns:
        empty_polygons = df['polygons'].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
        total_polygons = df['polygons'].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
        print(f"\nPolygons:")
        print(f"  Rows with empty polygons: {empty_polygons}/{len(df)}")
        print(f"  Total polygon items: {total_polygons}")
        
    if 'texts' in df.columns:
        def check_empty(x):
            try:
                if hasattr(x, 'size'):  # numpy array
                    return x.size == 0
                elif isinstance(x, list):
                    return len(x) == 0
                else:
                    return True
            except:
                return True
        
        def get_length(x):
            try:
                if hasattr(x, 'size'):  # numpy array
                    return x.size
                elif isinstance(x, list):
                    return len(x)
                else:
                    return 0
            except:
                return 0
        
        empty_texts = df['texts'].apply(check_empty).sum()
        total_texts = df['texts'].apply(get_length).sum()
        print(f"\nTexts:")
        print(f"  Rows with empty texts: {empty_texts}/{len(df)}")
        print(f"  Total text items: {total_texts}")
        
        # Find first row with texts
        found = False
        for idx, row in df.iterrows():
            texts_val = row['texts']
            try:
                if hasattr(texts_val, 'size'):  # numpy array
                    if texts_val.size > 0:
                        print(f"\n  First row with texts (row {idx}):")
                        print(f"    Image: {row.get('image_filename', 'N/A')}")
                        print(f"    Texts: {list(texts_val[:5])}... (showing first 5 of {texts_val.size})")
                        found = True
                        break
                elif isinstance(texts_val, list) and len(texts_val) > 0:
                    print(f"\n  First row with texts (row {idx}):")
                    print(f"    Image: {row.get('image_filename', 'N/A')}")
                    print(f"    Texts: {texts_val[:5]}... (showing first 5 of {len(texts_val)})")
                    found = True
                    break
            except Exception as e:
                continue
        if not found:
            print(f"\n  ⚠️  NO ROWS WITH TEXTS FOUND!")
            
    if 'labels' in df.columns:
        empty_labels = df['labels'].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
        total_labels = df['labels'].apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
        print(f"\nLabels:")
        print(f"  Rows with empty labels: {empty_labels}/{len(df)}")
        print(f"  Total label items: {total_labels}")
    
    # Check metadata
    if 'metadata' in df.columns:
        print(f"\nMetadata sample:")
        sample_meta = df.iloc[0]['metadata'] if len(df) > 0 else {}
        print(f"  {sample_meta}")
    
    # Check width/height
    if 'width' in df.columns and 'height' in df.columns:
        zero_dims = ((df['width'] == 0) | (df['height'] == 0)).sum()
        print(f"\nImage dimensions:")
        print(f"  Rows with zero width/height: {zero_dims}/{len(df)}")
        if zero_dims < len(df):
            print(f"  Sample dimensions: {df[df['width'] > 0].iloc[0][['width', 'height']].to_dict() if (df['width'] > 0).any() else 'N/A'}")

if __name__ == "__main__":
    output_dir = Path("data/output")
    
    files = [
        "baseline_train_pseudo_labels.parquet",
        "baseline_val_pseudo_labels.parquet",
        "baseline_test_pseudo_labels.parquet",
        "pseudo_labels_worst_performers_pseudo_labels.parquet",
    ]
    
    for filename in files:
        filepath = output_dir / filename
        if filepath.exists():
            inspect_file(filepath)
        else:
            print(f"\n⚠️  File not found: {filepath}")
