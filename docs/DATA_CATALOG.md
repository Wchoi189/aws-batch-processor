# Data Catalog

## Input Data

### Location
- **Development**: `data/input/sample_10.parquet` (10-image sample)
- **Production**: S3 bucket (`s3://YOUR_BUCKET/data/processed/`)

### Schema: Input Parquet
```
Columns:
- schema_version: str           # "v1.0"
- id: str                       # Unique identifier
- split: str                    # "train" or "val"
- image_path: str               # S3 URI: s3://bucket/images/file.jpg
- image_filename: str           # Original filename
- width: int                    # Image width (pixels)
- height: int                    # Image height (pixels)
- polygons: list[list[list[float]]]  # Existing (if any)
- texts: list[str]              # Existing (if any)
- labels: list[str]             # Existing (if any)
- metadata: dict                # Source info
```

**Example Row**:
```json
{
  "schema_version": "v1.0",
  "id": "baseline_train_img_000003",
  "split": "train",
  "image_path": "s3://ocr-batch-processing/images/img_000003.jpg",
  "image_filename": "img_000003.jpg",
  "width": 1200,
  "height": 1600,
  "polygons": [],
  "texts": [],
  "labels": [],
  "metadata": {"original_source": "baseline_dataset"}
}
```

## Output Data

### Schema: OCRStorageItem (Pydantic)
```python
class OCRStorageItem(BaseModel):
    schema_version: str = "v1.0"
   id: str
    split: str
    image_path: str
    image_filename: str
    width: int
    height: int
    polygons: list[list[list[float]]]  # [[x, y], [x, y], ...]
    texts: list[str]
    labels: list[str]
    metadata: dict
```

### Output Location
- **S3**: `s3://YOUR_BUCKET/data/processed/{dataset_name}_pseudo_labels.parquet`
- **Checkpoints**: `s3://YOUR_BUCKET/checkpoints/{dataset_name}_batch_XXXX.parquet`

## Image Storage

### Requirements
- Images must be in S3 before processing
- Referenced in parquet `image_path` as S3 URIs
- Format: JPG, PNG
- Max size: Recommended < 10MB

### Upload Images
```bash
aws s3 sync local/images/ s3://YOUR_BUCKET/images/
```

## Processing Flow

1. **Input**: Download parquet from S3
2. **Process**: For each row:
   - Download image from S3 (image_path)
   - Call Upstage API
   - Parse response into polygons/texts
   - Create OCRStorageItem
3. **Checkpoint**: Every 500 images → save to S3
4. **Output**: Final parquet → upload to S3

## Data Validation

**Before upload to S3**:
```python
import pandas as pd

df = pd.read_parquet('input.parquet')
assert 'image_path' in df.columns
assert all(df['image_path'].str.startswith('s3://'))
assert 'image_filename' in df.columns
```

**After processing**:
```python
df_out = pd.read_parquet('output.parquet')
assert 'polygons' in df_out.columns
assert 'texts' in df_out.columns
assert len(df_out) > 0
```

## API Response Structure

Upstage Document Parse API returns:
```json
{
  "pages": [{
    "words": [{
      "text": "Invoice",
      "boundingBox": {
        "vertices": [
          {"x": 100.5, "y": 50.2},
          {"x": 200.5, "y": 50.2},
          {"x": 200.5, "y": 80.2},
          {"x": 100.5, "y": 80.2}
        ]
      }
    }]
  }]
}
```

Converted to:
```python
polygons = [[[100.5, 50.2], [200.5, 50.2], [200.5, 80.2], [100.5, 80.2]]]
texts = ["Invoice"]
labels = ["text"]  # Generic label
```
