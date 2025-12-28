"""Storage schemas for OCR and KIE datasets.

These schemas are designed for serialization (JSONL/Parquet) and long-term storage,
bridging the gap between raw datasets and runtime tensor models in ocr.core.validation.

Design Goals:
1. Serialization-friendly: Use standard Python types (list, float, str) instead of numpy/torch.
2. Logic-agnostic: Pure data containers without validation logic (leave that to ocr.core.validation).
3. Versioned: Include schema_version for future evolution.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BaseStorageItem(BaseModel):
    """Base schema for all storage items."""

    model_config = ConfigDict(arbitrary_types_allowed=False)

    schema_version: str = "v1.0"
    id: str  # Unique identifier for the sample
    split: str = "train"  # train, val, test, unsup


class OCRStorageItem(BaseStorageItem):
    """Storage schema for general OCR data (Text Detection + Recognition)."""

    # Image Info
    image_path: str
    image_filename: str
    width: int
    height: int

    # Annotations
    # List of polygons, where each polygon is [ [x1,y1], [x2,y2], ... ]
    polygons: list[list[list[float]]] = Field(default_factory=list)

    # Text content corresponding to each polygon
    texts: list[str] = Field(default_factory=list)

    # Optional labels for detection (e.g., "text", "table", "header")
    labels: list[str] = Field(default_factory=list)

    # Arbitrary metadata
    metadata: dict[str, Any] = Field(default_factory=dict)

    def validate_lengths(self):
        """Ensure parallel lists have matching lengths."""
        if len(self.polygons) != len(self.texts):
            raise ValueError(f"Mismatch: {len(self.polygons)} polygons vs {len(self.texts)} texts")
        if self.labels and len(self.labels) != len(self.polygons):
            raise ValueError(f"Mismatch: {len(self.polygons)} polygons vs {len(self.labels)} labels")


class KIEStorageItem(OCRStorageItem):
    """Storage schema for Key Information Extraction (KIE) data."""

    # KIE Class labels corresponding to each polygon (e.g., "merchant", "total")
    kie_labels: list[str] = Field(default_factory=list)

    # Linking information: List of [source_index, target_index]
    linking: list[list[int]] = Field(default_factory=list)

    def validate_lengths(self):
        super().validate_lengths()
        if len(self.kie_labels) != len(self.polygons):
            raise ValueError(f"Mismatch: {len(self.polygons)} polygons vs {len(self.kie_labels)} kie_labels")
