# Session Handover Document

**Date:** 2025-12-28  
**Project:** AWS Batch OCR Pseudo-Label Processor  
**Status:** Partially Complete - Ready for Prebuilt Extraction Implementation

---

## Current Status Summary

### ✅ Completed & Working

1. **AWS Infrastructure**
   - S3 bucket, ECR, Secrets Manager, IAM roles configured
   - AWS Batch compute environment (Fargate Spot) operational
   - Job definitions and queues set up
   - GitHub Actions CI/CD pipeline working

2. **Core Processing Pipeline**
   - Async API integration implemented and tested
   - Rate limiting improvements (Retry-After, exponential backoff, polling limits)
   - Checkpointing and resume functionality working
   - S3-based image download and result storage

3. **API Response Parsing**
   - Fixed to use `elements` structure (not `pages`)
   - Handles normalized coordinates (0-1) with pixel conversion
   - Extracts text from `content.html` with HTML tag removal
   - Uses `category` as labels

4. **Bug Fixes Applied**
   - Fixed missing images in output (now includes all images, even failures)
   - Failed images tracked with `metadata.status='failed'`
   - All input images now appear in output

### 📊 Current Results

**baseline_val (reprocessed with fix):**
- ✅ All 404 images in output
- 267 successful (with OCR data)
- 137 failed/empty (tracked)
- Total: 1,775 polygons, 1,775 texts

**baseline_test:**
- 91 rows (100% with data)
- 612 polygons, 612 texts

**baseline_train:**
- Still processing (largest dataset)
- 3,165 rows processed so far

---

## Critical Discovery: Prebuilt Extraction API

### Issue Identified

User tested images in Upstage console and found:
- ❌ **Document Parsing** failed for some images
- ✅ **Document OCR** worked well
- ✅ **Prebuilt Extraction** worked excellently (designed for receipts/invoices)

### Prebuilt Extraction API

**Endpoint:** `https://console.upstage.ai/api/information-extraction/prebuilt-extraction`  
**Reference:** https://console.upstage.ai/api/information-extraction/prebuilt-extraction

**Key Points:**
- Specifically designed for receipts and invoices
- Provides higher quality results for receipt/invoice documents
- May have different response structure than Document Parsing
- Should be used for receipt/invoice-specific datasets

### Required Changes

1. **Add Prebuilt Extraction Support**
   - Implement new API endpoint handler
   - Parse prebuilt extraction response format
   - Create separate dataset processing option

2. **Dual Processing Strategy**
   - Option A: Document Parsing (current) - for general documents
   - Option B: Prebuilt Extraction - for receipts/invoices
   - Allow selection per dataset or per image

3. **Response Format Investigation**
   - Need to test prebuilt extraction API locally
   - Understand response structure differences
   - Adapt parsing logic accordingly

---

## Current Code Structure

### Key Files

1. **`src/batch_processor_base.py`**
   - Main processing logic
   - Async API integration
   - Response parsing (elements-based)
   - Rate limiting and retry logic

2. **`src/processor.py`**
   - S3 integration
   - Checkpoint management
   - AWS Batch entry point

3. **`src/schemas.py`**
   - OCRStorageItem schema
   - Data models

4. **`scripts/test_api_local.py`**
   - Local API testing tool
   - Can be extended for prebuilt extraction testing

### Current API Configuration

```python
API_URL_SUBMIT = "https://api.upstage.ai/v1/document-digitization/async"
API_URL_STATUS = "https://api.upstage.ai/v1/document-digitization/requests"
```

**Response Structure (Document Parsing):**
```json
{
  "elements": [
    {
      "category": "paragraph",
      "content": {"html": "...", "text": ""},
      "coordinates": [{"x": 0.2447, "y": 0.0714}, ...],
      "id": 0,
      "page": 1
    }
  ]
}
```

---

## Next Steps for Prebuilt Extraction

### 1. Research & Testing

- [ ] Test prebuilt extraction API locally with sample receipt image
- [ ] Document response structure differences
- [ ] Compare output quality vs Document Parsing
- [ ] Identify which datasets should use prebuilt extraction

### 2. Implementation

- [ ] Add prebuilt extraction endpoint configuration
- [ ] Create separate processing method or unified handler
- [ ] Implement response parsing for prebuilt extraction format
- [ ] Add dataset-level or image-level API selection

### 3. Integration

- [ ] Update job definition to support API selection
- [ ] Add command-line argument for API type
- [ ] Update documentation
- [ ] Test with receipt/invoice datasets

### 4. Validation

- [ ] Compare results from both APIs
- [ ] Verify all images processed correctly
- [ ] Ensure output schema consistency

---

## Known Issues & Solutions

### Issue 1: Missing Images in Output ✅ FIXED
- **Problem:** Only successful images appeared in output
- **Solution:** Modified `process_batch` to create entries for all images, even failures
- **Status:** Fixed and deployed

### Issue 2: Empty Results (Initial) ✅ FIXED
- **Problem:** All output files were empty (wrong API response parsing)
- **Solution:** Changed from `pages/words` to `elements` structure parsing
- **Status:** Fixed and tested locally

### Issue 3: High Failure Rate
- **Problem:** 137/404 images failed in baseline_val (34%)
- **Possible Causes:**
  - Document Parsing not suitable for all receipt types
  - Image quality issues
  - API rate limits (unlikely with current limits)
- **Solution:** Use Prebuilt Extraction for receipts/invoices

---

## Configuration

### AWS Resources
- **Region:** `ap-northeast-2`
- **S3 Bucket:** `ocr-batch-processing`
- **ECR Repository:** `batch-processor`
- **Job Queue:** Configured in `aws/config.env`

### GitHub Secrets Required
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`
- `JOB_ROLE_ARN`
- `EXEC_ROLE_ARN`

### Environment Variables
- `UPSTAGE_API_KEY` (in Secrets Manager or env)
- `S3_BUCKET`
- `DATASET_NAME`
- `CHECKPOINT_PREFIX`

---

## Testing Tools

### Local API Testing
```bash
python3 scripts/test_api_local.py [image_path]
```

### Output Inspection
```bash
python3 scripts/inspect_output.py
```

### Parquet Path Fixing
```bash
python3 scripts/fix_parquet_s3_paths.py
```

---

## Important Notes

1. **Rate Limiting**
   - Current: POLL_DELAY=5s, POLL_CONCURRENCY=2
   - Submission: 3 concurrent, 0.05s delay
   - ~99.8% reduction in polling requests achieved

2. **Checkpointing**
   - Saves every 500 images (batch_size)
   - Stored in S3: `s3://bucket/checkpoints/`
   - Resume with `--resume` flag

3. **Output Schema**
   - All images included (even failures)
   - Failed images: `metadata.status='failed'`
   - Successful images: polygons, texts, labels populated

4. **API Response Formats**
   - Document Parsing: `elements` array with normalized coordinates
   - Prebuilt Extraction: **TBD** (needs investigation)

---

## Files to Review

1. `src/batch_processor_base.py` - Core processing logic
2. `src/processor.py` - S3 and AWS Batch integration
3. `scripts/test_api_local.py` - API testing tool
4. `.github/workflows/deploy.yml` - CI/CD pipeline
5. `README.md` - Setup and usage documentation

---

## Quick Start for Next Session

1. **Test Prebuilt Extraction API:**
   ```bash
   # Extend test_api_local.py or create new test script
   # Test with sample receipt image
   # Document response structure
   ```

2. **Implement Prebuilt Extraction Support:**
   - Add new endpoint configuration
   - Create parsing logic
   - Add API selection option

3. **Reprocess Receipt Datasets:**
   - Use prebuilt extraction for receipt/invoice images
   - Compare results with Document Parsing
   - Validate output quality

---

## References

- Upstage Prebuilt Extraction API: https://console.upstage.ai/api/information-extraction/prebuilt-extraction
- Current API Endpoint: `https://api.upstage.ai/v1/document-digitization/async`
- Project Repository: https://github.com/Wchoi189/aws-batch-processor

---

**End of Session Handover**
