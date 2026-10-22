-- Read-only. Parameter: brand STRING. The view selects the current eligible snapshot.
SELECT snapshot_id,chunk_id,chunk_type,brand,sku,campaign_id,asset_id,claim_id,
  chunk_text,source_system,source_reference,evidence_references,image_uris,
  retrieval_status,approval_scope,content_sha256
FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_visual_reference_chunks`
WHERE brand=@brand
ORDER BY source_reference,chunk_id;
