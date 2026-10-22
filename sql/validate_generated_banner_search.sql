-- Read-only test fixtures, never inserted into any source-of-truth table.
-- The search verifier appends the production search SELECT using fixture as its
-- source, so both the exact distance ranking and its eligibility prefilters run.
WITH fixture AS (
  SELECT id AS asset_id,
    IF(id='other-sku',IF(@sku='CI-SKU-0001','CI-SKU-0002','CI-SKU-0001'),@sku) AS sku,
    id AS embedding_id,'gs://fixture/unused.png' AS gcs_uri,
    '1' AS gcs_generation,'fixture-only' AS sha256,'fixture-snapshot' AS knowledge_snapshot_id,
    'fixture-review' AS review_id,1 AS review_revision,
    'APPROVED_FOR_POC' AS review_status,'POC_VISUAL_REUSE_ONLY' AS review_approval_scope,
    IF(id='actual-source','ACTUAL','SYNTHETIC_POC') AS source_system,
    CASE id WHEN 'rejected' THEN 'REJECTED' WHEN 'pending' THEN 'PENDING_REVIEW'
      ELSE 'REGISTERED_POC_DRAFT' END AS status,
    IF(id='wrong-scope','MANUFACTURER_APPROVED','SYNTHETIC_POC_ONLY') AS approval_scope,
    id='real-approval' AS real_business_approved,
    IF(id='historical','POC_CONTEXT','GENERATED_POC_VISUAL_REFERENCE') AS retrieval_status,
    IF(id='benefit-evidence','BENEFIT_EVIDENCE','NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE') AS usage_scope,
    IF(id='wrong-model','gemini-embedding-001',@model) AS model_id,
    IF(id='wrong-dimension',768,@dimension) AS dimension,
    IF(id='wrong-location','global',@location) AS model_location,
    IF(id='wrong-task','RETRIEVAL_DOCUMENT','IMAGE') AS task_type,
    ARRAY_CONCAT(coords,ARRAY(SELECT 0.0 FROM UNNEST(GENERATE_ARRAY(1,510)))) AS vector
  FROM UNNEST([
    STRUCT('00-same-a' AS id,[1.0,0.0] AS coords),
    STRUCT('01-same-b' AS id,[2.0,0.0] AS coords),
    STRUCT('02-near' AS id,[1.0,1.0] AS coords),
    STRUCT('03-orthogonal' AS id,[0.0,1.0] AS coords),
    STRUCT('04-opposite' AS id,[-1.0,0.0] AS coords),
    STRUCT('other-sku' AS id,[1.0,0.0] AS coords),
    STRUCT('rejected' AS id,[1.0,0.0] AS coords),
    STRUCT('pending' AS id,[1.0,0.0] AS coords),
    STRUCT('real-approval' AS id,[1.0,0.0] AS coords),
    STRUCT('actual-source' AS id,[1.0,0.0] AS coords),
    STRUCT('wrong-scope' AS id,[1.0,0.0] AS coords),
    STRUCT('historical' AS id,[1.0,0.0] AS coords),
    STRUCT('benefit-evidence' AS id,[1.0,0.0] AS coords),
    STRUCT('wrong-model' AS id,[1.0,0.0] AS coords),
    STRUCT('wrong-dimension' AS id,[1.0,0.0] AS coords),
    STRUCT('wrong-location' AS id,[1.0,0.0] AS coords),
    STRUCT('wrong-task' AS id,[1.0,0.0] AS coords)
  ])
)
