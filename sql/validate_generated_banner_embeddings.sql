-- Parameter: asset_id STRING. Historical eligibility must stay at 20+60 images.
WITH eligible AS (
  SELECT * FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generated_banner_embeddings`
  WHERE asset_id=@asset_id
)
SELECT
  (SELECT COUNT(*) FROM eligible) eligible_vectors,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.image_embeddings`
    WHERE source_id=@asset_id AND source_system='SYNTHETIC_POC' AND generator_version='generated-banner-embeddings-v1') stored_vectors,
  (SELECT COUNT(*) FROM eligible WHERE dimension!=512 OR task_type!='IMAGE'
    OR retrieval_status!='GENERATED_POC_VISUAL_REFERENCE' OR real_business_approved
    OR ABS(COSINE_DISTANCE(vector,vector))>1e-12) invalid_vectors,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings`
    WHERE source_system='BEAUTYFEEDS') product_vectors,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings`
    WHERE source_system='SYNTHETIC_POC') historical_campaign_vectors,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings`
    WHERE source_id=@asset_id) leaked_historical_vectors;
