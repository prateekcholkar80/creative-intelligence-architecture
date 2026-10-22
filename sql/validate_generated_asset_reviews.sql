SELECT
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets`
    WHERE source_system='SYNTHETIC_POC') generated_assets,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_reviews`) audit_events,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_review_state`) review_states,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_review_state` S
    LEFT JOIN `project-661d501a-e709-4276-a26.ci_creative.current_generated_asset_reviews` R
      ON R.asset_id=S.asset_id AND R.review_id=S.review_id WHERE R.review_id IS NULL) invalid_pointers,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_creative.generated_campaign_assets` A
    LEFT JOIN `project-661d501a-e709-4276-a26.ci_creative.current_generated_asset_reviews` R
      ON R.asset_id=A.asset_id AND R.sku=A.sku
    WHERE A.source_system='SYNTHETIC_POC' AND R.review_id IS NULL) missing_current_reviews,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generated_banner_embeddings`
    WHERE review_status!='APPROVED_FOR_POC' OR review_approval_scope!='POC_VISUAL_REUSE_ONLY') invalid_eligible_reviews,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings`
    WHERE source_system='BEAUTYFEEDS') product_vectors,
  (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_brand_memory.poc_image_embeddings`
    WHERE source_system='SYNTHETIC_POC') historical_campaign_vectors;
