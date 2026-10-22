WITH identities AS (
  SELECT experiment_id,source_system,COUNT(*) identity_count
  FROM `project-661d501a-e709-4276-a26.ci_creative.creative_experiments`
  GROUP BY experiment_id,source_system
), current_arms AS (
  SELECT X.experiment_id,X.source_system,COUNT(*) eligible_arm_count,
    COUNT(DISTINCT arm.arm) unique_arm_count
  FROM `project-661d501a-e709-4276-a26.ci_creative.creative_experiments` X
  CROSS JOIN UNNEST(X.arms) arm
  JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generated_banner_embeddings` E
    ON E.asset_id=arm.asset_id AND E.sku=X.sku AND E.review_id=arm.review_id
      AND E.review_revision=arm.review_revision AND E.sha256=arm.content_sha256
      AND E.gcs_uri=arm.gcs_uri AND E.gcs_generation=arm.gcs_generation
      AND E.embedding_id=arm.embedding_id AND E.knowledge_snapshot_id=X.knowledge_snapshot_id
  WHERE JSON_VALUE(E.brief_json,'$.model_selection.layout')=arm.layout_id
  GROUP BY X.experiment_id,X.source_system
)
SELECT X.*,COALESCE(A.eligible_arm_count,0) eligible_arm_count,
  COALESCE(A.eligible_arm_count=2 AND A.unique_arm_count=2 AND I.identity_count=1,FALSE)
    AND X.source_system='SYNTHETIC_POC' AND X.status='DRAFT_NOT_LAUNCHED'
    AND X.real_business_approved=FALSE AND X.approval_scope='SYNTHETIC_POC_ONLY'
    AS current_evidence_eligible
FROM `project-661d501a-e709-4276-a26.ci_creative.creative_experiments` X
JOIN identities I USING(experiment_id,source_system)
LEFT JOIN current_arms A USING(experiment_id,source_system);
