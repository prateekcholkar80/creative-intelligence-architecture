BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{stage}`)=1 AS 'Expected one experiment draft';
ASSERT (SELECT source_system='SYNTHETIC_POC' AND status='DRAFT_NOT_LAUNCHED'
  AND approval_scope='SYNTHETIC_POC_ONLY' AND real_business_approved=FALSE
  AND ARRAY_LENGTH(arms)=2 AND winner IS NULL AND budget_per_arm IS NULL
  AND start_date IS NULL AND end_date IS NULL FROM `{stage}`) AS 'Unlaunched POC draft required';
ASSERT (SELECT COUNT(*) FROM `{stage}` S CROSS JOIN UNNEST(S.arms) arm
  JOIN `project-661d501a-e709-4276-a26.ci_brand_memory.poc_generated_banner_embeddings` E
  ON E.asset_id=arm.asset_id AND E.sku=S.sku AND E.review_id=arm.review_id
    AND E.review_revision=arm.review_revision AND E.sha256=arm.content_sha256
    AND E.gcs_uri=arm.gcs_uri AND E.gcs_generation=arm.gcs_generation
    AND E.embedding_id=arm.embedding_id AND E.knowledge_snapshot_id=S.knowledge_snapshot_id
  WHERE JSON_VALUE(E.brief_json,'$.model_selection.layout')=arm.layout_id)=2
  AS 'Experiment source identity or approval changed';
ASSERT NOT EXISTS(SELECT T.experiment_id FROM `{target}` T JOIN `{stage}` S
  USING(experiment_id,source_system) GROUP BY T.experiment_id HAVING COUNT(*)>1)
  AS 'Duplicate registered experiment identity';
ASSERT NOT EXISTS(SELECT 1 FROM `{target}` T JOIN `{stage}` S USING(experiment_id,source_system)
  WHERE {immutable_mismatch}) AS 'Existing experiment differs from immutable draft';
MERGE `{target}` T USING `{stage}` S
  ON T.experiment_id=S.experiment_id AND T.source_system=S.source_system
  WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});
COMMIT TRANSACTION;
