-- One schema-explicit staging row; all identifiers are controlled by the loader.
-- Audit records are insert-only; current pointer changes in the same transaction.
BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{stage}`)=1 AS 'Expected one review event';
ASSERT (SELECT COUNT(*) FROM `{assets}` A JOIN `{stage}` S USING(asset_id,sku)
  WHERE A.source_system='SYNTHETIC_POC' AND A.status='REGISTERED_POC_DRAFT'
    AND A.sha256=S.asset_sha256 AND A.gcs_generation=S.gcs_generation
    AND A.gcs_uri=S.gcs_uri AND A.knowledge_snapshot_id=S.knowledge_snapshot_id)=1
  AS 'Registered banner identity changed';
ASSERT (SELECT COUNT(*) FROM `{history}` E JOIN `{stage}` S USING(review_id))<=1
  AS 'Duplicate review request';
ASSERT (SELECT COUNT(*) FROM `{state}` C JOIN `{stage}` S USING(asset_id))<=1
  AS 'Duplicate current review pointers';
IF EXISTS(SELECT 1 FROM `{history}` E JOIN `{stage}` S USING(review_id)) THEN
  ASSERT EXISTS(SELECT 1 FROM `{history}` E JOIN `{stage}` S USING(review_id)
    WHERE E.request_sha256=S.request_sha256) AS 'Idempotency key reused for a different decision';
ELSE
  IF (SELECT decision_origin FROM `{stage}`)='SYSTEM' THEN
    ASSERT NOT EXISTS(SELECT 1 FROM `{state}` C JOIN `{stage}` S USING(asset_id))
      AS 'System initialization cannot overwrite a review';
    ASSERT (SELECT review_status='PENDING_REVIEW' AND revision=1 AND expected_revision=0 FROM `{stage}`)
      AS 'System can only initialize pending review';
  ELSE
    ASSERT EXISTS(SELECT 1 FROM `{current}` C JOIN `{stage}` S USING(asset_id)
      WHERE C.revision=S.expected_revision AND C.review_id=S.supersedes_review_id)
      AS 'Stale review revision; inspect latest state';
  END IF;
  ASSERT NOT EXISTS(SELECT 1 FROM `{history}` E JOIN `{stage}` S USING(asset_id)
    WHERE E.revision=S.revision) AS 'Duplicate review revision';
  ASSERT (SELECT review_status FROM `{stage}`)!='APPROVED_FOR_POC' OR EXISTS(
    SELECT 1 FROM `{grounded}` A JOIN `{stage}` S USING(asset_id,sku)
    WHERE A.sha256=S.asset_sha256 AND A.gcs_generation=S.gcs_generation
      AND A.knowledge_snapshot_id=S.knowledge_snapshot_id) AS 'Current product/claim/policy grounding required';
  INSERT INTO `{history}` ({columns}) SELECT {columns} FROM `{stage}`;
  MERGE `{state}` C USING `{stage}` S ON C.asset_id=S.asset_id
  WHEN MATCHED THEN UPDATE SET {updates}
  WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});
END IF;
COMMIT TRANSACTION;
