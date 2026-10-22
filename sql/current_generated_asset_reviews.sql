-- Unique current pointer must agree with one immutable audit event and the
-- highest revision. Duplicated or inconsistent authoritative records fail closed.
WITH state_counts AS (
  SELECT asset_id,COUNT(*) n FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_review_state`
  GROUP BY asset_id
), event_counts AS (
  SELECT review_id,COUNT(*) n FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_reviews`
  GROUP BY review_id
), revisions AS (
  SELECT asset_id,MAX(revision) latest,COUNT(*) n,COUNT(DISTINCT revision) distinct_n
  FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_reviews` GROUP BY asset_id
)
SELECT S.*
FROM `project-661d501a-e709-4276-a26.ci_creative.generated_asset_review_state` S
JOIN `project-661d501a-e709-4276-a26.ci_creative.generated_asset_reviews` E
  ON S.review_id=E.review_id AND TO_JSON_STRING(S)=TO_JSON_STRING(E)
JOIN state_counts SC ON SC.asset_id=S.asset_id AND SC.n=1
JOIN event_counts EC ON EC.review_id=S.review_id AND EC.n=1
JOIN revisions R ON R.asset_id=S.asset_id AND R.latest=S.revision AND R.n=R.distinct_n
WHERE S.source_system='SYNTHETIC_POC' AND S.approval_scope='POC_VISUAL_REUSE_ONLY'
  AND S.review_status IN ('PENDING_REVIEW','APPROVED_FOR_POC','REJECTED')
  AND S.revision>=1 AND S.reviewer!='' AND S.notes!=''
  AND (S.decision_origin='HUMAN' OR (S.decision_origin='SYSTEM' AND S.review_status='PENDING_REVIEW'))
