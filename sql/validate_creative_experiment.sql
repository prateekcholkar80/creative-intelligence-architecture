SELECT COUNT(*) experiment_rows,
  COUNTIF(source_system!='SYNTHETIC_POC' OR status!='DRAFT_NOT_LAUNCHED'
    OR approval_scope!='SYNTHETIC_POC_ONLY' OR real_business_approved
    OR ARRAY_LENGTH(arms)!=2 OR primary_metric!='CONVERSION_RATE'
    OR budget_per_arm IS NOT NULL OR start_date IS NOT NULL OR end_date IS NOT NULL
    OR winner IS NOT NULL) invalid_drafts,
  COUNTIF(NOT current_evidence_eligible) stale_evidence,
  MAX(eligible_arm_count) eligible_arms
FROM `project-661d501a-e709-4276-a26.ci_creative.current_creative_experiments`
WHERE experiment_id=@experiment_id AND source_system='SYNTHETIC_POC';
