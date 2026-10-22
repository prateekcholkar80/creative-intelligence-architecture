-- Atomic worker claim. Python replaces only fixed typed-row tokens; all runtime
-- identity, scope, revision and row values remain BigQuery parameters.
CREATE TEMP TABLE result(state STRING, job_row STRING, attempt_row STRING, event_row STRING,
  job_audit STRING, attempt_audit STRING, event_audit STRING);
BEGIN
  BEGIN TRANSACTION;
  ASSERT CURRENT_TIMESTAMP() < @plan_expires_at AS 'WORKFLOW_CLAIM_EXPIRED';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.workspaces`
    WHERE workspace_id=@workspace_id AND status='ACTIVE'
      AND ((revision=@workspace_revision AND updated_at=@workspace_updated_at)
        OR (revision=@workspace_candidate_revision AND updated_at=@claim_observed_at)))=1
    AS 'WORKFLOW_CLAIM_WORKSPACE_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.memberships`
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id AND status='ACTIVE'
      AND revision=@membership_revision AND updated_at=@membership_updated_at
      AND 'CREATOR' IN UNNEST(roles))=1 AS 'WORKFLOW_CLAIM_MEMBER_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.conversations`
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
      AND owner_id=@principal_id AND status='ACTIVE' AND access_revision=@conversation_revision
      AND updated_at=@conversation_updated_at)=1 AS 'WORKFLOW_CLAIM_CONVERSATION_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
    WHERE workspace_id=@workspace_id AND request_id=@request_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_PIN__))=1
    AS 'WORKFLOW_CLAIM_REQUEST_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs`
    WHERE workspace_id=@workspace_id AND job_id=@job_id)<=1 AS 'WORKFLOW_CLAIM_JOB_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts`
    WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_CLAIM_ATTEMPT_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events`
    WHERE workspace_id=@workspace_id AND event_id=@event_id
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_CLAIM_EVENT_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
    WHERE workspace_id=@workspace_id
      AND audit_id IN (@job_audit_id,@attempt_audit_id,@event_audit_id)
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=3
    AS 'WORKFLOW_CLAIM_AUDIT_DUPLICATE';

  ASSERT (
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
      WHERE workspace_id=@workspace_id AND job_id=@job_id
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_PIN__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts`
      WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events`
      WHERE workspace_id=@workspace_id AND event_id=@event_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
      WHERE workspace_id=@workspace_id
        AND audit_id IN (@job_audit_id,@attempt_audit_id,@event_audit_id)
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0)
    OR
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
      WHERE workspace_id=@workspace_id AND job_id=@job_id
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_CANDIDATE__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts` t
      WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_CANDIDATE__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events` t
      WHERE workspace_id=@workspace_id AND event_id=@event_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__EVENT_CANDIDATE__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@job_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_AUDIT__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@attempt_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_AUDIT__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@event_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__EVENT_AUDIT__))=1))
    AS 'WORKFLOW_CLAIM_IDEMPOTENCY_CONFLICT';

  IF (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts`
      WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0 THEN
    UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces` t
      SET status=r.status,revision=r.revision,created_at=r.created_at,updated_at=r.updated_at
      FROM UNNEST([__WORKSPACE_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.revision=@workspace_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_CLAIM_FENCE_CONFLICT';
    UPDATE `project-661d501a-e709-4276-a26.ci_application.jobs` t
      SET state=r.state,revision=r.revision,attempt_count=r.attempt_count,
        active_attempt_id=r.active_attempt_id,output_id=r.output_id,
        safe_error_code=r.safe_error_code,updated_at=r.updated_at,completed_at=r.completed_at
      FROM UNNEST([__JOB_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.job_id=r.job_id AND t.revision=@job_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_CLAIM_JOB_CONFLICT';
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.job_attempts`
      SELECT r.* FROM UNNEST([__ATTEMPT_CANDIDATE__]) r;
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.stage_events`
      SELECT r.* FROM UNNEST([__EVENT_CANDIDATE__]) r;
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.audit_events`
      SELECT r.* FROM UNNEST([__JOB_AUDIT__,__ATTEMPT_AUDIT__,__EVENT_AUDIT__]) r;
    INSERT INTO result SELECT 'APPLIED',TO_JSON_STRING(__JOB_CANDIDATE__),
      TO_JSON_STRING(__ATTEMPT_CANDIDATE__),TO_JSON_STRING(__EVENT_CANDIDATE__),
      TO_JSON_STRING(__JOB_AUDIT__),TO_JSON_STRING(__ATTEMPT_AUDIT__),TO_JSON_STRING(__EVENT_AUDIT__);
  ELSE
    INSERT INTO result SELECT 'REUSED',TO_JSON_STRING(__JOB_CANDIDATE__),
      TO_JSON_STRING(__ATTEMPT_CANDIDATE__),TO_JSON_STRING(__EVENT_CANDIDATE__),
      TO_JSON_STRING(__JOB_AUDIT__),TO_JSON_STRING(__ATTEMPT_AUDIT__),TO_JSON_STRING(__EVENT_AUDIT__);
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT * FROM result;
