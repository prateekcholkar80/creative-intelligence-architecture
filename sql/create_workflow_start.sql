-- Atomic workflow submission: one validated GENERATE request, one QUEUED job,
-- two exact audits, and an ACTIVE workspace fence. Python replaces only the
-- five fixed typed-row tokens below; all runtime values remain query parameters.
CREATE TEMP TABLE result(state STRING, request_row STRING, job_row STRING,
  access_audit STRING, request_audit STRING, job_audit STRING);
BEGIN
  BEGIN TRANSACTION;
  ASSERT CURRENT_TIMESTAMP() < @plan_expires_at AS 'WORKFLOW_START_EXPIRED';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.workspaces`
    WHERE workspace_id=@workspace_id AND status='ACTIVE' AND revision=@workspace_revision
      AND updated_at=@workspace_updated_at)=1 AS 'WORKFLOW_START_WORKSPACE_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.memberships`
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id AND status='ACTIVE'
      AND revision=@membership_revision AND updated_at=@membership_updated_at
      AND 'CREATOR' IN UNNEST(roles))=1 AS 'WORKFLOW_START_MEMBER_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.conversations`
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
      AND owner_id=@principal_id AND status='ACTIVE' AND access_revision=@conversation_revision
      AND updated_at=@conversation_updated_at)=1 AS 'WORKFLOW_START_CONVERSATION_CONFLICT';

  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests`
    WHERE workspace_id=@workspace_id AND request_id=@request_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_START_REQUEST_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs`
    WHERE workspace_id=@workspace_id AND job_id=@job_id)<=1 AS 'WORKFLOW_START_JOB_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
    WHERE workspace_id=@workspace_id AND audit_id IN (@access_audit_id,@request_audit_id,@job_audit_id)
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=3
    AS 'WORKFLOW_START_AUDIT_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests`
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id
      AND conversation_id=@conversation_id
      AND (idempotency_key_sha256=@idempotency_key_sha256
        OR generation_intent_id=@generation_intent_id)
      AND created_at BETWEEN @retry_start_at AND @plan_observed_at)<=1
    AS 'WORKFLOW_START_INTENT_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id
      AND conversation_id=@conversation_id
      AND (idempotency_key_sha256=@idempotency_key_sha256
        OR generation_intent_id=@generation_intent_id)
      AND created_at BETWEEN @retry_start_at AND @plan_observed_at)=0
    OR (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
      WHERE workspace_id=@workspace_id AND request_id=@request_id
        AND created_at BETWEEN @retry_start_at AND @plan_observed_at
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_ROW__))=1
    AS 'WORKFLOW_START_INTENT_CONFLICT';

  ASSERT (
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests`
      WHERE workspace_id=@workspace_id AND request_id=@request_id
        AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs`
      WHERE workspace_id=@workspace_id AND job_id=@job_id)=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
      WHERE workspace_id=@workspace_id AND audit_id IN (@access_audit_id,@request_audit_id,@job_audit_id)
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0)
    OR
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
      WHERE workspace_id=@workspace_id AND request_id=@request_id
        AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_ROW__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
      WHERE workspace_id=@workspace_id AND job_id=@job_id
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_ROW__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@access_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__ACCESS_AUDIT_ROW__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@request_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_AUDIT_ROW__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@job_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_AUDIT_ROW__))=1))
    AS 'WORKFLOW_START_IDEMPOTENCY_CONFLICT';

  IF (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests`
      WHERE workspace_id=@workspace_id AND request_id=@request_id
        AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0 THEN
    UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces` t
      SET status=r.status,revision=r.revision,created_at=r.created_at,updated_at=r.updated_at
      FROM UNNEST([__WORKSPACE_ROW__]) r
      WHERE t.workspace_id=r.workspace_id AND t.revision=@workspace_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_START_FENCE_CONFLICT';
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.requests`
      SELECT r.* FROM UNNEST([__REQUEST_ROW__]) r;
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.jobs`
      SELECT r.* FROM UNNEST([__JOB_ROW__]) r;
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.audit_events`
      SELECT r.* FROM UNNEST([__ACCESS_AUDIT_ROW__,__REQUEST_AUDIT_ROW__,__JOB_AUDIT_ROW__]) r;
    INSERT INTO result SELECT 'APPLIED',TO_JSON_STRING(__REQUEST_ROW__),TO_JSON_STRING(__JOB_ROW__),
      TO_JSON_STRING(__ACCESS_AUDIT_ROW__),TO_JSON_STRING(__REQUEST_AUDIT_ROW__),
      TO_JSON_STRING(__JOB_AUDIT_ROW__);
  ELSE
    INSERT INTO result SELECT 'REUSED',TO_JSON_STRING(__REQUEST_ROW__),TO_JSON_STRING(__JOB_ROW__),
      TO_JSON_STRING(__ACCESS_AUDIT_ROW__),TO_JSON_STRING(__REQUEST_AUDIT_ROW__),
      TO_JSON_STRING(__JOB_AUDIT_ROW__);
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT * FROM result;
