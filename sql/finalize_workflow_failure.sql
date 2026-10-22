-- Atomic FAILED/BLOCKED workflow finalization after a recorded terminal invocation.
CREATE TEMP TABLE result(state STRING, job_row STRING, attempt_row STRING,
  event_rows ARRAY<STRING>, audit_rows ARRAY<STRING>);
BEGIN
  BEGIN TRANSACTION;
  ASSERT CURRENT_TIMESTAMP()<@plan_expires_at AS 'WORKFLOW_FAILURE_EXPIRED';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.workspaces`
    WHERE workspace_id=@workspace_id AND status='ACTIVE'
      AND ((revision=@workspace_revision AND updated_at=@workspace_updated_at)
        OR (revision=@workspace_candidate_revision AND updated_at=@finalized_at)))=1
    AS 'WORKFLOW_FAILURE_WORKSPACE_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.memberships`
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id AND status='ACTIVE'
      AND revision=@membership_revision AND updated_at=@membership_updated_at
      AND 'CREATOR' IN UNNEST(roles))=1 AS 'WORKFLOW_FAILURE_MEMBER_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.conversations`
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
      AND owner_id=@principal_id AND status='ACTIVE' AND access_revision=@conversation_revision
      AND updated_at=@conversation_updated_at)=1 AS 'WORKFLOW_FAILURE_CONVERSATION_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
    WHERE workspace_id=@workspace_id AND request_id=@request_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_PIN__))=1 AS 'WORKFLOW_FAILURE_REQUEST_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs`
    WHERE workspace_id=@workspace_id AND job_id=@job_id)<=1 AS 'WORKFLOW_FAILURE_JOB_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts`
    WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_FAILURE_ATTEMPT_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations` t
    WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
      AND state=@terminal_state AND safe_error_code=@safe_error_code
      AND dispatch_receipt_id=@dispatch_receipt_id AND worker_id=@worker_id
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__INVOCATION_PIN__))=1
    AS 'WORKFLOW_FAILURE_INVOCATION_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.outputs`
    WHERE workspace_id=@workspace_id AND job_id=@job_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
    AS 'WORKFLOW_FAILURE_OUTPUT_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events`
    WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')) IN (1,@event_row_count)
    AS 'WORKFLOW_FAILURE_EVENT_COUNT_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events` t
    WHERE workspace_id=@workspace_id AND event_id=@existing_event_id
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__EXISTING_EVENT__))=1
    AS 'WORKFLOW_FAILURE_EVENT_PREFIX_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
    WHERE workspace_id=@workspace_id AND audit_id IN UNNEST(@audit_ids)
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')) IN (0,@audit_row_count)
    AS 'WORKFLOW_FAILURE_AUDIT_COUNT_CONFLICT';

  ASSERT (
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
      WHERE workspace_id=@workspace_id AND job_id=@job_id
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_PIN__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts` t
      WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_PIN__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events`
      WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.stage_events`
      WHERE workspace_id=@workspace_id AND event_id IN UNNEST(@new_event_ids)
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
      WHERE workspace_id=@workspace_id AND audit_id IN UNNEST(@audit_ids)
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0)
    OR
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
      WHERE workspace_id=@workspace_id AND job_id=@job_id
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_CANDIDATE__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts` t
      WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_CANDIDATE__))=1
     AND (SELECT COUNT(*) FROM UNNEST(__NEW_EVENTS__) expected
      JOIN `project-661d501a-e709-4276-a26.ci_application.stage_events` actual
        ON actual.workspace_id=expected.workspace_id AND actual.event_id=expected.event_id
        AND actual.occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(actual)=TO_JSON_STRING(expected))=@new_event_row_count
     AND (SELECT COUNT(*) FROM UNNEST(__AUDIT_ROWS__) expected
      JOIN `project-661d501a-e709-4276-a26.ci_application.audit_events` actual
        ON actual.workspace_id=expected.workspace_id AND actual.audit_id=expected.audit_id
        AND actual.occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(actual)=TO_JSON_STRING(expected))=@audit_row_count))
    AS 'WORKFLOW_FAILURE_IDEMPOTENCY_CONFLICT';

  IF (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
      WHERE workspace_id=@workspace_id AND job_id=@job_id
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_PIN__))=1 THEN
    UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces` t
      SET status=r.status,revision=r.revision,created_at=r.created_at,updated_at=r.updated_at
      FROM UNNEST([__WORKSPACE_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.revision=@workspace_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_FAILURE_FENCE_CONFLICT';
    UPDATE `project-661d501a-e709-4276-a26.ci_application.jobs` t
      SET state=r.state,revision=r.revision,output_id=r.output_id,safe_error_code=r.safe_error_code,
        updated_at=r.updated_at,completed_at=r.completed_at
      FROM UNNEST([__JOB_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.job_id=r.job_id AND t.revision=@job_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_FAILURE_JOB_CONFLICT';
    UPDATE `project-661d501a-e709-4276-a26.ci_application.job_attempts` t
      SET state=r.state,revision=r.revision,safe_error_code=r.safe_error_code,
        updated_at=r.updated_at,completed_at=r.completed_at
      FROM UNNEST([__ATTEMPT_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.attempt_id=r.attempt_id
        AND t.revision=@attempt_revision
        AND t.started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31');
    ASSERT @@row_count=1 AS 'WORKFLOW_FAILURE_ATTEMPT_CONFLICT';
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.stage_events`
      SELECT r.* FROM UNNEST(__NEW_EVENTS__) r;
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.audit_events`
      SELECT r.* FROM UNNEST(__AUDIT_ROWS__) r;
    INSERT INTO result VALUES ('APPLIED',TO_JSON_STRING(__JOB_CANDIDATE__),
      TO_JSON_STRING(__ATTEMPT_CANDIDATE__),
      ARRAY(SELECT TO_JSON_STRING(r) FROM UNNEST(__NEW_EVENTS__) r),
      ARRAY(SELECT TO_JSON_STRING(r) FROM UNNEST(__AUDIT_ROWS__) r));
  ELSE
    INSERT INTO result VALUES ('REUSED',TO_JSON_STRING(__JOB_CANDIDATE__),
      TO_JSON_STRING(__ATTEMPT_CANDIDATE__),
      ARRAY(SELECT TO_JSON_STRING(r) FROM UNNEST(__NEW_EVENTS__) r),
      ARRAY(SELECT TO_JSON_STRING(r) FROM UNNEST(__AUDIT_ROWS__) r));
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT * FROM result;
