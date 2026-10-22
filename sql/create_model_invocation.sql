-- Atomic creation of one PLANNED planner invocation after exact evidence commit.
CREATE TEMP TABLE candidate_evidence AS
  SELECT r.* FROM UNNEST(__EVIDENCE_ROWS__) r;
CREATE TEMP TABLE result(state STRING, invocation_row STRING, audit_row STRING);
BEGIN
  BEGIN TRANSACTION;
  ASSERT CURRENT_TIMESTAMP()<@plan_expires_at AS 'WORKFLOW_INVOCATION_EXPIRED';
  ASSERT (SELECT COUNT(*) FROM candidate_evidence)=@evidence_row_count
    AS 'WORKFLOW_INVOCATION_EVIDENCE_COUNT';
  ASSERT (SELECT COUNT(DISTINCT evidence_ref_id) FROM candidate_evidence)=@evidence_row_count
    AS 'WORKFLOW_INVOCATION_EVIDENCE_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM candidate_evidence
    WHERE workspace_id=@workspace_id AND evidence_snapshot_id=@evidence_snapshot_id
      AND job_id=@job_id AND request_id=@request_id AND attempt_id=@attempt_id)=@evidence_row_count
    AS 'WORKFLOW_INVOCATION_EVIDENCE_SCOPE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.workspaces`
    WHERE workspace_id=@workspace_id AND status='ACTIVE'
      AND ((revision=@workspace_revision AND updated_at=@workspace_updated_at)
        OR (revision=@workspace_candidate_revision AND updated_at=@create_observed_at)))=1
    AS 'WORKFLOW_INVOCATION_WORKSPACE_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.memberships`
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id AND status='ACTIVE'
      AND revision=@membership_revision AND updated_at=@membership_updated_at
      AND 'CREATOR' IN UNNEST(roles))=1 AS 'WORKFLOW_INVOCATION_MEMBER_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.conversations`
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
      AND owner_id=@principal_id AND status='ACTIVE' AND access_revision=@conversation_revision
      AND updated_at=@conversation_updated_at)=1 AS 'WORKFLOW_INVOCATION_CONVERSATION_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
    WHERE workspace_id=@workspace_id AND request_id=@request_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_PIN__))=1
    AS 'WORKFLOW_INVOCATION_REQUEST_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.jobs` t
    WHERE workspace_id=@workspace_id AND job_id=@job_id AND state='RUNNING'
      AND active_attempt_id=@attempt_id AND CURRENT_TIMESTAMP()<deadline_at
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__JOB_PIN__))=1
    AS 'WORKFLOW_INVOCATION_JOB_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.job_attempts` t
    WHERE workspace_id=@workspace_id AND attempt_id=@attempt_id AND state='RUNNING'
      AND worker_id=@worker_id
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__ATTEMPT_PIN__))=1
    AS 'WORKFLOW_INVOCATION_ATTEMPT_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.evidence_snapshots`
    WHERE workspace_id=@workspace_id AND evidence_snapshot_id=@evidence_snapshot_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=@evidence_row_count
    AS 'WORKFLOW_INVOCATION_EVIDENCE_MISSING';
  ASSERT (SELECT COUNT(*)
    FROM `project-661d501a-e709-4276-a26.ci_application.evidence_snapshots` t
    JOIN candidate_evidence c USING (workspace_id,evidence_snapshot_id,evidence_ref_id)
    WHERE t.workspace_id=@workspace_id AND t.evidence_snapshot_id=@evidence_snapshot_id
      AND t.created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(c))=@evidence_row_count
    AS 'WORKFLOW_INVOCATION_EVIDENCE_CHANGED';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
    WHERE workspace_id=@workspace_id AND audit_id=@evidence_audit_id
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__EVIDENCE_AUDIT__))=1
    AS 'WORKFLOW_INVOCATION_EVIDENCE_AUDIT_MISSING';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
    WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_INVOCATION_ID_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
    WHERE workspace_id=@workspace_id AND request_id=@request_id AND stage='PLANNER' AND call_number=1
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_INVOCATION_STAGE_DUPLICATE';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
    WHERE workspace_id=@workspace_id AND audit_id=@invocation_audit_id
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=1
    AS 'WORKFLOW_INVOCATION_AUDIT_DUPLICATE';

  ASSERT (
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
      WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
      WHERE workspace_id=@workspace_id AND request_id=@request_id AND stage='PLANNER' AND call_number=1
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
      WHERE workspace_id=@workspace_id AND audit_id=@invocation_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0)
    OR
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations` t
      WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__INVOCATION_ROW__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
      WHERE workspace_id=@workspace_id AND request_id=@request_id AND stage='PLANNER' AND call_number=1
        AND invocation_id=@invocation_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@invocation_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__INVOCATION_AUDIT__))=1))
    AS 'WORKFLOW_INVOCATION_IDEMPOTENCY_CONFLICT';

  IF (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
      WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0 THEN
    UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces` t
      SET status=r.status,revision=r.revision,created_at=r.created_at,updated_at=r.updated_at
      FROM UNNEST([__WORKSPACE_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.revision=@workspace_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_INVOCATION_FENCE_CONFLICT';
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.model_invocations`
      SELECT r.* FROM UNNEST([__INVOCATION_ROW__]) r;
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.audit_events`
      SELECT r.* FROM UNNEST([__INVOCATION_AUDIT__]) r;
    INSERT INTO result VALUES ('APPLIED',TO_JSON_STRING(__INVOCATION_ROW__),
      TO_JSON_STRING(__INVOCATION_AUDIT__));
  ELSE
    INSERT INTO result VALUES ('REUSED',TO_JSON_STRING(__INVOCATION_ROW__),
      TO_JSON_STRING(__INVOCATION_AUDIT__));
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT * FROM result;
