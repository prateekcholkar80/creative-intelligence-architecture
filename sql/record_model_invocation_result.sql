-- Atomic post-dispatch result/accounting publication. Recovery fragments are
-- rendered only for UNKNOWN outcomes; no provider call occurs in this script.
CREATE TEMP TABLE result(state STRING, invocation_row STRING, job_row STRING,
  attempt_row STRING, result_audit STRING, accounting_audit STRING,
  recovery_job_audit STRING, recovery_attempt_audit STRING);
BEGIN
  BEGIN TRANSACTION;
  ASSERT CURRENT_TIMESTAMP()<@plan_expires_at AS 'WORKFLOW_RESULT_EXPIRED';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.workspaces`
    WHERE workspace_id=@workspace_id AND status='ACTIVE'
      AND ((revision=@workspace_revision AND updated_at=@workspace_updated_at)
        OR (revision=@workspace_candidate_revision AND updated_at=@result_observed_at)))=1
    AS 'WORKFLOW_RESULT_WORKSPACE_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.memberships`
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id AND status='ACTIVE'
      AND revision=@membership_revision AND updated_at=@membership_updated_at
      AND 'CREATOR' IN UNNEST(roles))=1 AS 'WORKFLOW_RESULT_MEMBER_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.conversations`
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
      AND owner_id=@principal_id AND status='ACTIVE' AND access_revision=@conversation_revision
      AND updated_at=@conversation_updated_at)=1 AS 'WORKFLOW_RESULT_CONVERSATION_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.requests` t
    WHERE workspace_id=@workspace_id AND request_id=@request_id
      AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
      AND TO_JSON_STRING(t)=TO_JSON_STRING(__REQUEST_PIN__))=1
    AS 'WORKFLOW_RESULT_REQUEST_CONFLICT';
  __PARENT_ASSERT__
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations`
    WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
      AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=1
    AS 'WORKFLOW_RESULT_INVOCATION_MISSING';
  ASSERT (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
    WHERE workspace_id=@workspace_id AND audit_id IN (@result_audit_id,@accounting_audit_id)
      AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))<=2
    AS 'WORKFLOW_RESULT_AUDIT_DUPLICATE';
  __RECOVERY_AUDIT_COUNT__

  ASSERT (
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations` t
      WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
        AND state='DISPATCHED' AND revision=@invocation_revision
        AND dispatch_receipt_id=@dispatch_receipt_id AND worker_id=@worker_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__INVOCATION_PIN__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events`
      WHERE workspace_id=@workspace_id AND audit_id IN (@result_audit_id,@accounting_audit_id)
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0
     AND __RECOVERY_NEW__)
    OR
    ((SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations` t
      WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__INVOCATION_CANDIDATE__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@result_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__RESULT_AUDIT__))=1
     AND (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.audit_events` t
      WHERE workspace_id=@workspace_id AND audit_id=@accounting_audit_id
        AND occurred_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__ACCOUNTING_AUDIT__))=1
     AND __RECOVERY_REPLAY__))
    AS 'WORKFLOW_RESULT_IDEMPOTENCY_CONFLICT';

  IF (SELECT COUNT(*) FROM `project-661d501a-e709-4276-a26.ci_application.model_invocations` t
      WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id
        AND state='DISPATCHED' AND revision=@invocation_revision
        AND started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')
        AND TO_JSON_STRING(t)=TO_JSON_STRING(__INVOCATION_PIN__))=1 THEN
    UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces` t
      SET status=r.status,revision=r.revision,created_at=r.created_at,updated_at=r.updated_at
      FROM UNNEST([__WORKSPACE_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.revision=@workspace_revision;
    ASSERT @@row_count=1 AS 'WORKFLOW_RESULT_FENCE_CONFLICT';
    __RECOVERY_UPDATE__
    UPDATE `project-661d501a-e709-4276-a26.ci_application.model_invocations` t
      SET state=r.state,revision=r.revision,dispatch_receipt_id=r.dispatch_receipt_id,
        worker_id=r.worker_id,returned_model_id=r.returned_model_id,response_sha256=r.response_sha256,
        finish_reason=r.finish_reason,safety_outcome=r.safety_outcome,safe_error_code=r.safe_error_code,
        usage_status=r.usage_status,input_tokens=r.input_tokens,output_tokens=r.output_tokens,
        cached_tokens=r.cached_tokens,thinking_tokens=r.thinking_tokens,cost_status=r.cost_status,
        estimated_cost=r.estimated_cost,currency=r.currency,pricing_version=r.pricing_version,
        completed_at=r.completed_at
      FROM UNNEST([__INVOCATION_CANDIDATE__]) r
      WHERE t.workspace_id=r.workspace_id AND t.invocation_id=r.invocation_id
        AND t.revision=@invocation_revision
        AND t.started_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31');
    ASSERT @@row_count=1 AS 'WORKFLOW_RESULT_INVOCATION_CONFLICT';
    INSERT INTO `project-661d501a-e709-4276-a26.ci_application.audit_events`
      SELECT r.* FROM UNNEST([__RESULT_AUDIT__,__ACCOUNTING_AUDIT__]) r;
    __RECOVERY_AUDIT_INSERT__
    INSERT INTO result VALUES ('APPLIED',TO_JSON_STRING(__INVOCATION_CANDIDATE__),
      TO_JSON_STRING(__JOB_CANDIDATE__),TO_JSON_STRING(__ATTEMPT_CANDIDATE__),
      TO_JSON_STRING(__RESULT_AUDIT__),TO_JSON_STRING(__ACCOUNTING_AUDIT__),
      __RECOVERY_RESULT__);
  ELSE
    INSERT INTO result VALUES ('REUSED',TO_JSON_STRING(__INVOCATION_CANDIDATE__),
      TO_JSON_STRING(__JOB_CANDIDATE__),TO_JSON_STRING(__ATTEMPT_CANDIDATE__),
      TO_JSON_STRING(__RESULT_AUDIT__),TO_JSON_STRING(__ACCOUNTING_AUDIT__),
      __RECOVERY_RESULT__);
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT * FROM result;
