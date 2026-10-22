-- Server-only sharing script. Render only the fixed schema/table tokens in
-- sharing_query.py; every intent/attempt value is a named query parameter.
-- No session mode. Never submit this script automatically after an unknown job.
DECLARE workspace_rows ARRAY<STRUCT<__WORKSPACES_TYPE__>>;
DECLARE membership_rows ARRAY<STRUCT<__MEMBERSHIPS_TYPE__>>;
DECLARE conversation_rows ARRAY<STRUCT<__CONVERSATIONS_TYPE__>>;
DECLARE receipt_rows ARRAY<STRUCT<__AUDIT_EVENTS_TYPE__>>;
DECLARE workspace_row STRUCT<__WORKSPACES_TYPE__>;
DECLARE membership_row STRUCT<__MEMBERSHIPS_TYPE__>;
DECLARE conversation_row STRUCT<__CONVERSATIONS_TYPE__>;
DECLARE receipt_row STRUCT<__AUDIT_EVENTS_TYPE__>;
DECLARE observed_at TIMESTAMP;
DECLARE result_state STRING;
CREATE TEMP FUNCTION valid_id(value STRING, maximum INT64) AS (
  LENGTH(value) BETWEEN 1 AND maximum AND NOT REGEXP_CONTAINS(value,
    r'[\x00-\x20\x7f\x{0085}\x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}]')
);

BEGIN TRANSACTION;
BEGIN
  -- CURRENT_TIMESTAMP is fixed at transaction start, including queued work.
  SET observed_at = CURRENT_TIMESTAMP();
  ASSERT @accepted_at <= observed_at AND observed_at < @deadline_at
    AS 'SHARING_INTENT_WINDOW_INVALID';
  SET workspace_rows = ARRAY(
    SELECT AS STRUCT __WORKSPACES_COLUMNS__ FROM __WORKSPACES_TABLE__
    WHERE workspace_id = @workspace_id LIMIT 2);
  SET membership_rows = ARRAY(
    SELECT AS STRUCT __MEMBERSHIPS_COLUMNS__ FROM __MEMBERSHIPS_TABLE__
    WHERE workspace_id = @workspace_id AND principal_id = @principal_id LIMIT 2);
  SET conversation_rows = ARRAY(
    SELECT AS STRUCT __CONVERSATIONS_COLUMNS__ FROM __CONVERSATIONS_TABLE__
    WHERE workspace_id = @workspace_id AND conversation_id = @conversation_id LIMIT 2);
  -- Look up the PREFIX: exact-ID lookup would miss changed-command conflicts.
  -- Retain the original server-issued interval on every transport attempt.
  SET receipt_rows = ARRAY(
    SELECT AS STRUCT __AUDIT_EVENTS_COLUMNS__ FROM __AUDIT_EVENTS_TABLE__
    WHERE workspace_id = @workspace_id AND STARTS_WITH(audit_id, @audit_prefix)
      AND occurred_at >= @accepted_at AND occurred_at < @deadline_at LIMIT 2);
  ASSERT ARRAY_LENGTH(workspace_rows) = 1 AS 'SHARING_WORKSPACE_NOT_UNIQUE';
  ASSERT ARRAY_LENGTH(membership_rows) = 1 AS 'SHARING_MEMBERSHIP_NOT_UNIQUE';
  ASSERT ARRAY_LENGTH(conversation_rows) = 1 AS 'SHARING_CONVERSATION_NOT_UNIQUE';
  ASSERT ARRAY_LENGTH(receipt_rows) <= 1 AS 'SHARING_RECEIPT_NOT_UNIQUE';
  SET workspace_row = workspace_rows[OFFSET(0)];
  SET membership_row = membership_rows[OFFSET(0)];
  SET conversation_row = conversation_rows[OFFSET(0)];

  -- Validate all control metadata, then current creator/owner permission, even
  -- for a historical retry. A prior receipt never grants current access.
  ASSERT workspace_row.schema_version = @schema_version
    AND workspace_row.status = 'ACTIVE' AND workspace_row.revision > 0
    AND workspace_row.created_at <= workspace_row.updated_at
    AND workspace_row.updated_at <= observed_at AS 'SHARING_WORKSPACE_INVALID';
  ASSERT membership_row.schema_version = @schema_version
    AND membership_row.identity_issuer = @identity_issuer
    AND valid_id(membership_row.identity_subject, 256)
    AND membership_row.principal_id = CONCAT('iap-v1:', LOWER(TO_HEX(SHA256(
      CONCAT(membership_row.identity_issuer, CHR(0), membership_row.identity_subject)))))
    AND membership_row.status = 'ACTIVE' AND membership_row.revision > 0
    AND membership_row.assignment_origin IN ('ADMIN', 'SYNC', 'BOOTSTRAP')
    AND valid_id(membership_row.updated_by, 256)
    AND NOT EXISTS (SELECT 1 FROM UNNEST(membership_row.roles) AS role
                    WHERE role IS NULL OR role NOT IN ('VIEWER', 'CREATOR', 'REVIEWER', 'ADMIN'))
    AND ARRAY_LENGTH(membership_row.roles) =
        (SELECT COUNT(DISTINCT role) FROM UNNEST(membership_row.roles) AS role)
    AND 'CREATOR' IN UNNEST(membership_row.roles)
    AND membership_row.created_at <= membership_row.updated_at
    AND membership_row.updated_at <= observed_at AS 'SHARING_MEMBERSHIP_INVALID';
  ASSERT conversation_row.schema_version = @schema_version
    AND conversation_row.status = 'ACTIVE'
    AND conversation_row.visibility IN ('PRIVATE', 'TEAM')
    AND conversation_row.access_revision > 0
    AND conversation_row.owner_id = @principal_id
    AND conversation_row.created_at <= conversation_row.updated_at
    AND conversation_row.updated_at <= observed_at AS 'SHARING_CONVERSATION_INVALID';

  IF ARRAY_LENGTH(receipt_rows) = 1 THEN
    SET receipt_row = receipt_rows[OFFSET(0)];
    ASSERT receipt_row.audit_id = @audit_id AS 'SHARING_IDEMPOTENCY_CONFLICT';
    ASSERT receipt_row.schema_version = @schema_version
      AND receipt_row.actor_id = @principal_id AND receipt_row.actor_kind = 'USER'
      AND receipt_row.action = 'SET_VISIBILITY' AND receipt_row.allowed IS TRUE
      AND receipt_row.reason_code = @reason_code AND receipt_row.policy_version = @policy_version
      AND receipt_row.resource_type = 'CONVERSATION' AND receipt_row.resource_id = @conversation_id
      AND receipt_row.conversation_id = @conversation_id AND receipt_row.owner_id = @principal_id
      AND receipt_row.request_id IS NULL AND receipt_row.job_id IS NULL
      AND receipt_row.trace_id = @trace_id
      AND receipt_row.membership_revision = @expected_membership_revision
      AND receipt_row.resource_revision = @expected_conversation_revision
      AND receipt_row.new_revision = @expected_conversation_revision + 1
      AND receipt_row.mutation_outcome = 'APPLIED' AND receipt_row.occurred_at <= observed_at
      AS 'SHARING_RECEIPT_INVALID';
    ASSERT conversation_row.access_revision >= receipt_row.new_revision
      AND conversation_row.created_at <= receipt_row.occurred_at
      AND receipt_row.occurred_at <= conversation_row.updated_at
      AND (conversation_row.access_revision > receipt_row.new_revision
           OR conversation_row.visibility = @visibility) AS 'SHARING_RECEIPT_INCONSISTENT';
    ASSERT @commit_status != 'NOT_COMMITTED' AS 'SHARING_COMMIT_OBSERVATION_CONFLICT';
    SET result_state = 'REUSED';
    -- Preserve later changes: no update/fence on the historical receipt branch.
  ELSEIF @commit_status IN ('UNKNOWN', 'COMMITTED') THEN
    SET result_state = 'UNKNOWN';
    -- Missing receipt is not positive evidence that a previous job did not commit.
  ELSE
    ASSERT @commit_status IN ('NOT_SUBMITTED', 'NOT_COMMITTED') AS 'SHARING_COMMIT_STATUS_INVALID';
    ASSERT membership_row.revision = @expected_membership_revision AS 'STALE_MEMBERSHIP_REVISION';
    ASSERT workspace_row.revision = @expected_workspace_revision AS 'STALE_WORKSPACE_REVISION';
    ASSERT workspace_row.revision < 9223372036854775807 AS 'WORKSPACE_REVISION_EXHAUSTED';
    ASSERT conversation_row.access_revision = @expected_conversation_revision AS 'STALE_CONVERSATION_REVISION';
    ASSERT conversation_row.visibility != @visibility AS 'CONVERSATION_VISIBILITY_UNCHANGED';

    -- All access-changing participants must mutate this same existing fence;
    -- native BigQuery conflict handling, not an unenforced key, coordinates them.
    UPDATE __WORKSPACES_TABLE__
      SET revision = revision + 1, updated_at = observed_at
      WHERE workspace_id = @workspace_id AND revision = @expected_workspace_revision AND status = 'ACTIVE';
    ASSERT @@row_count = 1 AS 'SHARING_WORKSPACE_UPDATE_CONFLICT';
    UPDATE __CONVERSATIONS_TABLE__
      SET visibility = @visibility, access_revision = access_revision + 1, updated_at = observed_at
      WHERE workspace_id = @workspace_id AND conversation_id = @conversation_id
        AND owner_id = @principal_id AND status = 'ACTIVE'
        AND access_revision = @expected_conversation_revision;
    ASSERT @@row_count = 1 AS 'SHARING_CONVERSATION_UPDATE_CONFLICT';
    SET receipt_row = (SELECT AS STRUCT
      @schema_version AS schema_version, @workspace_id AS workspace_id, @audit_id AS audit_id,
      @principal_id AS actor_id, 'USER' AS actor_kind, 'SET_VISIBILITY' AS action,
      TRUE AS allowed, @reason_code AS reason_code, @policy_version AS policy_version,
      'CONVERSATION' AS resource_type, @conversation_id AS resource_id,
      @conversation_id AS conversation_id, @principal_id AS owner_id,
      CAST(NULL AS STRING) AS request_id, CAST(NULL AS STRING) AS job_id,
      @trace_id AS trace_id, @expected_membership_revision AS membership_revision,
      @expected_conversation_revision AS resource_revision,
      @expected_conversation_revision + 1 AS new_revision, 'APPLIED' AS mutation_outcome,
      observed_at AS occurred_at);
    INSERT INTO __AUDIT_EVENTS_TABLE__ (__AUDIT_EVENTS_COLUMNS__)
      SELECT __AUDIT_EVENTS_RECEIPT_COLUMNS__;
    ASSERT @@row_count = 1 AS 'SHARING_AUDIT_INSERT_CONFLICT';
    SET workspace_row = (SELECT AS STRUCT workspace_row.* REPLACE(
      workspace_row.revision + 1 AS revision, observed_at AS updated_at));
    SET conversation_row = (SELECT AS STRUCT conversation_row.* REPLACE(
      conversation_row.access_revision + 1 AS access_revision,
      @visibility AS visibility, observed_at AS updated_at));
    SET result_state = 'APPLIED';
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
-- Only after COMMIT. These records are server-internal validation inputs and
-- must not be forwarded to the browser or logged with identity metadata.
SELECT result_state AS state, observed_at AS observed_at,
  workspace_row AS workspace, membership_row AS membership,
  conversation_row AS conversation, receipt_row AS receipt;
