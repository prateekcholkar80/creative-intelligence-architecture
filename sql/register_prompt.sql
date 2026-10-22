-- Server-only initial prompt registration transaction. Fixed identifiers and
-- typed row expressions are rendered by prompt_registry_query.py. Every value
-- comes from a validated revision-one DRAFT registration plan.
DECLARE candidate STRUCT<__PROMPT_REGISTRY_TYPE__>;
DECLARE history_candidate STRUCT<__PROMPT_REGISTRY_HISTORY_TYPE__>;
DECLARE current_rows ARRAY<STRUCT<__PROMPT_REGISTRY_TYPE__>>;
DECLARE initial_history_rows ARRAY<STRUCT<__PROMPT_REGISTRY_HISTORY_TYPE__>>;
DECLARE current_row STRUCT<__PROMPT_REGISTRY_TYPE__>;
DECLARE initial_history_row STRUCT<__PROMPT_REGISTRY_HISTORY_TYPE__>;
DECLARE history_scope_count INT64;
DECLARE observed_at TIMESTAMP;
DECLARE result_state STRING;

SET candidate = (SELECT AS STRUCT __PROMPT_REGISTRY_JSON_ROW__);
SET history_candidate = (SELECT AS STRUCT __PROMPT_REGISTRY_HISTORY_JSON_ROW__);

BEGIN TRANSACTION;
BEGIN
  SET observed_at = CURRENT_TIMESTAMP();

  -- Defense in depth: the Python contract has already reconstructed and hashed
  -- both rows. SQL still refuses any non-initial or cross-scope proposal.
  ASSERT candidate.schema_version = @storage_version
    AND candidate.prompt_contract_version = @contract_version
    AND candidate.vertex_project = @vertex_project
    AND candidate.vertex_location = @vertex_location
    AND candidate.revision = 1 AND candidate.parent_digest IS NULL
    AND candidate.status = 'DRAFT' AND candidate.evaluation_id IS NULL
    AND candidate.reason_code = 'IMPORTED_FROM_VERTEX'
    AND candidate.imported_at = candidate.changed_at
    AND candidate.imported_by = candidate.changed_by
    AND candidate.changed_at <= observed_at
    AS 'PROMPT_REGISTRATION_CANDIDATE_INVALID';
  ASSERT history_candidate.schema_version = @storage_version
    AND history_candidate.revision = 1
    AND history_candidate.parent_digest IS NULL
    AND history_candidate.status = 'DRAFT'
    AND history_candidate.evaluation_id IS NULL
    AND history_candidate.reason_code = 'IMPORTED_FROM_VERTEX'
    AND __HISTORY_MATCHES_CANDIDATE__
    AS 'PROMPT_REGISTRATION_HISTORY_INVALID';

  SET current_rows = ARRAY(
    SELECT AS STRUCT __PROMPT_REGISTRY_COLUMNS__
    FROM __PROMPT_REGISTRY_TABLE__
    WHERE (prompt_key = candidate.prompt_key
           AND snapshot_sha256 = candidate.snapshot_sha256)
       OR (vertex_project = candidate.vertex_project
           AND vertex_location = candidate.vertex_location
           AND vertex_prompt_id = candidate.vertex_prompt_id
           AND vertex_version_id = candidate.vertex_version_id)
    LIMIT 2);

  -- The literal complete timestamp domain satisfies the table's required
  -- partition filter while detecting partial/orphan registration history.
  SET history_scope_count = (
    SELECT COUNT(*) FROM __PROMPT_REGISTRY_HISTORY_TABLE__
    WHERE changed_at >= TIMESTAMP('0001-01-01 00:00:00+00')
      AND changed_at <= TIMESTAMP('9999-12-31 23:59:59.999999+00')
      AND ((prompt_key = candidate.prompt_key
            AND snapshot_sha256 = candidate.snapshot_sha256)
        OR (vertex_project = candidate.vertex_project
            AND vertex_location = candidate.vertex_location
            AND vertex_prompt_id = candidate.vertex_prompt_id
            AND vertex_version_id = candidate.vertex_version_id)));
  SET initial_history_rows = ARRAY(
    SELECT AS STRUCT __PROMPT_REGISTRY_HISTORY_COLUMNS__
    FROM __PROMPT_REGISTRY_HISTORY_TABLE__
    WHERE changed_at >= TIMESTAMP('0001-01-01 00:00:00+00')
      AND changed_at <= TIMESTAMP('9999-12-31 23:59:59.999999+00')
      AND revision = 1
      AND ((prompt_key = candidate.prompt_key
            AND snapshot_sha256 = candidate.snapshot_sha256)
        OR (vertex_project = candidate.vertex_project
            AND vertex_location = candidate.vertex_location
            AND vertex_prompt_id = candidate.vertex_prompt_id
            AND vertex_version_id = candidate.vertex_version_id))
    LIMIT 2);

  ASSERT ARRAY_LENGTH(current_rows) <= 1
    AS 'PROMPT_REGISTRATION_CURRENT_NOT_UNIQUE';
  ASSERT ARRAY_LENGTH(initial_history_rows) <= 1
    AS 'PROMPT_REGISTRATION_HISTORY_NOT_UNIQUE';

  IF ARRAY_LENGTH(current_rows) = 0 AND history_scope_count = 0 THEN
    INSERT INTO __PROMPT_REGISTRY_TABLE__ (__PROMPT_REGISTRY_COLUMNS__)
      SELECT __PROMPT_REGISTRY_CANDIDATE_COLUMNS__;
    ASSERT @@row_count = 1 AS 'PROMPT_REGISTRATION_CURRENT_INSERT_FAILED';
    INSERT INTO __PROMPT_REGISTRY_HISTORY_TABLE__ (__PROMPT_REGISTRY_HISTORY_COLUMNS__)
      SELECT __PROMPT_REGISTRY_HISTORY_CANDIDATE_COLUMNS__;
    ASSERT @@row_count = 1 AS 'PROMPT_REGISTRATION_HISTORY_INSERT_FAILED';
    SET current_row = candidate;
    SET initial_history_row = history_candidate;
    SET result_state = 'APPLIED';
  ELSE
    ASSERT ARRAY_LENGTH(current_rows) = 1
      AND ARRAY_LENGTH(initial_history_rows) = 1
      AS 'PROMPT_REGISTRATION_PARTIAL_CONFLICT';
    SET current_row = current_rows[OFFSET(0)];
    SET initial_history_row = initial_history_rows[OFFSET(0)];
    ASSERT __REGISTRY_IMMUTABLE_MATCH__
      AS 'PROMPT_REGISTRATION_IDENTITY_CONFLICT';
    ASSERT TO_JSON_STRING(initial_history_row) = TO_JSON_STRING(history_candidate)
      AS 'PROMPT_REGISTRATION_HISTORY_CONFLICT';
    SET result_state = 'REUSED';
  END IF;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;

-- Minimal receipt only. Full prompt text/settings must not enter logs or UI.
SELECT result_state AS state, observed_at AS observed_at,
  candidate.prompt_key AS prompt_key,
  candidate.snapshot_sha256 AS snapshot_sha256,
  history_candidate.record_digest AS record_digest;
