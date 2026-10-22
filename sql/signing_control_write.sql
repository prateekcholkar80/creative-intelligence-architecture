-- 4.2 Critical/Must have/shared pilot. Render only installed identifiers/schemas.
-- Requires an existing validated singleton head. No genesis or unrelated deletes.
DECLARE result_state STRING;
DECLARE observed_at TIMESTAMP;
BEGIN
  BEGIN TRANSACTION;
  SET observed_at = CURRENT_TIMESTAMP();
  ASSERT observed_at >= @accepted_at AND observed_at < @deadline_at AS 'SIGNING_WRITE_WINDOW_CLOSED';
  __TEMP_INPUTS__
  CREATE TEMP TABLE current_head AS SELECT * FROM __SIGNING_HEADS__ WHERE policy_id=@policy_id;
  CREATE TEMP TABLE current_checkpoints AS SELECT * FROM __SIGNING_CHECKPOINTS__
    WHERE policy_id=@policy_id AND consumer_id=@consumer_id;
  ASSERT (SELECT COUNT(*) FROM current_head)=1 AS 'SIGNING_HEAD_NOT_UNIQUE';
  ASSERT (SELECT COUNT(*) FROM current_checkpoints)<=1 AS 'SIGNING_CHECKPOINT_NOT_UNIQUE';
  ASSERT (SELECT COUNT(*) FROM proposed_publication)=1 AS 'SIGNING_PROPOSAL_NOT_UNIQUE';
  ASSERT (SELECT COUNT(*) FROM current_head WHERE schema_version=@schema_version
    AND revision>=1 AND control_revision>=1 AND published_at<=observed_at)=1 AS 'SIGNING_HEAD_INVALID';
  ASSERT NOT EXISTS (SELECT 1 FROM current_checkpoints WHERE schema_version!=@schema_version
    OR checkpoint_revision<1 OR publication_revision<1 OR updated_at>observed_at
    OR publication_revision>(SELECT revision FROM current_head)) AS 'SIGNING_CHECKPOINT_INVALID';
  ASSERT (SELECT COUNT(*) FROM __SIGNING_PUBLICATIONS__ p JOIN current_head h
    ON p.policy_id=h.policy_id AND p.revision=h.revision AND p.publication_digest=h.publication_digest
       AND p.published_at=h.published_at AND p.schema_version=@schema_version)=1 AS 'SIGNING_HEAD_BODY_INVALID';
  ASSERT (SELECT COUNT(*) FROM __SIGNING_PUBLICATIONS__ WHERE policy_id=@policy_id
    AND revision=(SELECT revision FROM current_head))=1 AS 'SIGNING_HEAD_BODY_NOT_UNIQUE';
  ASSERT NOT EXISTS (SELECT 1 FROM current_checkpoints c WHERE
    (SELECT COUNT(*) FROM __SIGNING_PUBLICATIONS__ p WHERE p.policy_id=@policy_id
      AND p.revision=c.publication_revision AND p.publication_digest=c.publication_digest
      AND p.published_at<=c.updated_at AND p.schema_version=@schema_version)!=1)
      AS 'SIGNING_CHECKPOINT_BODY_INVALID';
  ASSERT NOT EXISTS (SELECT 1 FROM __SIGNING_PUBLICATIONS__ p WHERE p.policy_id=@policy_id
    AND p.revision>(SELECT revision FROM current_head)) AS 'SIGNING_UNHEADED_PUBLICATION';
  ASSERT @kind IN ('PUBLICATION','CHECKPOINT') AS 'SIGNING_WRITE_KIND_INVALID';
  ASSERT (SELECT COUNT(*) FROM __SIGNING_PUBLICATIONS__ p WHERE p.policy_id=@policy_id
    AND p.revision=(SELECT revision FROM proposed_publication))<=1 AS 'SIGNING_PUBLICATION_NOT_UNIQUE';
  IF @kind='CHECKPOINT' THEN
    ASSERT (SELECT TO_JSON_STRING(p) FROM __SIGNING_PUBLICATIONS__ p WHERE p.policy_id=@policy_id
      AND p.revision=(SELECT revision FROM proposed_publication))
      =(SELECT TO_JSON_STRING(p) FROM proposed_publication p) AS 'SIGNING_CHECKPOINT_TARGET_CHANGED';
    ASSERT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM __SIGNING_KEY_HISTORY__
      WHERE policy_id=@policy_id AND publication_revision=(SELECT revision FROM proposed_publication)
      ORDER BY key_id))=TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM proposed_history ORDER BY key_id))
      AS 'SIGNING_CHECKPOINT_HISTORY_CHANGED';
  END IF;

  IF @kind='PUBLICATION' AND EXISTS (SELECT 1 FROM __SIGNING_PUBLICATIONS__ p
      WHERE p.policy_id=@policy_id AND p.revision=(SELECT revision FROM proposed_publication)) THEN
    ASSERT (SELECT TO_JSON_STRING(p) FROM __SIGNING_PUBLICATIONS__ p
      WHERE p.policy_id=@policy_id AND p.revision=(SELECT revision FROM proposed_publication))
      = (SELECT TO_JSON_STRING(p) FROM proposed_publication p) AS 'SIGNING_PUBLICATION_REPLAY_CONFLICT';
    ASSERT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM __SIGNING_KEY_HISTORY__
      WHERE policy_id=@policy_id AND publication_revision=(SELECT revision FROM proposed_publication)
      ORDER BY key_id)) = TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM proposed_history ORDER BY key_id))
      AS 'SIGNING_HISTORY_REPLAY_CONFLICT';
    SET result_state='REUSED';
  ELSEIF @kind='CHECKPOINT' AND EXISTS (SELECT 1 FROM current_checkpoints
      WHERE publication_revision >= (SELECT revision FROM proposed_publication)) THEN
    ASSERT NOT EXISTS (SELECT 1 FROM current_checkpoints
      WHERE publication_revision=(SELECT revision FROM proposed_publication)
        AND publication_digest!=(SELECT publication_digest FROM proposed_publication)) AS 'SIGNING_FLOOR_CONFLICT';
    SET result_state=IF(TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM current_checkpoints))
      =TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM proposed_checkpoint)), 'REUSED', 'SATISFIED');
  ELSE
    -- Compare exact typed rows from the prior validated snapshot. A typed plan alone
    -- is not authority; the adapter must authenticate and capture those rows freshly.
    ASSERT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM current_head))
      = TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM expected_signing_heads)) AS 'SIGNING_HEAD_CHANGED';
    ASSERT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM current_checkpoints))
      = TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM expected_signing_checkpoints)) AS 'SIGNING_CHECKPOINT_CHANGED';
    ASSERT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM __SIGNING_PUBLICATIONS__
      WHERE policy_id=@policy_id AND revision IN (SELECT revision FROM expected_signing_publications)
      ORDER BY revision)) = TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM expected_signing_publications ORDER BY revision))
      AS 'SIGNING_PARENT_CHANGED';
    ASSERT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM __SIGNING_KEY_HISTORY__
      WHERE policy_id=@policy_id AND publication_revision IN (SELECT revision FROM expected_signing_publications)
      ORDER BY publication_revision,key_id)) = TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM expected_signing_key_history
      ORDER BY publication_revision,key_id)) AS 'SIGNING_PARENT_HISTORY_CHANGED';
    ASSERT (SELECT control_revision FROM current_head)<9223372036854775807 AS 'SIGNING_FENCE_EXHAUSTED';

    IF @kind='PUBLICATION' THEN
      ASSERT (SELECT revision FROM proposed_publication)=(SELECT revision+1 FROM current_head)
        AND (SELECT parent_digest FROM proposed_publication)=(SELECT publication_digest FROM current_head)
        AND (SELECT publisher_id FROM proposed_publication)=@actor_id
        AND (SELECT command_id FROM proposed_publication)=@command_id
        AND (SELECT recorded_at FROM proposed_publication)=@accepted_at AS 'SIGNING_PARENT_CONFLICT';
      ASSERT NOT EXISTS (SELECT 1 FROM __SIGNING_KEY_HISTORY__ WHERE policy_id=@policy_id
        AND publication_revision=(SELECT revision FROM proposed_publication)) AS 'SIGNING_ORPHAN_HISTORY';
    ELSE
      ASSERT (SELECT COUNT(*) FROM proposed_checkpoint)=1 AS 'SIGNING_CHECKPOINT_PROPOSAL_INVALID';
      ASSERT (SELECT writer_id FROM proposed_checkpoint)=@actor_id
        AND (SELECT command_id FROM proposed_checkpoint)=@command_id
        AND (SELECT consumer_id FROM proposed_checkpoint)=@consumer_id
        AND (SELECT instance_id FROM proposed_checkpoint)=JSON_VALUE(@snapshot_json,'$.instance_id')
        AND (SELECT updated_at FROM proposed_checkpoint)=@accepted_at
        AND (SELECT publication_revision FROM proposed_checkpoint)=(SELECT revision FROM current_head)
        AND (SELECT publication_digest FROM proposed_checkpoint)=(SELECT publication_digest FROM current_head)
        AND (SELECT checkpoint_revision FROM proposed_checkpoint)
          =COALESCE((SELECT checkpoint_revision+1 FROM current_checkpoints),1) AS 'SIGNING_CHECKPOINT_CAS_CONFLICT';
      ASSERT (SELECT TO_JSON_STRING(p) FROM __SIGNING_PUBLICATIONS__ p WHERE p.policy_id=@policy_id
        AND p.revision=(SELECT revision FROM proposed_publication))
        =(SELECT TO_JSON_STRING(p) FROM proposed_publication p) AS 'SIGNING_CHECKPOINT_TARGET_CHANGED';
    END IF;
    -- Both publication and checkpoint writers mutate this same EXISTING fence.
    -- Inserting into an absent checkpoint alone is never treated as a lock.
    UPDATE __SIGNING_HEADS__ SET control_revision=control_revision+1
      WHERE policy_id=@policy_id AND schema_version=@schema_version
        AND revision=(SELECT revision FROM current_head)
        AND control_revision=(SELECT control_revision FROM current_head)
        AND publication_digest=(SELECT publication_digest FROM current_head);
    ASSERT @@row_count=1 AS 'SIGNING_FENCE_CONFLICT';
    -- PROBE_AFTER_FENCE
    IF @kind='PUBLICATION' THEN
      INSERT INTO __SIGNING_PUBLICATIONS__ (__SIGNING_PUBLICATIONS_COLUMNS__)
        SELECT __SIGNING_PUBLICATIONS_COLUMNS__ FROM proposed_publication;
      ASSERT @@row_count=1 AS 'SIGNING_PUBLICATION_INSERT_CONFLICT';
      INSERT INTO __SIGNING_KEY_HISTORY__ (__SIGNING_KEY_HISTORY_COLUMNS__)
        SELECT __SIGNING_KEY_HISTORY_COLUMNS__ FROM proposed_history;
      ASSERT @@row_count=(SELECT history_count FROM proposed_publication) AS 'SIGNING_HISTORY_INSERT_CONFLICT';
      UPDATE __SIGNING_HEADS__ SET revision=(SELECT revision FROM proposed_publication),
        publication_digest=(SELECT publication_digest FROM proposed_publication),
        published_at=(SELECT published_at FROM proposed_publication), publisher_id=@actor_id, command_id=@command_id
        WHERE policy_id=@policy_id;
      ASSERT @@row_count=1 AS 'SIGNING_HEAD_UPDATE_CONFLICT';
    ELSEIF (SELECT COUNT(*) FROM current_checkpoints)=0 THEN
      INSERT INTO __SIGNING_CHECKPOINTS__ (__SIGNING_CHECKPOINTS_COLUMNS__)
        SELECT __SIGNING_CHECKPOINTS_COLUMNS__ FROM proposed_checkpoint;
      ASSERT @@row_count=1 AS 'SIGNING_CHECKPOINT_INSERT_CONFLICT';
    ELSE
      UPDATE __SIGNING_CHECKPOINTS__ SET checkpoint_revision=(SELECT checkpoint_revision FROM proposed_checkpoint),
        publication_revision=(SELECT publication_revision FROM proposed_checkpoint),
        publication_digest=(SELECT publication_digest FROM proposed_checkpoint),
        instance_id=(SELECT instance_id FROM proposed_checkpoint), updated_at=@accepted_at,
        writer_id=@actor_id, command_id=@command_id
        WHERE policy_id=@policy_id AND consumer_id=@consumer_id
          AND checkpoint_revision=(SELECT checkpoint_revision FROM current_checkpoints);
      ASSERT @@row_count=1 AS 'SIGNING_CHECKPOINT_UPDATE_CONFLICT';
    END IF;
    SET result_state='APPLIED';
  END IF;
  CREATE TEMP TABLE signing_write_result AS SELECT result_state AS state, observed_at AS observed_at,
    (SELECT AS STRUCT * FROM __SIGNING_HEADS__ WHERE policy_id=@policy_id) AS head,
    (SELECT AS STRUCT * FROM proposed_publication) AS publication,
    ARRAY(SELECT AS STRUCT * FROM proposed_history ORDER BY key_id) AS history,
    ARRAY(SELECT AS STRUCT * FROM __SIGNING_CHECKPOINTS__ WHERE policy_id=@policy_id AND consumer_id=@consumer_id) AS checkpoints;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
-- PROBE_AFTER_COMMIT
SELECT * FROM signing_write_result;
