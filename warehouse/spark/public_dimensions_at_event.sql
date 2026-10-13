-- Read-only candidate. No table registration, writes, or actual Spark/Hive
-- acceptance is implied. Load the validated artifact tables in an isolated
-- stage; projection is transient, already-whitelisted, and source-isolated.
-- All timestamps must use the builder's canonical UTC microsecond format.
-- fact_ordinal is a temporary row number, NOT an event/user/request identifier.
-- DISTINCT removes repeated same-release history receipts; it does not merge
-- different facts. Each input ordinal must be unique in this invocation.
WITH covered AS (
    SELECT DISTINCT f.fact_ordinal, f.source, f.app, f.kind, f.dimension_key,
                    c.release_id
    FROM public_dim_fact_projection f
    LEFT JOIN public_dim_coverage c
      ON f.source = c.source AND f.app = c.app
     AND c.covered_from <= f.occurred_at AND f.occurred_at < c.covered_to
)
SELECT f.fact_ordinal, f.source, f.app, f.kind, f.release_id, d.dimension_id,
       CASE WHEN f.release_id IS NULL THEN 'unknown'
            WHEN d.present = TRUE THEN 'present'
            ELSE 'not_present' END AS dimension_status
FROM covered f
LEFT JOIN public_dim_scd2 d
  ON f.source = d.source AND f.app = d.app AND f.release_id = d.release_id
 AND f.kind = d.kind AND f.dimension_key = d.dimension_key;
