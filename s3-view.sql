CREATE OR REPLACE VIEW s3_analytics_view AS
WITH
-- A) Deduplicate inventory to one row per key (latest by lastmodifieddate)
inv_latest_by_key AS (
  SELECT
      key,
      max_by(CAST(size AS bigint), from_iso8601_timestamp(lastmodifieddate)) AS size,
      -- latest LastModified per key (timestamp)
      max_by(from_iso8601_timestamp(lastmodifieddate), from_iso8601_timestamp(lastmodifieddate)) AS lastmodified_ts
  FROM {inventory_bucket_name}
  WHERE isdeletemarker = 'false'
    AND islatest = 'true'
  GROUP BY key
),

-- B) Inventory exploded to folder-prefixes (based on the deduped set)
inv_base AS (
  SELECT
      key,
      size,
      lastmodified_ts,
      split(key, '/') AS parts,
      cardinality(split(key, '/')) AS n
  FROM inv_latest_by_key
),
inv_prefixes AS (
  SELECT
      array_join(slice(parts, 1, level), '/') AS prefix,
      size,
      lastmodified_ts,
      level AS prefix_level
  FROM inv_base
  CROSS JOIN UNNEST(sequence(1, least(greatest(n - 1, 1), 5))) AS t(level)
),
inv_agg AS (
  SELECT
      prefix,
      prefix_level,
      sum(size) AS inv_total_bytes
  FROM inv_prefixes
  GROUP BY 1, 2
),

-- NEW: inventory date range per prefix
inv_dates AS (
  SELECT
      prefix,
      min(lastmodified_ts) AS inv_first_ts,
      max(lastmodified_ts) AS inv_latest_ts
  FROM inv_prefixes
  GROUP BY 1
),

-- C) Logs parsed + exploded to same prefix logic
logs_raw AS (
  SELECT
      date_parse(
        substr(requestdatetime, 1, length(requestdatetime) - 6),
        '%d/%b/%Y:%H:%i:%s'
      ) AS log_ts,
      operation,
      key
  FROM {logs_bucket_name}
  WHERE key IS NOT NULL
    AND key <> ''
    AND regexp_like(operation, '(?i)GET')
),
logs_joined AS (
  SELECT
      l.log_ts,
      l.operation,
      i.key,
      i.size,
      split(i.key, '/') AS parts,
      cardinality(split(i.key, '/')) AS n
  FROM logs_raw l
  JOIN inv_latest_by_key i
    ON i.key = l.key
),
logs_prefixes AS (
  SELECT
      array_join(slice(parts, 1, level), '/') AS prefix,
      log_ts,
      size
  FROM logs_joined
  CROSS JOIN UNNEST(sequence(1, least(greatest(n - 1, 1), 5))) AS t(level)
),

-- NEW: logs date range per prefix (across all GET-like operations)
logs_dates AS (
  SELECT
      prefix,
      min(log_ts) AS first_log_ts,
      max(log_ts) AS latest_log_ts
  FROM logs_prefixes
  GROUP BY 1
),

-- D) Aggregate per prefix across rolling windows (MERGED: GET + COPY.GET)
ops_agg AS (
  SELECT
      prefix,

      -- counts
      count_if(log_ts >= current_timestamp - INTERVAL '1'  DAY)  AS op_count_1d,
      count_if(log_ts >= current_timestamp - INTERVAL '7'  DAY)  AS op_count_7d,
      count_if(log_ts >= current_timestamp - INTERVAL '14' DAY)  AS op_count_14d,
      count_if(log_ts >= current_timestamp - INTERVAL '21' DAY)  AS op_count_21d,
      count_if(log_ts >= current_timestamp - INTERVAL '30' DAY)  AS op_count_30d,
      count_if(log_ts >= current_timestamp - INTERVAL '60' DAY)  AS op_count_60d,
      count_if(log_ts >= current_timestamp - INTERVAL '90' DAY)  AS op_count_90d,

      -- bytes (sum current sizes of objects referenced by those ops)
      sum(IF(log_ts >= current_timestamp - INTERVAL '1'  DAY,  size, BIGINT '0')) AS op_bytes_1d,
      sum(IF(log_ts >= current_timestamp - INTERVAL '7'  DAY,  size, BIGINT '0')) AS op_bytes_7d,
      sum(IF(log_ts >= current_timestamp - INTERVAL '14' DAY, size, BIGINT '0')) AS op_bytes_14d,
      sum(IF(log_ts >= current_timestamp - INTERVAL '21' DAY, size, BIGINT '0')) AS op_bytes_21d,
      sum(IF(log_ts >= current_timestamp - INTERVAL '30' DAY, size, BIGINT '0')) AS op_bytes_30d,
      sum(IF(log_ts >= current_timestamp - INTERVAL '60' DAY, size, BIGINT '0')) AS op_bytes_60d,
      sum(IF(log_ts >= current_timestamp - INTERVAL '90' DAY, size, BIGINT '0')) AS op_bytes_90d

  FROM logs_prefixes
  GROUP BY 1
),

-- E) Base output so we can reuse inv_total_gb alias below
base AS (
  SELECT
      ia.prefix,
      ia.prefix_level,
      ia.inv_total_bytes,
      (CAST(ia.inv_total_bytes AS double) / 1073741824.0) AS inv_total_gb,

      COALESCE(oa.op_count_1d,  0) AS op_count_1d,
      COALESCE(oa.op_bytes_1d,  0) AS op_bytes_1d,
      COALESCE(oa.op_count_7d,  0) AS op_count_7d,
      COALESCE(oa.op_bytes_7d,  0) AS op_bytes_7d,
      COALESCE(oa.op_count_14d, 0) AS op_count_14d,
      COALESCE(oa.op_bytes_14d, 0) AS op_bytes_14d,
      COALESCE(oa.op_count_21d, 0) AS op_count_21d,
      COALESCE(oa.op_bytes_21d, 0) AS op_bytes_21d,
      COALESCE(oa.op_count_30d, 0) AS op_count_30d,
      COALESCE(oa.op_bytes_30d, 0) AS op_bytes_30d,
      COALESCE(oa.op_count_60d, 0) AS op_count_60d,
      COALESCE(oa.op_bytes_60d, 0) AS op_bytes_60d,
      COALESCE(oa.op_count_90d, 0) AS op_count_90d,
      COALESCE(oa.op_bytes_90d, 0) AS op_bytes_90d,

      -- bring in date ranges
      ld.first_log_ts,
      ld.latest_log_ts,
      id.inv_first_ts,
      id.inv_latest_ts
  FROM inv_agg ia
  LEFT JOIN ops_agg oa
    ON oa.prefix = ia.prefix
  LEFT JOIN logs_dates ld
    ON ld.prefix = ia.prefix
  LEFT JOIN inv_dates id
    ON id.prefix = ia.prefix
),

-- F) Final projection with all dollar columns + new date cols
priced AS (
  SELECT
    prefix,
    prefix_level,
    inv_total_bytes,
    inv_total_gb,

    -- NEW: formatted date columns
    date_format(first_log_ts, '%Y-%m-%d') AS first_s3_logs_date,
    date_format(latest_log_ts, '%Y-%m-%d') AS latest_s3_logs_date,
    date_format(inv_first_ts, '%Y-%m-%d')  AS first_s3_inventory_date,
    date_format(inv_latest_ts, '%Y-%m-%d') AS latest_s3_inventory_date,

    -- Storage class pricing columns (per GiB)
    inv_total_gb * 0.022   AS inv_total_dollar_standard,
    inv_total_gb * 0.0128  AS inv_total_dollar_standard_ia,
    inv_total_gb * 0.00128 AS inv_total_dollar_glacier_deep_archive,
    inv_total_gb * 0.00376 AS inv_total_dollar_glacier_flexible_retrieval,
    inv_total_gb * 0.00410 AS inv_total_dollar_glacier_instant_retrieval,

    -- raw counts/bytes
    op_count_1d,  op_bytes_1d,
    op_count_7d,  op_bytes_7d,
    op_count_14d, op_bytes_14d,
    op_count_21d, op_bytes_21d,
    op_count_30d, op_bytes_30d,
    op_count_60d, op_bytes_60d,
    op_count_90d, op_bytes_90d,

    -- 1d request/byte costs
    CAST(op_bytes_1d AS double)/1073741824.0 * 0.01 AS op_bytes_1d_dollar_standard_ia,
    CAST(op_count_1d AS double)/10000.0 * 0.01     AS op_count_1d_dollar_standard_ia,

    CAST(op_bytes_1d AS double)/1073741824.0 * 0.02 AS op_bytes_1d_dollar_glacier_deep_archive,
    (CAST(op_count_1d AS double)/10000.0 + CAST(op_count_1d AS double)/10000.0 * 0.01)
                                                  AS op_count_1d_dollar_glacier_deep_archive,

    CAST(op_bytes_1d AS double)/1073741824.0 * 0.01 AS op_bytes_1d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_1d AS double)/10000.0 * 0.5 + CAST(op_count_1d AS double)/10000.0 * 0.01)
                                                  AS op_count_1d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_1d AS double)/1073741824.0 * 0.03 AS op_bytes_1d_dollar_glacier_instant_retrieval,
    CAST(op_count_1d AS double)/10000.0 * 0.1       AS op_count_1d_dollar_glacier_instant_retrieval,

    -- 7d
    CAST(op_bytes_7d AS double)/1073741824.0 * 0.01 AS op_bytes_7d_dollar_standard_ia,
    CAST(op_count_7d AS double)/10000.0 * 0.01     AS op_count_7d_dollar_standard_ia,

    CAST(op_bytes_7d AS double)/1073741824.0 * 0.02 AS op_bytes_7d_dollar_glacier_deep_archive,
    (CAST(op_count_7d AS double)/10000.0 + CAST(op_count_7d AS double)/10000.0 * 0.01)
                                                  AS op_count_7d_dollar_glacier_deep_archive,

    CAST(op_bytes_7d AS double)/1073741824.0 * 0.01 AS op_bytes_7d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_7d AS double)/10000.0 * 0.5 + CAST(op_count_7d AS double)/10000.0 * 0.01)
                                                  AS op_count_7d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_7d AS double)/1073741824.0 * 0.03 AS op_bytes_7d_dollar_glacier_instant_retrieval,
    CAST(op_count_7d AS double)/10000.0 * 0.1       AS op_count_7d_dollar_glacier_instant_retrieval,

    -- 14d
    CAST(op_bytes_14d AS double)/1073741824.0 * 0.01 AS op_bytes_14d_dollar_standard_ia,
    CAST(op_count_14d AS double)/10000.0 * 0.01     AS op_count_14d_dollar_standard_ia,

    CAST(op_bytes_14d AS double)/1073741824.0 * 0.02 AS op_bytes_14d_dollar_glacier_deep_archive,
    (CAST(op_count_14d AS double)/10000.0 + CAST(op_count_14d AS double)/10000.0 * 0.01)
                                                  AS op_count_14d_dollar_glacier_deep_archive,

    CAST(op_bytes_14d AS double)/1073741824.0 * 0.01 AS op_bytes_14d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_14d AS double)/10000.0 * 0.5 + CAST(op_count_14d AS double)/10000.0 * 0.01)
                                                  AS op_count_14d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_14d AS double)/1073741824.0 * 0.03 AS op_bytes_14d_dollar_glacier_instant_retrieval,
    CAST(op_count_14d AS double)/10000.0 * 0.1       AS op_count_14d_dollar_glacier_instant_retrieval,

    -- 21d
    CAST(op_bytes_21d AS double)/1073741824.0 * 0.01 AS op_bytes_21d_dollar_standard_ia,
    CAST(op_count_21d AS double)/10000.0 * 0.01     AS op_count_21d_dollar_standard_ia,

    CAST(op_bytes_21d AS double)/1073741824.0 * 0.02 AS op_bytes_21d_dollar_glacier_deep_archive,
    (CAST(op_count_21d AS double)/10000.0 + CAST(op_count_21d AS double)/10000.0 * 0.01)
                                                  AS op_count_21d_dollar_glacier_deep_archive,

    CAST(op_bytes_21d AS double)/1073741824.0 * 0.01 AS op_bytes_21d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_21d AS double)/10000.0 * 0.5 + CAST(op_count_21d AS double)/10000.0 * 0.01)
                                                  AS op_count_21d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_21d AS double)/1073741824.0 * 0.03 AS op_bytes_21d_dollar_glacier_instant_retrieval,
    CAST(op_count_21d AS double)/10000.0 * 0.1       AS op_count_21d_dollar_glacier_instant_retrieval,

    -- 30d
    CAST(op_bytes_30d AS double)/1073741824.0 * 0.01 AS op_bytes_30d_dollar_standard_ia,
    CAST(op_count_30d AS double)/10000.0 * 0.01     AS op_count_30d_dollar_standard_ia,

    CAST(op_bytes_30d AS double)/1073741824.0 * 0.02 AS op_bytes_30d_dollar_glacier_deep_archive,
    (CAST(op_count_30d AS double)/10000.0 + CAST(op_count_30d AS double)/10000.0 * 0.01)
                                                  AS op_count_30d_dollar_glacier_deep_archive,

    CAST(op_bytes_30d AS double)/1073741824.0 * 0.01 AS op_bytes_30d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_30d AS double)/10000.0 * 0.5 + CAST(op_count_30d AS double)/10000.0 * 0.01)
                                                  AS op_count_30d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_30d AS double)/1073741824.0 * 0.03 AS op_bytes_30d_dollar_glacier_instant_retrieval,
    CAST(op_count_30d AS double)/10000.0 * 0.1       AS op_count_30d_dollar_glacier_instant_retrieval,

    -- 60d
    CAST(op_bytes_60d AS double)/1073741824.0 * 0.01 AS op_bytes_60d_dollar_standard_ia,
    CAST(op_count_60d AS double)/10000.0 * 0.01     AS op_count_60d_dollar_standard_ia,

    CAST(op_bytes_60d AS double)/1073741824.0 * 0.02 AS op_bytes_60d_dollar_glacier_deep_archive,
    (CAST(op_count_60d AS double)/10000.0 + CAST(op_count_60d AS double)/10000.0 * 0.01)
                                                  AS op_count_60d_dollar_glacier_deep_archive,

    CAST(op_bytes_60d AS double)/1073741824.0 * 0.01 AS op_bytes_60d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_60d AS double)/10000.0 * 0.5 + CAST(op_count_60d AS double)/10000.0 * 0.01)
                                                  AS op_count_60d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_60d AS double)/1073741824.0 * 0.03 AS op_bytes_60d_dollar_glacier_instant_retrieval,
    CAST(op_count_60d AS double)/10000.0 * 0.1       AS op_count_60d_dollar_glacier_instant_retrieval,

    -- 90d
    CAST(op_bytes_90d AS double)/1073741824.0 * 0.01 AS op_bytes_90d_dollar_standard_ia,
    CAST(op_count_90d AS double)/10000.0 * 0.01     AS op_count_90d_dollar_standard_ia,

    CAST(op_bytes_90d AS double)/1073741824.0 * 0.02 AS op_bytes_90d_dollar_glacier_deep_archive,
    (CAST(op_count_90d AS double)/10000.0 + CAST(op_count_90d AS double)/10000.0 * 0.01)
                                                  AS op_count_90d_dollar_glacier_deep_archive,

    CAST(op_bytes_90d AS double)/1073741824.0 * 0.01 AS op_bytes_90d_dollar_glacier_flexible_retrieval,
    (CAST(op_count_90d AS double)/10000.0 * 0.5 + CAST(op_count_90d AS double)/10000.0 * 0.01)
                                                  AS op_count_90d_dollar_glacier_flexible_retrieval,

    CAST(op_bytes_90d AS double)/1073741824.0 * 0.03 AS op_bytes_90d_dollar_glacier_instant_retrieval,
    CAST(op_count_90d AS double)/10000.0 * 0.1       AS op_count_90d_dollar_glacier_instant_retrieval

  FROM base
)

SELECT *
FROM priced
WHERE inv_total_dollar_standard >= 10
ORDER BY inv_total_bytes DESC, prefix;