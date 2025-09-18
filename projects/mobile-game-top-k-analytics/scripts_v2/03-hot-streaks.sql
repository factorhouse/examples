-- // Switch to the catalog and database
USE CATALOG demo_hv;
USE game_analytics;

-- // Set configurations
SET 'pipeline.name' = 'LeaderboardHotStreaksVersion2';
SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '5 min';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '1000';

-- // Load dependent Jar files
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';
ADD JAR 'file:///tmp/connector/flink-connector-jdbc-3.3.0-1.20.jar';
ADD JAR 'file:////tmp/postgres/postgresql-42.7.3.jar';

-- // Insert hot streaks results into a database table
INSERT INTO hot_streaks_v2
WITH 
short_term_agg AS (
  SELECT
    user_id,
    event_time,
    score,
    SUM(score) OVER w_short AS short_term_sum,
    COUNT(score) OVER w_short AS short_term_count
  FROM user_scores_v2
  WINDOW w_short AS (
      PARTITION BY user_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW
  )
),
long_term_agg AS (
  SELECT
    user_id,
    event_time,
    short_term_sum,
    short_term_count,
    SUM(score) OVER w_long AS long_term_sum,
    COUNT(score) OVER w_long AS long_term_count
  FROM short_term_agg
  WINDOW w_long AS (
      PARTITION BY user_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW
  )
),
current_hotness AS (
  SELECT
    user_id,
    event_time,
    (short_term_sum * 1.0 / NULLIF(short_term_count, 0)) AS short_term_avg,
    (long_term_sum * 1.0 / NULLIF(long_term_count, 0)) AS long_term_avg,
    ((short_term_sum * 1.0 / NULLIF(short_term_count, 0)) / NULLIF((long_term_sum * 1.0 / NULLIF(long_term_count, 0)), 0)) AS current_hotness_ratio
  FROM long_term_agg
),
latest_user_hotness AS (
  SELECT
    user_id,
    short_term_avg,
    long_term_avg,
    current_hotness_ratio
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) as rn
    FROM current_hotness
  )
  WHERE rn = 1
),
ranked_hotness AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY current_hotness_ratio DESC, user_id ASC) as rnk,
    user_id,
    short_term_avg,
    long_term_avg,
    current_hotness_ratio
  FROM latest_user_hotness
)
SELECT
  rnk,
  user_id,
  short_term_avg,
  long_term_avg,
  current_hotness_ratio AS peak_hotness
FROM ranked_hotness
WHERE rnk <= 10;
