-- // Switch to the catalog and database
USE CATALOG demo_hv;
USE game_analytics;

-- // Set configurations
SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '1 min';
-- State can be purged quickly because the calculation only looks back 60 seconds.
SET 'table.exec.state.ttl' = '5 min';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3 s';
SET 'table.exec.mini-batch.size' = '5000';

-- // Insert top player results into a Kafka topic
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';

INSERT INTO hot_streakers
WITH short_term_agg AS (
  SELECT
    user_id,
    event_time,
    score,
    SUM(score) OVER w_short AS short_term_sum,
    COUNT(score) OVER w_short AS short_term_count
  FROM user_scores
  WINDOW w_short AS (
      PARTITION BY user_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW
  )
), long_term_agg AS (
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
), peak_hotness AS (
  SELECT
    user_id,
    MAX(short_term_avg) AS short_term_avg,
    MAX(long_term_avg) AS long_term_avg,
    MAX(hotness_ratio) AS peak_hotness
  FROM (
    SELECT
        user_id,
        short_term_sum * 1.0 / NULLIF(short_term_count, 0) AS short_term_avg,
        long_term_sum * 1.0 / NULLIF(long_term_count, 0) AS long_term_avg,
        ((short_term_sum * 1.0 / NULLIF(short_term_count, 0)) / NULLIF((long_term_sum * 1.0 / NULLIF(long_term_count, 0)), 0)) AS hotness_ratio
    FROM long_term_agg
  ) 
  GROUP BY user_id
)
SELECT
  rnk,
  user_id,
  short_term_avg,
  long_term_avg,
  peak_hotness
FROM (
  SELECT
    user_id,
    short_term_avg,
    long_term_avg,
    peak_hotness,
    ROW_NUMBER() OVER (ORDER BY peak_hotness DESC, user_id ASC) as rnk
  FROM peak_hotness
)
WHERE rnk <= 10;