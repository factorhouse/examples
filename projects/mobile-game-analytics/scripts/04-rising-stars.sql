-- // Switch to the catalog and database
USE CATALOG demo_hv;
USE game_analytics;

-- // Set job-specific configurations
SET 'parallelism.default' = '3';
SET 'table.exec.state.ttl' = '1 min';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1 s';
SET 'table.exec.mini-batch.size' = '100';
SET 'execution.checkpointing.interval' = '5s';

-- // Insert top player results into a Kafka topic
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';

INSERT INTO rising_stars
WITH RankImprovements AS (
  SELECT
    window_end,
    user_id,
    curr_rank,
    (LAG(curr_rank, 1, curr_rank) OVER (PARTITION BY user_id ORDER BY window_end) - curr_rank) AS rank_improvement
  FROM (
    SELECT
      window_start,
      window_end,
      user_id,
      RANK() OVER (PARTITION BY window_end ORDER BY SUM(score) DESC) as curr_rank
    FROM
      TABLE(TUMBLE(TABLE user_scores, DESCRIPTOR(event_time), INTERVAL '5' SECONDS))
    GROUP BY
      window_start, window_end, user_id
  )
)
-- 2. Main query using the Top-N pattern to get the 10 best improvements per window
SELECT
  window_end,
  user_id,
  curr_rank,
  rank_improvement
FROM (
  SELECT
    *,
    -- Now, rank the improvements themselves within each window
    ROW_NUMBER() OVER (PARTITION BY window_end ORDER BY rank_improvement DESC) as rn
  FROM RankImprovements
  -- Pre-filter for players who actually improved to make the final ranking more efficient
  WHERE rank_improvement > 0
)
-- Filter to get only the top 10 improvements for each window
WHERE rn <= 10;



CREATE TEMPORARY VIEW WindowedUserScores AS
SELECT
    window_start,
    window_end,
    user_id,
    SUM(score) AS total_score
FROM
    TABLE(TUMBLE(TABLE user_scores, DESCRIPTOR(event_time), INTERVAL '5' SECONDS))
GROUP BY
    window_start, window_end, user_id;

WITH WindowedRanks AS (
  SELECT
    window_start,
    window_end,
    user_id,
    -- This RANK now works because it's reading from the view, not the TUMBLE function.
    RANK() OVER (PARTITION BY window_start, window_end ORDER BY total_score DESC) as curr_rank
  FROM WindowedUserScores -- Reading from the VIEW is the key.
),
-- CTE to calculate the rank improvement
RankImprovements AS (
  SELECT
    window_end,
    user_id,
    curr_rank,
    (LAG(curr_rank, 1, curr_rank) OVER (PARTITION BY user_id ORDER BY window_end) - curr_rank) AS rank_improvement
  FROM WindowedRanks
)
-- Final SELECT to filter for only those players who improved their rank
SELECT
  window_end,
  user_id,
  curr_rank,
  rank_improvement
FROM RankImprovements
WHERE rank_improvement > 0;