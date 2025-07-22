-- // Switch to the catalog and database
USE CATALOG demo_hv;
USE game_analytics;

-- // Set job-specific configurations
SET 'parallelism.default' = '3';
SET 'table.exec.state.ttl' = '24 h';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3 s';
SET 'table.exec.mini-batch.size' = '500';
SET 'execution.checkpointing.interval' = '5s';

-- // Insert top player results into a Kafka topic
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';

INSERT INTO top_players
WITH player_ranks AS (
  SELECT
    user_id,
    team_name,
    CAST(total_score AS BIGINT) AS total_score,
    ROW_NUMBER() OVER (ORDER BY total_score DESC) as rnk
  FROM (
    SELECT
      user_id,
      MAX(team_name) AS team_name,
      SUM(score) AS total_score
    FROM user_scores
    GROUP BY user_id
  )
)
SELECT
  rnk,
  user_id,
  team_name,
  total_score
FROM player_ranks
WHERE rnk <= 10;
