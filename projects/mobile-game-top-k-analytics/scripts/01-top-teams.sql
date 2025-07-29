-- // Switch to the catalog and database
USE CATALOG demo_hv;
USE game_analytics;

-- // Set configurations
SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '1 min';
-- Set state TTL to be longer than the max team lifetime (40 mins) to ensure correctness
SET 'table.exec.state.ttl' = '60 min';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '6 s';
SET 'table.exec.mini-batch.size' = '10000';

-- // Insert top teams results into a Kafka topic
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';

INSERT INTO top_teams
WITH team_ranks AS (
  SELECT
    team_id,
    team_name,
    CAST(total_score AS BIGINT) AS total_score,
    ROW_NUMBER() OVER (ORDER BY total_score DESC, team_name ASC) as rnk
  FROM (
    SELECT
      team_id,
      MAX(team_name) AS team_name,
      SUM(score) AS total_score
    FROM user_scores
    GROUP BY team_id
  )  
)
SELECT
  rnk,
  team_id,
  team_name,
  total_score
FROM team_ranks
WHERE rnk <= 10;
