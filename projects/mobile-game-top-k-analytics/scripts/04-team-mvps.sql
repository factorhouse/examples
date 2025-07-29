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

-- // Insert top player results into a Kafka topic
ADD JAR 'file:///tmp/connector/flink-sql-connector-kafka-3.3.0-1.20.jar';
ADD JAR 'file:///tmp/connector/flink-sql-avro-confluent-registry-1.20.1.jar';

INSERT INTO team_mvps
WITH player_agg AS (
  SELECT
    user_id,
    team_id,
    SUM(score) AS player_total
  FROM user_scores
  GROUP BY user_id, team_id
), team_agg AS (
  SELECT
    team_id,
    team_name,
    SUM(score) AS team_total
  FROM user_scores
  GROUP BY team_id, team_name
), contrib_agg AS (
  SELECT
    pt.user_id,
    tt.team_name,
    pt.player_total,
    tt.team_total,
    pt.player_total * 1.0 / tt.team_total AS contrib_ratio
  FROM player_agg pt
  JOIN team_agg tt
    ON  pt.team_id = tt.team_id
), top_player_per_team AS (
  SELECT
    team_rnk,
    user_id,
    team_name,
    player_total,
    team_total,
    contrib_ratio
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY team_name ORDER BY contrib_ratio DESC, user_id ASC) as team_rnk
    FROM contrib_agg
  )
  WHERE team_rnk = 1
)
SELECT
  rnk,
  user_id,
  team_name,
  player_total,
  team_total,
  contrib_ratio
FROM (
  SELECT
    user_id,
    team_name,
    player_total,
    team_total,
    contrib_ratio,
    ROW_NUMBER() OVER (ORDER BY contrib_ratio DESC) as rnk
  FROM top_player_per_team
)
WHERE rnk <= 10;