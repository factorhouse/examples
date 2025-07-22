-- Set parallelism to match the number of Kafka partitions
SET 'parallelism.default' = '3';
-- Enable checkpointing for fault tolerance
SET 'execution.checkpointing.interval' = '1min';
-- Enable mini-batching for windowed aggregations
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1 s'; -- Shorter latency for windows
-- Purge state after 1 minute; windows are 5s, so this is a safe buffer
SET 'table.exec.state.ttl' = '1 min';

-- The query itself
WITH PlayerContribution AS (
  SELECT
    window_end,
    team_id,
    user_id,
    SUM(score) AS player_total_score,
    -- Calculate team's total score in the same window using an OVER clause
    SUM(SUM(score)) OVER (PARTITION BY window_start, window_end, team_id) as team_total_score
  FROM
    TABLE(TUMBLE(TABLE user_scores, DESCRIPTOR(event_time), INTERVAL '5' SECONDS))
  GROUP BY
    window_start, window_end, team_id, user_id
)
SELECT
  window_end,
  team_id,
  user_id,
  player_total_score,
  team_total_score,
  -- Calculate the contribution percentage
  (player_total_score * 100.0 / team_total_score) AS contribution_percentage
FROM PlayerContribution
-- Use QUALIFY to efficiently select the top player per team in each window
QUALIFY ROW_NUMBER() OVER (PARTITION BY window_end, team_id ORDER BY player_total_score DESC) = 1;