-- // Switch to the catalog and database
USE CATALOG demo_hv;
CREATE DATABASE IF NOT EXISTS game_analytics;
USE game_analytics;

-- // Source table
DROP TABLE IF EXISTS user_scores_v2;
CREATE TABLE user_scores_v2 (
  `user_id`           STRING,
  `team_id`           STRING,
  `team_name`         STRING,
  `score`             INT,
  `event_time_millis` BIGINT,
  `readable_time`     STRING,
  `event_type`        STRING,
  `event_time`        AS TO_TIMESTAMP_LTZ(event_time_millis, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-score',
  'properties.bootstrap.servers' = 'kafka-1:19092',

  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://schema:8081',
  'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'avro-confluent.basic-auth.user-info' = 'admin:admin',
  'avro-confluent.schema-registry.subject' = 'user-score-value',
  'scan.startup.mode' = 'latest-offset'
);

-- // Top teams table
DROP TABLE IF EXISTS top_teams_v2;
CREATE TABLE top_teams_v2 (
  rnk         BIGINT,
  team_id     STRING,
  team_name   STRING,
  total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/fh_dev',
  'table-name' = 'demo.top_teams',
  'username' = 'db_user',
  'password' = 'db_password',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1s',
  'sink.parallelism' = '1',
  'sink.max-retries' = '3'
);

-- // Top players table
DROP TABLE IF EXISTS top_players_v2;
CREATE TABLE top_players_v2 (
  rnk         BIGINT,
  user_id     STRING,
  team_name   STRING,
  total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/fh_dev',
  'table-name' = 'demo.top_players',
  'username' = 'db_user',
  'password' = 'db_password',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1s',
  'sink.parallelism' = '1',
  'sink.max-retries' = '3'
);

-- // Hot streaks table
DROP TABLE IF EXISTS hot_streaks_v2;
CREATE TABLE hot_streaks_v2 (
  rnk             BIGINT,
  user_id         STRING,
  short_term_avg  DOUBLE,
  long_term_avg   DOUBLE,
  peak_hotness    DOUBLE,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/fh_dev',
  'table-name' = 'demo.hot_streaks',
  'username' = 'db_user',
  'password' = 'db_password',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1s',
  'sink.parallelism' = '1',
  'sink.max-retries' = '3'
);

-- // Team MVP table
DROP TABLE IF EXISTS team_mvps_v2;
CREATE TABLE team_mvps_v2 (
  rnk             BIGINT,
  user_id         STRING,
  team_name       STRING,
  player_total    BIGINT,
  team_total      BIGINT,
  contrib_ratio   DOUBLE,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/fh_dev',
  'table-name' = 'demo.team_mvps',
  'username' = 'db_user',
  'password' = 'db_password',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1s',
  'sink.parallelism' = '1',
  'sink.max-retries' = '3'
);
