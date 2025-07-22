-- // Switch to the catalog and database
USE CATALOG demo_hv;
CREATE DATABASE IF NOT EXISTS game_analytics;
USE game_analytics;

-- // Source table
DROP TABLE IF EXISTS user_scores;
CREATE TABLE user_scores (
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
DROP TABLE IF EXISTS top_teams;
CREATE TABLE top_teams (
  rnk         BIGINT,
  team_id     STRING,
  team_name   STRING,
  total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'top-teams',
  'properties.bootstrap.servers' = 'kafka-1:19092',
  'properties.cleanup.policy' = 'compact',

  'key.format' = 'avro-confluent',
  'key.avro-confluent.schema-registry.url' = 'http://schema:8081',
  'key.avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'key.avro-confluent.basic-auth.user-info' = 'admin:admin',
  'key.avro-confluent.schema-registry.subject' = 'top-teams-key',
  
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema:8081',
  'value.avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'value.avro-confluent.basic-auth.user-info' = 'admin:admin',
  'value.avro-confluent.schema-registry.subject' = 'top-teams-value',
  
  'sink.parallelism' = '3'
);

-- // Top players table
DROP TABLE IF EXISTS top_players;
CREATE TABLE top_players (
  rnk         BIGINT,
  user_id     STRING,
  team_name   STRING,
  total_score BIGINT,
  PRIMARY KEY (rnk) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'top-players',
  'properties.bootstrap.servers' = 'kafka-1:19092',
  'properties.cleanup.policy' = 'compact',

  'key.format' = 'avro-confluent',
  'key.avro-confluent.schema-registry.url' = 'http://schema:8081',
  'key.avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'key.avro-confluent.basic-auth.user-info' = 'admin:admin',
  'key.avro-confluent.schema-registry.subject' = 'top-players-key',
  
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema:8081',
  'value.avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  'value.avro-confluent.basic-auth.user-info' = 'admin:admin',
  'value.avro-confluent.schema-registry.subject' = 'top-players-value',
  
  'sink.parallelism' = '3'
);