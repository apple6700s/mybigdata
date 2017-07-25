drop table if exists DS_COMMONS_KAFKA_OFFSET;
CREATE table DS_COMMONS_KAFKA_OFFSET    (
  "pk" VARCHAR  PRIMARY KEY,  --  topic|update_date|partition
  "r"."offset" VARCHAR
);