-- trend趋势表
DROP TABLE if exists DS_BANYAN_COMMON_TREND;
CREATE table DS_BANYAN_COMMON_TREND (
    "pk" VARCHAR  PRIMARY KEY,  -- source|id|type
    "r"."source" VARCHAR,
    "r"."id" VARCHAR,
    "r"."parent_id" VARCHAR,
    "r"."type" VARCHAR,
    "r"."update_date" VARCHAR ,
    "r"."data" VARCHAR
) COMPRESSION='SNAPPY';