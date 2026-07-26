-- Landing zone table for accelerometer readings from the mobile app.
-- user and timestamp are reserved words, so they need backticks here.
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  `user` string,
  `timestamp` bigint,
  x double,
  y double,
  z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://stedi-lakehouse-2026/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
