-- Landing zone table for Step Trainer sensor readings.
-- serialnumber stays a string so it matches the customer table for joins later.
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorreadingtime bigint,
  serialnumber string,
  distancefromobject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://stedi-lakehouse-2026/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
