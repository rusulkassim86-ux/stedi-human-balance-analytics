-- Landing zone table for customer data straight from the STEDI website.
-- Dates are stored as epoch milliseconds, so they're typed as bigint.
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  customername string,
  email string,
  phone string,
  birthday string,
  serialnumber string,
  registrationdate bigint,
  lastupdatedate bigint,
  sharewithresearchasofdate bigint,
  sharewithpublicasofdate bigint,
  sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://stedi-lakehouse-2026/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
