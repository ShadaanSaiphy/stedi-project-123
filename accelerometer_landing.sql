CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  timestamp BIGINT,
  user STRING,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1')
LOCATION 's3://stedi-project-123/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');