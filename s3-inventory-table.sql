CREATE EXTERNAL TABLE `{inventory_bucket_name}`(
  `bucket` string COMMENT 'from deserializer', 
  `key` string COMMENT 'from deserializer', 
  `versionid` string COMMENT 'from deserializer', 
  `islatest` string COMMENT 'from deserializer', 
  `isdeletemarker` string COMMENT 'from deserializer', 
  `size` string COMMENT 'from deserializer', 
  `lastmodifieddate` string COMMENT 'from deserializer', 
  `storageclass` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'escapeChar'='\\', 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://{inventory_bucket_name}/.../data'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1757249847')