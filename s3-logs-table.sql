## Log object key format
#  - [DestinationPrefix][YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
## Log object key example
#  - 2025-07-01-10-12-56-[UniqueString]


CREATE EXTERNAL TABLE `{logs_bucket_name}`(
  `bucketowner` string COMMENT '', 
  `bucket_name` string COMMENT '', 
  `requestdatetime` string COMMENT '', 
  `remoteip` string COMMENT '', 
  `requester` string COMMENT '', 
  `requestid` string COMMENT '', 
  `operation` string COMMENT '', 
  `key` string COMMENT '', 
  `request_uri` string COMMENT '', 
  `httpstatus` string COMMENT '', 
  `errorcode` string COMMENT '', 
  `bytessent` bigint COMMENT '', 
  `objectsize` bigint COMMENT '', 
  `totaltime` string COMMENT '', 
  `turnaroundtime` string COMMENT '', 
  `referrer` string COMMENT '', 
  `useragent` string COMMENT '', 
  `versionid` string COMMENT '', 
  `hostid` string COMMENT '', 
  `sigv` string COMMENT '', 
  `ciphersuite` string COMMENT '', 
  `authtype` string COMMENT '', 
  `endpoint` string COMMENT '', 
  `tlsversion` string COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.RegexSerDe' 
WITH SERDEPROPERTIES ( 
  'input.regex'='([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://{logs_bucket_name}/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1691918923')
