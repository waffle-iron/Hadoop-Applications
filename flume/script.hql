CREATE TABLE tweetsavro
  ROW FORMAT SERDE
     'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ('avro.schema.url'='hdfs:///user/youruser/examples/schema/twitteravroschema.avsc') ;

LOAD DATA INPATH '/user/flume/tweets/avrotweets/FlumeData.*' OVERWRITE INTO TABLE tweetsavro;