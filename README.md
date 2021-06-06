# ETL Framework Explained:
The goal of this repo is to give an understanding of how to use pyspark and spark sql to transform raw data to information.Using diffrent techniques as part of the process.This can either be done on AWS EMR using pyspark,HDFS and Apache Hive or On local system having  pyspark,spark sql and local file system,Both these methodologies will be discussed here.

## To Install :
* Install pyspark on the local system
* Linux operating system on the local system

## Prerequisite :
* Install pyspark on the local system
* Linux operating system on the local system

## Topics Covered :
* Insights into diffrent types of data sources
* Parsing nested json files using pyspark
* Transfroming data using spark sql
* Constructing a datawarehouse from tranformed data.
* Demo on AWS EMR using Hive as the metastore on HDFS

### Parsing nested json files using pyspark :
###### Instantiate a Spark Session :
<addr> spark.


### Insights into diffrent types of data sources:
###### Storage file type :
*Parquet,CSV,AVRO,JSON
###### Storage system :
* Localfile system
* HDFS
* AWS S3


## HDFS Commands :

* hdfs dfs -ls "pathname"
* hdfs dfs -mkdir "path/pathname"
* hdfs dfs -put "localfile/path" "HDFS directory/pathname"
* sudo -u hdfs hadoop fs -chown root /home/hadoop/
* sudo -u hdfs hadoop fs -put "/home/hadoop/test.json" "/home/hadoop/"

## Hive Commands :
* Create database etl;

## Pyspark Commands :
* df=spark.read.json("/home/hadoop/test.json")
* df.registerTempTable("test")
* df.repartition(6).write.mode("overwrite").save('hdfs://ip-10-0-2-82.ec2.internal:8020/home/hadoop/etl/trans/', "parquet")
* spark.sql("alter table etl.trans set location 'hdfs://ip-10-0-2-82.ec2.internal:8020/home/hadoop/etl/trans'")
* df=spark.sql("""select payload.Items[0]['itemid'] as item_id,
        payload.Items[0]['itemname'] as item_name,
        payload.Items[0]['itemprice']as item_price,
        payload.Items[0]['quantity'] as quantity
        from test""")
