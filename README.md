
# Data Engineering:
Role of a Data engineer from customer to Website and analytics.

<img width="948" alt="image" src="https://user-images.githubusercontent.com/14869837/124615169-61d20780-de4b-11eb-911e-7f2563dd1b4d.png">

# Framework Explained:
The goal of this repo is to give an understanding of how to use pyspark and spark sql to transform raw data to information.Using diffrent techniques as part of the process.This can either be done on AWS EMR using pyspark,HDFS and Apache Hive or On local system having  pyspark,spark sql and local file system,Both these methodologies will be discussed here.

<img width="676" alt="image" src="https://user-images.githubusercontent.com/14869837/124387685-e1c66900-dcb5-11eb-9cc5-f58c8d3fa8a3.png">


## To Install :
* Install pyspark on the local system
* Linux operating system on the local system

## Prerequisite :
* Install pyspark3.1 on the local system
* Linux operating system on the local system

## Topics Covered :
* Insights into diffrent types of data sources
* Parsing nested json files using pyspark
* Transfroming data using spark sql
* Constructing a datawarehouse from tranformed data.
* Demo on AWS EMR using Hive as the metastore on HDFS

### Insights into diffrent types of data sources:
###### Storage file type :
* Parquet,CSV,AVRO,JSON,API's
###### Storage system :
* Localfile system
* HDFS
* AWS S3

## Parsing nested json files using pyspark :
##### Instantiate a Spark Session :
`spark = SparkSession.builder.appName('abc').getOrCreate()`

##### Read Json as a spark dataframe:
`df=spark.read.json("./files/")`

##### Resgister as Temp table:
`df.registerTempTable("test")`


##### Parse Temp table using spark SQL
`spark.sql(""" select * from test """).show()`


## Transfroming data using spark sql
##### DW Schema Structure :

##### Identify fact and dimension tables:

##### spark sql to transform data:

## Constructing a datawarehouse from tranformed data






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
* df.repartition(6).write.mode("overwrite").save('path', "parquet")
* spark.sql("alter table etl.trans set location 'path'")
* df=spark.sql("""select payload.Items[0]['itemid'] as item_id,
        payload.Items[0]['itemname'] as item_name,
        payload.Items[0]['itemprice']as item_price,
        payload.Items[0]['quantity'] as quantity
        from test""")
