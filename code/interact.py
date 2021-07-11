import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,ArrayType,IntegerType

spark = SparkSession.builder.appName('abc').getOrCreate()

df=spark.read.json("./files/")
df.printSchema()

# Test it out

schema = StructType([
    StructField("itemid", ArrayType(StringType()), False),
    StructField("itemname", ArrayType(StringType()), False),
    StructField("category", ArrayType(StringType()), False),
    StructField("quantity", ArrayType(StringType()), False),
    StructField("itemprice", ArrayType(StringType()), False)
])


a=[{"itemid": "1", "itemname": "apples", "category": "fruits","quantity": "6", "itemprice" : "2.00"}, {"itemid": "2", "itemname": "white bread", "category": "breads","quantity": "6", "itemprice" : "3.00"}, {"itemid": "3", "itemname": "oranges", "category": "fruits","quantity": "6", "itemprice" : "3.00"}]


#convertUDF = udf(lambda z: parse(z,txt),schema)

convertUDF = udf(parse)
df_payload.select(convertUDF(col("Items"))).show(truncate=False)

df_payload.withColumn("itemid",convertUDF(col("Items"),'itemid'))\
        .withColumn("itemname",convertUDF(col("Items")))\
        .withColumn("category",convertUDF(col("Items")))\
        .withColumn("quantity",convertUDF(col("Items")))\
        .withColumn("itemprice",convertUDF(col("Items")))


convertUDF = udf(parse,schema)


df.select(col("payload.*")).select(explode("Items").alias("flat")).select("flat.itemid","flat.itemname","flat.category","flat.itemprice","flat.quantity").show()


 df.select(col("payload.*")).show()

##########################################
##########################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Instantiating Spark Session
spark = SparkSession.builder.appName('abc').getOrCreate()

#Reading json to Spark Dataframe
df=spark.read.json("./files/")
df.printSchema()

#Explore Payload using spark sql
df_payload=df.select(F.col("payload.*"))


#save it as test

df_payload.registerTempTable("test")

# create fact table

#using explode function and spark sql
df_transformed=spark.sql("""select Transactionid,
explode(Items) as item,
cast(item.itemid as int),
item.itemname,cast(item.itemprice as float),
cast(item.quantity as int),
item.category,
paymentinfo,
cast(paymentinfo.paymentid as int),
paymentinfo.paymenttype,
cast(paymentinfo.totalprice as float),
cast(subtotal as float),
cast(cast(transactiontimestamp as int) as timestamp) as transaction_timestamp,
date(cast(cast(transactiontimestamp as int) as timestamp)) as trans_date from test""")


# Create fact table
# Cast to required datatypes
fact_trans=spark.sql(""" select cast(cast(transactiontimestamp as int) as timestamp) as transaction_timestamp,
	Transactionid,explode(Items.itemid) as itemid,
	paymentinfo.paymentid from test""")


df_transformed.select("transaction_timestamp","Transactionid","itemid","paymentid","quantity","trans_date").show()

# Create dim tables

# Items Table
df_transformed.select("itemid","itemname","itemprice").distinct().orderBy('itemid').show()

# Payment Table
df_transformed.select("paymentinfo.paymentid","paymentinfo.paymenttype","subtotal","paymentinfo.totalprice").distinct().orderBy('paymentid').show()

df_transformed.select("paymentid","paymenttype","subtotal","totalprice").distinct().orderBy('paymentid').show()


#perfom analytics

#which item is the most sold item ?


#which item has high revenue ?


#which category has most sales ?











