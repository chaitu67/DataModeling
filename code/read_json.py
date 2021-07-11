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
fact_trans=df_transformed.select("transaction_timestamp","Transactionid","itemid","paymentid","quantity","trans_date")

# Create dim tables

# Items Table
dim_item=df_transformed.select("itemid","itemname","itemprice").distinct().orderBy('itemid')

# Payment Table
dim_payment=df_transformed.select("paymentid","paymenttype","subtotal","totalprice").distinct().orderBy('paymentid')


#perfom analytics

#which item is the most sold item ?


#which item has high revenue ?


#which category has most sales ?
