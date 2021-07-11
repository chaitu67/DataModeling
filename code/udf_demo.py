import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('abc').getOrCreate()
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)
df.show(truncate=False)

def upperCase(str):
    return str.upper()


upperCaseUDF = udf(lambda z:upperCase(z))   

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))).show()