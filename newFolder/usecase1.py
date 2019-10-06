# *****************************************************************
# * create a new column with values based on a condition
# * it should be optimised
# * so broadcast a key,value pair over a cluster
# * also how to create udf
# * how to save a df into a file at a specified location after an operation
# * always register a fn as udf by importing udf utility
# ******************************************************************



from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
spark = SparkSession.builder.getOrCreate()
df2=spark.read.csv("E:/scala/spark-data/temp-data.csv",header=False)

refDict = {"TMAX":1,"TMIN":0,"PRCP":2}

broadcastRefDict = spark.sparkContext.broadcast(refDict)
# by using udf
def uf(x):
    return broadcastRefDict.value.get(x)
uf_fn = udf(uf)
df2.withColumn('code',uf_fn(df2['_c2'])).write.csv(path="E:\\spark\\data\\usecaseresults\\uc1.csv")

# directly placing the expression
getting error
# df3 = df2.withColumn('code', broadcastRefDict.value.get(df2['_c2']))
df3.write.csv(path="E:\\spark\\data\\usecaseresults\\uc1.csv")

# C:\Users\Administrator\PycharmProjects\Usecases\src\usecase1.py

