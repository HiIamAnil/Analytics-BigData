pyspark --master yarn \
  --conf spark.ui.port=12569 \
  --num-executors 2 \
  --executor-memory 512M

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('retail').getOrCreate()
orders=spark.read.csv("C:\spark\data\retail_db\orders.csv")
order_items=spark.read.csv("C:\spark\data\retail_db\order_items.csv")

for i in orders. \
map(lambda o: o.split(,)[3]). \
distinct(). \
collect():
