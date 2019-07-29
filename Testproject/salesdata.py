

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, format_number, mean
sd = spark.read.csv('C:\spark\data\Ecom-Cust.csv',header = True, inferSchema = True)

sd2= sd.withColumnRenamed('Time on Website','webtime').withColumnRenamed('Time on App','apptime').withColumnRenamed('Length of Membership','meblen').withColumnRenamed('Yearly Amount Spent','expense').withColumnRenamed('Avg Session Length','sessionlen')

sd3 = sd2.select('Email','Address','Avatar',format_number(sd2['apptime'].cast('float'),2).alias('apptime'), format_number(sd2['webtime'].cast('float'),2).alias('webtime'),format_number(sd2['meblen'].cast('float'),2).alias('meblen'),format_number(sd2['expense'].cast('float'),2).alias('expense'))


sd3.orderBy(sd3['expense'].desc())

(sd3.orderBy(sd3['expense'].desc()).head(2)[0][6])
(sd3.select(mean('expense')).head(1)[0])
# print(sd3.orderBy(sd3['expense'].desc()).tail(1))

# how to select a particular row 

print(sd3.orderBy(sd3['expense'].desc()).head(3)[2]) # index starts with 1

sd3.orderBy(sd3['expense'].desc()).head(3)[2] #to targer particular row

sd3.orderBy(sd3['expense'].desc()).collect()[2] #to targer particular row index starts with 0

from pyspark.sql.functions import year

#adding new columns by calculating its value from exisign column
yeardf=df.withColumn("Year", year(df['Date']))

sd3.select(max('apptime')).show()