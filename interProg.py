

from pyspark.sql.functions import format_number


flights=flights.na.replace('NA','0')

# df= flights.select('carrier',format_number(flights['air_time'].cast('float'),1)).orderBy('air_time',ascending=False).show()


# df1=flights.na.replace('NA','0')
# df1=flights.select(format_number(['air_time'].cast('float'),1)).orderBy('air_time',ascending=False).show(5,False)

