# -*- coding: utf-8 -*-
"""
Created on Sat Jun  8 16:12:51 2019

@author: satis
"""

#joins
#Typical project 

#first load the data into HDFS

#Then load the data from HDFS/equivalent to RDD or DFs
orders=sc.textFile('dbfs:/FileStore/tables/orders.csv')
ordersItem=sc.textFile('dbfs:/FileStore/tables/orders.csv')

#perform transformations using Map 

    #get orderId, date and price
    #price is in orderItem and date is order
    #so we need to perform join, for that we need to join both the ds
    #available ds need to be converted into key value/ pairedRDDs using map transformation

ordersMap = orders.map(lambda x:((x.split(',')[0]),(x.split(',')[1][:10])))
orderItemsMap = ordersItem.map(lambda y:((y.split(',')[1]),float(y.split(',')[4])))

#both the ds are created with key,value pairs now the pyspark join 
#will understand the first value as the key and joins both ds

ordersMap.join(orderItemMap)


