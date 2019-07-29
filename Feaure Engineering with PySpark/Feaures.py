
import sys


# Project decide selling price of a house. We have dataset of house selling price of 2017, using this sample we should determine the selling price of house in 2018
# This dataset is sample of details of 5.5 milliion sold out houses in US.

# Given data has
# Listing Price say X, we need to predict sales closing price Y which is unknown.
# 	Attributes available
# 	-Date, Location, Size, Price, Construction Materials, Amenties.

# Limitations of data we have 
# 	- It has covered very small geographical area. To apply the analysis based on this into new area poses a risk.
# 	- It is limited to residential areas. Not known about the business location.
# 	- It has only one year of data. This poses problem for drawing conclusions when it comes to trends during seasons.



# Read the file into a dataframe


df = spark.read.parquet('/src/../Real_Estate.parq')

# Select our dependent variable

Y_df = df.select(['SALESCLOSEPRICE'])



# Display summary statistics

Y_df.describe().show()

Let's suppose each month you get a new file. You know to expect a certain number of records and columns. In this exercise we will create a function that will validate the file loaded.

def check_load(df, num_records, num_columns):
	if(num_record