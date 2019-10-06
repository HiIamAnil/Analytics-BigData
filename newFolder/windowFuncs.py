#########################################################################################
# window funtions
# The below use case includes 
# 1.loading a file with | sep and then extracting year from date col.
# 2. Loading another file to df 
# 3. joining above dfs using requi cond 
# 4. Renaming columns
# 5. Then importing window and rank funtions
# 6. Defining window 
# 7. Saving the end results
# you cannot save df directly as textfile should convet it to rdd
# dff=dff.withColumn('year',dff['_c1'].substr(8,4) <<postion,length
#Note : dff = sc.textfile(trainingdata+"part-00000").map(lambda x: x.replace('[','').replace(']','').split('|')).toDF()
# best combination of python and spark
#########################################################################################

# lines=Source.fromFile("E:\scala\spark-data\movie-description") scala

from pyspark.sql import Window
from pyspark.sql.functions import rank
dff = spark.read.csv("E:\scala\spark-data\movie-description",sep="|")
movie_year_df = dff.withColumn('year',dff['_c2'].substr(8,4)).select("_c0","_c1","_c2","year")

movie_number_df = spark.read.csv("E:\scala\spark-data\movie-data.data",inferSchema = True,header=False, sep="\t").select("_c0","_c3")
cond=movie_number_df._c0 == movie_year_df._c0
mv_fi_df=movie_year_df.join(movie_number_df,cond,'right').withColumnRenamed("_c1","movie").withColumnRenamed("_c3","gross").select("movie","year","gross")

windowSpec = Window.partitionBy("year").orderBy("gross")

mv_fi_df=mv_fi_df.withColumn("rank",rank().over(windowSpec))
mv_fi_df.filter(mv_fi_df.rank == 1).show()
