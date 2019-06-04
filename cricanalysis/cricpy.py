# pyspark --master yarn \
#   --conf spark.ui.port=12569 \
#   --num-executors 2 \
#   --executor-memory 512M


from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

spark=SparkSession.builder.appName('cric').getOrCreate()

df=spark.read.csv('C:\spark\data\deliveries.csv',header='True',inferSchema='True')

#11df.select('bowling_team','total_runs').groupBy('bowling_team').sum('total_runs').show()

def hih_mar_win(df,x):
	highest=0
	diff=0
	match=0
	for i in range(1,x):
		c=df.filter('match_id == i and inning NOT IN (3,4)')

		# principles 
		# 1. cleanse the data always
		# 2. always filter the data as early as possible

		# victory margin always depends upon 1st batting side no matter what second batting will do
		# so we should matterke sure that second side batting is complete played or not 


		all_out=c.filter((c.inning == 2)).select(countDistinct('player_dismissed')).collect()[0][0]
		all_over=c.filter((c.inning == 2)).select(countDistinct('over'))



		if( (all_out.collect()[0][0] == 10) | (all_over.collect()[0][0] == 20) ):
			# print('i inside fir if')
			d=c.groupBy('inning').sum('total_runs')
			diff=abs(d.collect()[1][1] - d.collect()[0][1])
			print(i)
			if(diff>highest):
				# print('i inside sec if')
				highest=diff
				match=i
	print("match number is {}".format(match))
	print("vicotry margin is {}".format(highest))
	return match


hih_mar_win(df,578)


# if ():  #i != 242 & i != 487 & i != 512
			# df2=df.filter(df.inning)