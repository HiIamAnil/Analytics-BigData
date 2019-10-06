############################################################################################
#coalesce is efficient than repartition when you apply these with same no of parti then coa is done first
# you can use glom() to see the data as an array from each partition provided if your dataset is small
#
#***Usecase: Count how many users have clicked a link which is not their subscribed topic***
# 2 datasets are available
# UserData                                              Events
# |UserId|SubTopicInfo|                                 |UserId|ClickLinkInfo|
# |......|............|                                 |......|............|
# |......|............|                                 |......|............|
############################################################################################
