from pyspark.sql import SparkSession
from pyspark.sql import Row

spark=SparkSession.builder.appName('MLAnalysis').getOrCreate()
sc = spark.sparkContext

#################USING U.GENRE ##########################
# Creating genre_rdd from u.genre
genre_rdd=spark.sparkContext.textFile('hdfs:///user/jkaul/ml-100k/u.genre')
#genre_rdd = genre_rdd.map(lambda x : x.split(',')[0].encode('ascii','ignore').split('|')[0])
genre_rdd = genre_rdd.map(lambda x : x.split('|')[0])
'''
tmp=genre_rdd.take(10)
print type(tmp)
for a in tmp:
    print a
'''
# Making genre_list as global to use it in function
global genre_list
genre_list= genre_rdd.collect()


###################USING U.ITEM ##################################
# creating item_rdd from u.item
item_rdd = sc.textFile("hdfs:///user/jkaul/ml-100k/u.item")

# Changing the unicode to Strings and seperatings columns
item_rdd = item_rdd.map(lambda x: x.split(',')[0].encode('ascii', 'ignore'))

# function to map movie id with genre
def parser(line):
  fields = line.split('|')
  MID = fields[0]
  result = []
  for count, item in enumerate(fields[5:], start = 0):
    if item == '1':
        result.append((MID, genre_list[count]))
  return result

# creating rdd with movieid, genre
item_rdd = item_rdd.map(parser)

item_rdd = item_rdd.flatMap(lambda x:x)

item_df = item_rdd.map(lambda line: Row(MID = line[0], genre = line[1])).toDF()

######USING U.USER FILE #######################
user_rdd = sc.textFile("hdfs:///user/jkaul/ml-100k/u.user")

user_rdd = user_rdd.map(lambda x: x.split(',')[0].encode('ascii','ignore').split('|'))
user_rdd = user_rdd.map(lambda x:(x[0], x[3]))

user_df = user_rdd.map(lambda line: Row(UID = line[0], profession = line[1])).toDF()
#################################################

data_rdd = sc.textFile("hdfs:///user/jkaul/ml-100k/u.data")

data_rdd = data_rdd.map(lambda x : x.split(',')[0].encode('ascii','ignore').split())

data_rdd = data_rdd.map(lambda x: (x[0], x[1], x[2]))

data_df = data_rdd.map(lambda line: Row(UID = line[0], MID = line[1], rating = line[2])).toDF()

####################################################

# data_df : MID, UID, rating  ---- a
# user_df : UID, profession ------ b
# item_df : MID, genre ------ c

data_user_df = data_df.join(user_df, data_df.UID == user_df.UID, 'inner').select(data_df.rating, data_df.MID, user_df.profession)

result = data_user_df.join(item_df, data_user_df.MID == item_df.MID, 'inner') \
                   .select(data_user_df.rating, data_user_df.profession, item_df.genre)

result = result.select(result.rating.cast('int').alias('rating'), result.profession, result.genre)

from pyspark.sql.functions import *

trail1 = result.groupBy(col("profession"), col("genre")).agg({"rating":"count"})
trail1 = trail1.select(col("profession"), col("genre"), col("count(rating)").alias("total_ratings")).orderBy("profession")
trail1.show()

trail2 = result.select('profession', 'genre', 'rating').filter('rating>3')
trail2.show()

trail3 = trail2.groupBy(col("profession"), col("genre")).agg({"rating":"count"})
trail3.show()

trail4 = trail3.select(col("profession"), col("genre"), col("count(rating)").alias("num_ratings"))
trail4.show()

output = trail1.join(trail4, (trail1.profession == trail4.profession) & (trail1.genre == trail4.genre), 'inner') \
                .select(trail1.profession, trail1.genre, trail1.total_ratings, trail4.num_ratings)

output1 = output.select(col("profession"), col("genre"), (col("num_ratings")/col("total_ratings")).alias("wa_ratings"))

final_output = output1.orderBy(output1["profession"], output1["genre"], desc("wa_ratings"))
final_output.show(final_output.count())

final_pre.printSchema()
final_output.printSchema()
