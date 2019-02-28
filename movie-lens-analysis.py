from pyspark.sql import SparkSession
from pyspark.sql import Row

def mapper1(line):
    fields=line.split('|')
    return Row(movieId=int(fields[0]), movie_name=fields[1].encode('utf-8'))

def mapper(line):
    lines=line.split()
    return Row(Movie_ID=int(lines[1]))

spark=SparkSession.builder.appName('Move_analysis').getOrCreate()

lines=spark.sparkContext.textFile("hdfs:///user/jkaul/ml-100k/u.item")
items=lines.map(mapper1)

df_items=spark.createDataFrame(items).cache()

raw_data=spark.sparkContext.textFile('hdfs:///user/jkaul/ml-100k/u.data')
data=raw_data.map(mapper)

df_data=spark.createDataFrame(data).cache()

tmp_df=df_data.groupBy(df_data['Movie_ID']).count().orderBy('count', ascending=False)

tmp_df.join(df_items, df_items.movieId == tmp_df.Movie_ID).orderBy('count', ascending=False).select(df_items['movie_name'],tmp_df['count']).show()
