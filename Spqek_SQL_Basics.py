from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections

spark=SparkSession.builder.appName("SparkSQL").getOrCreate()


#DEFINE SCHEMA
def mapper(line):
    fields=line.split('|')
    return Row(movieId=int(fields[0]), movie_name=fields[1].encode('utf-8'))

lines=spark.sparkContext.textFile("hdfs:///user/jkaul/ml-100k/u.item")
items=lines.map(mapper)

#Infer Schema and register DF as table
schema_item=spark.createDataFrame(items).cache()
schema_item.withColumn('movie_id_plus10',schema_item['movieId']+10).show()

print('\n \n \n \n ')
"""
schema_item.createOrReplaceTempView("items")

m_query=spark.sql("SELECT * FROM items WHERE  movieId>200 AND movieId<400")

for results in m_query.collect():
    print results
"""

schema_item.filter((schema_item['movieId'] > 400) & (schema_item['movieId'] < 414)).show()

print('\n \n \n \n ')

schema_item.show()
