from pyspark import SparkConf, SparkContext
configs=SparkConf().setAppName("Movie_Ratings")
sc=SparkContext(conf=configs)

ip='hdfs:///user/jkaul/ml-100k/u.data'

raw_data=sc.textFile(ip)
m_id_count=raw_data.map(lambda x : (x.split()[1],1)).reduceByKey(lambda x,y : x+y)
m_id_sorted=m_id_count.map(lambda (x,y):(y,x)).sortByKey(ascending = False)

results=m_id_sorted.collect()

for result in results:
    count=str(result[0])
    name=result[1]

    print name +'\t' + count
