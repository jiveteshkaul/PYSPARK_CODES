from pyspark import SparkConf, SparkContext


def loadMovieNames():
    Movie_Name={}
    with open("/home/jkaul/u.item") as f:
        for line in f:
            fields=line.split("|")
            Movie_Name[int(fields[0])]=fields[1].decode('ascii','ignore')
    return Movie_Name

def makePairs((user,ratings)):
    (movie1,rating1)=ratings[0]
    (movie2,rating2)=ratings[1]
    return ((movie1,movie2),(rating1,rating2))

def filterDuplicates((userId,ratings)):
    (movie1,rating1)=ratings[0]
    (movie2,rating2)=ratings[1]
    return movie1 < movie2

conf=SparkConf().setAppName('Movie Similarity')
sc=SparkContext(conf=conf)
