from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

import string
import math

def cleanUp(string):
	if string is not None:
		string = string.encode('ascii','ignore').encode('utf-8').lower()
		x = string.replace("\'s","").replace("\'","").translate(table)
	return x

def tokenize(string):
	tokenize=[]
	if string is not None:
		for x in string.encode('ascii','ignore').encode('utf-8').lower().split():
			x = x.replace("\'s","").replace("\'","")
			if x.find('http')==0 or x.find('@')==0 :
				tokenize.append(x.strip())
			else:
				for x in x.translate(table).split():
					if x not in stopwordsList:
						tokenize.append(x.strip())
	return tokenize

def wordDistance(list):
	wordDistance=[]
	l=len(list)
	n = 1
	while n <= 4:
		x = 0
		while x<l and x+n<l:
			wordDistance.append((list[x]+' '+list[x+n],n))
			#wordDistance.append((list[x+n]+' '+list[x],n*-1))
			x+=1
		n+=1
	return wordDistance


spark = SparkSession\
    .builder\
    .appName("twitter_collocation")\
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

table = string.maketrans("!\"$%&()*+,-./:;<=>?[\]^_`{|}~@#","                               ")
stopfile = 'hdfs://get-it-done-instance.c.get-it-done-project.internal:8020/hadoop/hdfs/stop-word-list.csv'
stopwordsList = sc.textFile(stopfile).flatMap(lambda x: x.split(',')).map(lambda x: x.encode('utf-8').strip()).collect()
stopwordsList.extend(['target','gt','rt','amp'])

textDF = sqlContext.read.json('hdfs://get-it-done-instance.c.get-it-done-project.internal:8020/temp/flume/twitterstream/*.?????????????')
textRDD = textDF.select("text").rdd.map(list).cache()

wordDistanceRDD=textRDD.map(lambda x: tokenize(x[0])).flatMap(lambda x: wordDistance(x)).cache()
wordMeanRDD=wordDistanceRDD.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0],x[1] + y[1])).map(lambda x:(x[0],float(x[1][0])/x[1][1]))
wordMeanDict=dict(wordMeanRDD.collect())
wordCountDict=dict(wordDistanceRDD.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y).collect())
wordStandardDevRDD=wordDistanceRDD.map(lambda x: (x[0],(float(x[1])-wordMeanDict[x[0]])**2)).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0],x[1] + y[1])).map(lambda x:(x[0],math.sqrt(float(x[1][0])/x[1][1])))
result=wordStandardDevRDD.map(lambda x: (x[0],wordMeanDict[x[0]],x[1],wordCountDict[x[0]])).cache()
lowestSDList=result.filter(lambda x:x[2]!=0.0).map(lambda x: (x[0],x[1],x[2],x[3],x[2]/x[1])).takeOrdered(25, key = lambda x: (x[2]))





lowestSDSchema = StructType([
    StructField("phrases", StringType(), True),
    StructField("mean", FloatType(), True),
    StructField("sd", FloatType(), True),
    StructField("count", IntegerType(), True),
    StructField("cov", FloatType(), True)
])
lowestSDDF=sqlContext.createDataFrame(lowestSDList,lowestSDSchema)

lowestSDDF.write \
    .format("jdbc") \
    .option("url","jdbc:mysql://get-it-done-instance.c.get-it-done-project.internal:3306/twitter") \
    .option("dbtable", "review_stats") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("user", "hive") \
    .option("password", "admin") \
    .mode('overWrite') \
    .save()



phrases=[]
for x in lowestSDList:
	temp=[]
	for y in x[0].split():
		temp.append(y+' ')
	for y in x[0].split():
		temp.append(y)
	phrases.append(temp)

	

def check(list):
	string = ''.join(list).encode('ascii','ignore').encode('utf-8').lower()
	for x in phrases:
		first=string.find(x[0])
		second=string.find(x[1])
		if second>=0 and first >= 0 and second > first:
			return (x[2]+' '+x[3],string)
	return None


textReviewList=textRDD.filter(lambda x: x[0] != None).map(lambda x: check(x)).filter(lambda x: x != None).collect()

textReviewSchema = StructType([
    StructField("phrases", StringType(), True),
    StructField("text", StringType(), True)
])

textReviewDF=sqlContext.createDataFrame(textReviewList,textReviewSchema)

## writing to mysql

textReviewDF.write \
    .format("jdbc") \
    .option("url","jdbc:mysql://get-it-done-instance.c.get-it-done-project.internal:3306/twitter") \
    .option("dbtable", "review") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("user", "hive") \
    .option("password", "admin") \
    .mode('overWrite') \
    .save()



spark.stop()
'''
>>> cd /usr/hdp/current/spark2-client/
>>>sudo -u hdfs ./bin/spark-submit  --master yarn-client --driver-cores 1 --executor-cores 1 /projects/twitterAnalysis/spark/collocations.py

'''