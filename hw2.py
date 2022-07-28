import pyspark
from csv import reader
from pyspark import SparkContext
import re

sc = SparkContext(appName="hw2")
sc.setLogLevel("ERROR")
data = sc.textFile("hdfs://group4-1:54310/hw1-input/")
rdd = data.mapPartitions(lambda x: reader(x))


rdd = rdd.filter(lambda x: (re.match(r'07/[0-9]{2}/[0-9]{4}', x[5])))
crimes = rdd.map(lambda x: (x[7],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(3)

mostCrime = rdd.map(lambda x: (x[13],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(1)

print("Most of the crime happening in New York and Total Reported Crimes: ", mostCrime )
print("Top 3 crimes that happened in the month of July: ", crimes)

rdd = data.mapPartitions(lambda x: reader(x))
rdd = rdd.filter(lambda x: ("DANGEROUS WEAPONS" in x[7]))
rdd = rdd.filter(lambda x: (re.match(r'07/[0-9]{2}/[0-9]{4}', x[5])))

print("Total number of crime reported with 'DANGEROUS WEAPONS' in the month of July: %s " % (rdd.count()))