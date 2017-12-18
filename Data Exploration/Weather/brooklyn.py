import sys;
from pyspark import SparkContext;
from csv import reader;
from datetime import *;
import re;
import csv
import cStringIO

def row2csv(data):
	return ','.join(str(d) for d in data)


if __name__ == "__main__":
	#loading the file
	sc=SparkContext();
	data = sc.textFile(sys.argv[1], 1);

	#converting the data to rdd
	data = data.mapPartitions(lambda x : reader(x));

	#extracting header
	header = data.first();

	#removing the header from the data
	data = data.filter(lambda x : x != header);

	brooklyn = data.map(lambda x: (x[0],x[1]));

	#count
	count_brooklyn = brooklyn.map(lambda x: (x[0],1)).reduceByKey(lambda x,y:x+y).collect();

	#parallelize count and format
	count_brooklyn=sc.parallelize(count_brooklyn);
	count_brooklyn = count_brooklyn.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	#save the file as csv
	count_brooklyn=count_brooklyn.map(row2csv);
	count_brooklyn.saveAsTextFile("brooklyn-crime-feb.csv");

