import sys;
from pyspark import SparkContext;
from csv import reader;
from datetime import *;
import re;
import csv
import cStringIO

def row2csv(row):
	buffer = cStringIO.StringIO()
	writer = csv.writer(buffer)
	writer.writerow([str(s).encode("utf-8") for s in row])
	buffer.seek(0)
	return buffer.read().strip();

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
	
	#overall crimes
	overall = data.map(lambda x: (x[0],x[1],x[6],x[7],x[8],x[9],x[11],x[13],x[14]));
        overall = overall.map(lambda x: (x[2],x[3],x[4]));
	#bronx
	bronx = data.filter(lambda x : x[13] == 'BRONX');
	bronx = bronx.map(lambda x: (x[0],x[1],x[6],x[7],x[8],x[9],x[11],x[13],x[14]));
	bronx = bronx.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	#brooklyn
        brooklyn = data.filter(lambda x : x[13] == 'BROOKLYN');
        brooklyn = brooklyn.map(lambda x: (x[0],x[1],x[6],x[7],x[8],x[9],x[11],x[13],x[14]));
        brooklyn = brooklyn.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	#manhattan
        manhattan = data.filter(lambda x : x[13] == 'MANHATTAN');
        manhattan = manhattan.map(lambda x: (x[0],x[1],x[6],x[7],x[8],x[9],x[11],x[13],x[14]));
        manhattan = manhattan.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	#queens
        queens = data.filter(lambda x : x[13] == 'QUEENS');
        queens = queens.map(lambda x: (x[0],x[1],x[6],x[7],x[8],x[9],x[11],x[13],x[14]));
        queens = queens.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	#staten island
        staten = data.filter(lambda x : x[13] == 'STATEN ISLAND');
        staten = staten.map(lambda x: (x[0],x[1],x[6],x[7],x[8],x[9],x[11],x[13],x[14]));
        staten = staten.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));

	#count
	count_overall = overall.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect();
	count_bronx = bronx.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect();
        count_brooklyn = brooklyn.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect();
        count_manhattan = manhattan.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect();
        count_queens = queens.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect();
        count_staten = staten.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect();
	
	#parallelize count
	final_overall=sc.parallelize(count_overall);
	final_bronx=sc.parallelize(count_bronx);
	final_brooklyn=sc.parallelize(count_brooklyn);
	final_manhattan=sc.parallelize(count_manhattan);
	final_queens=sc.parallelize(count_queens);
	final_staten=sc.parallelize(count_staten);	

	#save the file
	final_overall.saveAsTextFile("overall-crime.out");
	final_bronx.saveAsTextFile("bronx-crime.out");
	final_brooklyn.saveAsTextFile("brooklyn-crime.out");
	final_manhattan.saveAsTextFile("manhattan-crime.out");
	final_queens.saveAsTextFile("queens-crime.out");
	final_staten.saveAsTextFile("staten-crime.out");
	#csv
	final_overall=final_overall.map(row2csv);
	final_overall.saveAsTextFile("overall-crime.csv");
