import sys;
from pyspark import SparkContext;
from csv import reader;
from datetime import *;
import re;
import csv
import cStringIO
from operator import add

def row2csv(row):
	buffer = cStringIO.StringIO()
	writer = csv.writer(buffer)
	writer.writerow([str(s).encode("utf-8") for s in row])
	buffer.seek(0)
	return buffer.read().strip();
def toCSVLine(data):
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
	
	#precint
	precinct=data.filter(lambda x: x[14]== '75')
	precinct=precinct.map(lambda x: (x[6], str(x[7]).replace(',',' ')));

	#overall crimes
	overall = data.map(lambda x: (x[0],x[1],x[6],x[10],x[11],x[13],x[14],x[18]));
        overall = overall.map(lambda x: (x[7]));
	
	#bronx
	bronx = data.filter(lambda x : x[13] == 'BRONX');
	bronx = bronx.map(lambda x: (x[0],x[1],x[6],x[10],x[11],x[13],x[14],x[18]));
	#bronx = bronx.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	bronx = bronx.map(lambda x: (x[7]));
	#brooklyn
        brooklyn = data.filter(lambda x : x[13] == 'BROOKLYN');
        brooklyn = brooklyn.map(lambda x: (x[0],x[1],x[6],x[10],x[11],x[13],x[14],x[18]));
       	#brooklyn = brooklyn.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	brooklyn = brooklyn.map(lambda x: (x[7]));
	#manhattan
        manhattan = data.filter(lambda x : x[13] == 'MANHATTAN');
        manhattan = manhattan.map(lambda x: (x[0],x[1],x[6],x[10],x[11],x[13],x[14],x[18]));
        #manhattan = manhattan.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	manhattan = manhattan.map(lambda x: (x[7]));
	#queens
        queens = data.filter(lambda x : x[13] == 'QUEENS');
        queens = queens.map(lambda x: (x[0],x[1],x[6],x[10],x[11],x[13],x[14],x[18]));
        #queens = queens.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	queens = queens.map(lambda x: (x[7]));
	#staten island
	staten = data.filter(lambda x : x[13] =='STATEN ISLAND');
        staten = staten.map(lambda x: (x[0],x[1],x[6],x[10],x[11],x[13],x[14],x[18]));
        #staten = staten.map(lambda x: (x[1].split('/')[2],x[2],x[3],x[4],x[5],x[6],x[8]));
	staten = staten.map(lambda x: (x[7]));

	#count
	count_overall = overall.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect();
	count_bronx = bronx.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect();
        count_brooklyn = brooklyn.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect();
        count_manhattan = manhattan.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect();
        count_queens = queens.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect();
        count_staten = staten.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect();
	count_precinct=precinct.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).sortByKey().collect()
	#parallelize count and format
	count_overall=sc.parallelize(count_overall);
	count_overall = count_overall.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	count_bronx=sc.parallelize(count_bronx);
	count_bronx = count_bronx.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	count_brooklyn=sc.parallelize(count_brooklyn);
	count_brooklyn = count_brooklyn.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	count_manhattan=sc.parallelize(count_manhattan);
	count_manhattan = count_manhattan.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	count_queens=sc.parallelize(count_queens);
	count_queens = count_queens.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	count_staten=sc.parallelize(count_staten);	
	count_staten = count_staten.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	count_precinct=sc.parallelize(count_precinct);
	count_precinct=count_precinct.map(lambda x: (str(x[0]).replace("'","").replace('(','').replace(')',''),x[1]));
	
	#save the file
	count_overall.saveAsTextFile("overall-crime.out");
	count_bronx.saveAsTextFile("bronx-crime.out");
	count_brooklyn.saveAsTextFile("brooklyn-crime.out");
	count_manhattan.saveAsTextFile("manhattan-crime.out");
	count_queens.saveAsTextFile("queens-crime.out");
	count_staten.saveAsTextFile("staten-crime.out");
	#count_precinct.saveAsTextFile("precinct-crime.out");
	#csv
	#final_overall=count_overall.map(row2csv);
        final_overall=count_overall.map(toCSVLine);
	final_bronx=count_bronx.map(toCSVLine);
	final_brooklyn=count_brooklyn.map(toCSVLine);
	final_manhattan=count_manhattan.map(toCSVLine);
	final_queens=count_queens.map(toCSVLine);
	final_staten=count_staten.map(toCSVLine);
	final_precinct=count_precinct.map(toCSVLine);
	#output
	final_overall.saveAsTextFile("overall-crime.csv");
	final_bronx.saveAsTextFile("bronx-crime.csv");
	final_brooklyn.saveAsTextFile("brooklyn-crime.csv");
	final_manhattan.saveAsTextFile("manhattan-crime.csv");
	final_queens.saveAsTextFile("queens-crime.csv");
	final_staten.saveAsTextFile("staten-crime.csv");
	final_precinct.saveAsTextFile("precinct-crime.csv");




