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


	#bronx
	bronx = data.filter(lambda x : x[13] == 'BRONX' and x[7] == 'ASSUALT');
	bronx = bronx.map(lambda x: (x[21],x[22]));

	#brooklyn
        brooklyn = data.filter(lambda x : x[13] == 'BROOKLYN' and x[7] == 'ASSAULT');
        brooklyn = brooklyn.map(lambda x: (x[21],x[22]));

	#manhattan
        manhattan = data.filter(lambda x : x[13] == 'MANHATTAN' and x[7] == 'LARCENY');
        manhattan = manhattan.map(lambda x: (x[21],x[22]));

	#queens
        queens = data.filter(lambda x : x[13] == 'QUEENS' and x[7] == 'LARCENY');
        queens = queens.map(lambda x: (x[21],x[22]));

	#staten island
        staten = data.filter(lambda x : x[13] == 'STATEN ISLAND' and x[7] == 'ASSAULT');
        staten = staten.map(lambda x: (x[21],x[22]));

	#save the file as csv

	bronx=bronx.map(row2csv);
	bronx.saveAsTextFile("bronx-assault-all.csv");
	brooklyn=brooklyn.map(row2csv);
	brooklyn.saveAsTextFile("brooklyn-assault-all.csv");
	manhattan=manhattan.map(row2csv);
	manhattan.saveAsTextFile("manhattan-larceny-all.csv");
	queens=queens.map(row2csv);
	queens.saveAsTextFile("queens-larceny-all.csv");
	staten=staten.map(row2csv);
	staten.saveAsTextFile("staten-assault-all.csv");

