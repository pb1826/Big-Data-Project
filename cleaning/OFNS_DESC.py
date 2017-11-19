from __future__ import print_function

import sys
import string
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))		
			
	def validity(x):
		x = x.upper()
		if x == '':
			return "NULL"
		elif (len(x) > 3 and type(x) == str):
			return "VALID"
		else:	
			return "INVALID"
			
	header = lines.first()
	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[7]))
	col8 = lines.map(lambda x: (validity(x)))					
	#col8_result = col8.filter(lambda x: (x[7] == "VALID")).map(lambda x: (x[7]))
        col8.saveAsTextFile("OFNS_DESC.out")
        sc.stop()
