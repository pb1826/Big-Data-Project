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
                occur = ['FRONT OF', 'INSIDE', 'REAR OF', 'OUTSIDE', 'OPPOSITE OF']
		if x == '':
			return "NULL"
		elif x in occur:
			return "VALID"
		else:	
                        return "INVALID"
			
	header = lines.first()
	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[15]))
	col16 = lines.map(lambda x: (validity(x)))
        #col16_result = col8.filter(lambda x: (x[15] == "VALID")).map(lambda x: (x[15]))			
	col16.saveAsTextFile("LOC_OF_OCCUR_DESC.out")
        sc.stop()
