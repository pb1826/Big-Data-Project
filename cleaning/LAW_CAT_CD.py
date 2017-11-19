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
                law = ['MISDEMEANOR', 'VIOLATION', 'FELONY']
		if x == '':
			return "NULL"
		elif x in law:
			return "VALID"
		else:	
                        return "INVALID"
			
	header = lines.first()
	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[11]))
	col12 = lines.map(lambda x: (validity(x)))
        #col12_result = col8.filter(lambda x: (x[11] == "VALID")).map(lambda x: (x[11]))			
	col12.saveAsTextFile("LAW_CAT_CD.out")
        sc.stop()
