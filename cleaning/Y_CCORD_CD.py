from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
import time

if __name__ == "__main__":
     sc = SparkContext()
     lines = sc.textFile(sys.argv[1], 1)
     lines = lines.mapPartitions(lambda x: reader(x))
     
     def validity(x):
         if x == '':
                   return "NULL"
         match_y = re.match('\d*', x)
         if match_y is not None:
                   return "VALID"
         else :
                   return "INVALID"

     header = lines.first() #extract_header
     lines = lines.filter(lambda x: x!=header).map(lambda x: (x[20]))
     col21 = lines.map(lambda x: (x,validity(x)))
     #col21_result = col21.filter(lambda x: x[20] == "VALID").map(lambda x: (x[20]))
     col21.saveAsTextFile("Y_COORD_CD.out")
     sc.stop()
