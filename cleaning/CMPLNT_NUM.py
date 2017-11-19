from __future__ import print_function
import string
from operator import add
from pyspark import SparkContext
from csv import reader
import sys
import re

if __name__ == "__main__":
     sc = SparkContext()
     lines = sc.textFile(sys.argv[1], 1)
     lines = lines.mapPartitions(lambda x: reader(x))

     def data_type(num):
         try:
            number = int(num)
            return "INT"
         except ValueError:
            return type(num) 
 
     def validity(x):
         if x == '' or x == ' ' :
              return NULL
         match_str= re.match('[1-9][0-9]+',x)
         if match_str is not None:
              return "VALID"
         else :
              return "INVALID"

     header = lines.first() #extract header
     lines = lines.filter(lambda x : x!= header).map(lambda x: (x[0]))
     col1 = lines.map(lambda x: (x,data_type(x),validity(x)))
    # col1_result = col1.filter(lambda x: x[0] == "VALID").map(lambda x: (x[0]))
     col1.saveAsTextFile("CMPLNT_NUM.out")
     sc.stop()
