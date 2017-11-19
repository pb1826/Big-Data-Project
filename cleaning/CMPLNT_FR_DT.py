from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
     sc = SparkContext()
     lines = sc.textFile(sys.argv[1], 1)
     lines = lines.mapPartitions(lambda x: reader(x))
     
     def validity(x):
         if x == '':
                   return "NULL"
         try :
             if x != datetime.strptime(x, "%m/%d/%Y").strftime('%m/%d/%Y'):
                                      raise ValueError
             match_date=re.match('(0?[1-9]|1[0-2])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-6])$', x) #crime reported 2006-2016
             if match_date is not None:
                                      return "VALID"
             else :
                                      return "INVALID"
         except ValueError:
                   return "INVALID"

     header = lines.first() #extract_header
     lines = lines.filter( lambda x: x!=header).map(lambda x: (x[1]))
     col2 = lines.map(lambda x: (x,validity(x)))
     #col2_result = col2.filter( lambda x: x[1] == "VALID").map(lambda x: (x[1]))
     col2.saveAsTextFile("CMPLNT_FR_DT.out")
     sc.stop()
