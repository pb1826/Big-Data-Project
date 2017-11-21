import sys
import re
import numpy as np
from pyspark import SparkContext
from csv import reader
from datetime import datetime
#check null
def check_null(x):
	try:
		if x=='':
			return True;
	except ValueError(x):
		return False;
# check type
def check_type(x):
	try:
		switcher={
			int: "INT",
			float: "FLOAT",
			long: "LONG",
			str: "STRING",
			#datetime.date:"DATE",
			#datetime.time: "TIME",
		}
		return switcher.get(type(x),"NULL")
	except ValueError:
		return "INVALID";
				

#x[6],x[8] key code, pd code
def check_id(x):
	#check if the field is null or 3 digits long
	try:
		if(check_null(x)):
			return "NULL";

		elif(re.match('[1-9][0-9][0-9]$',x)):
			return "VALID",check_type(x);
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";
#x[1] FROM DATE, [3] TO DATE,x[5] RPT DATE
def check_date(x):
	# check if the field is null or if it is between the years 2006-2016
	try:
		if(check_null(x)):
			return "NULL";

		elif(re.match('(0?[1-9]|1[0-2])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-6])$',x)):
			return "VALID","DATE";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID"; 
#time x[4],x[2]
def check_time(x):
	#check if the field is null or if it is in 24 hr format
	try:
		if(check_null(x)):
			return "NULL";
		elif(re.match('(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$',x)):
			return "VALID","TIME";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";	
#x[7]OFNS DSC,x[9] PD DESC,x[12] JURIS_DESC,x[16] Prem type,x[17] Parks NM,x[18] HAD
def check_desc(x):
	#check if the field is null or is a string
	try:
		if(check_null(x)):
			return "NULL";
		elif(type(x)==str):
			return "VALID","STRING";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";
def check_boro(x):
	#check if the field is null or in the list of new york boroughs
	try:
		if(check_null(x)):
			return "NULL";
		elif(x in ['MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND']):
			return "VALID","STRING";
		else:
			return "INVALID";
	
	except ValueError:
		return "INVALID";
#CRM_ATPT_CPTD_TD x[10]
def check_action(x):
	try:
		if(check_null(x)):
			return "NULL";
		elif(x in ['COMPLETED','ATTEMPTED']):
			return "VALID","STRING";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";


def check_xloc(x):
	#check if item is null or if it is a coordinate
	#X-coordinate (East-West): minimum: 909900; maximum: 1067600
	try:
		x=int(x);
		if(check_null(x)):
			return "NULL";
		elif((909900<x) & (x<1067600)):
			return "VALID","COORDINATES";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";
def check_yloc(x):
	#check if the item is null or if it is a coordinate
	#for nyc Y-coordinate (North-South): minimum: 117500; maximum: 275000
	try:
		x=int(x);
		if(check_null(x)):
			return "NULL";
		elif((117500<x) & (x<275000)):
			return "VALID","COORDINATES";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";

def check_lon(x):
	#checking if the field is null or if it is a lon between -73 and -75
	try:
		x=float(x);
		if(check_null(x)):
			return "NULL";
		elif((-75<x) & (x<-73)):
			return "VALID","COORDINATE";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID"; 
def check_lat(x):
	#checking if the field is null or if it is a lat in between 40 and 41
	try:
		x=float(x);
		if(check_null(x)):
			return "NULL";
		elif((40<x) & (x<41)):
			return "VALID","COORDINATE";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";
def lat_lon(x):
	try:
		x=x.replace('(','').replace(')','');
		lat,lon=x.split(',');
		a=check_lat(lat);
		b=check_lon(lon);
		if(a[0]=="VALID" and b[0]=="VALID"):
			return "VALID","STRING";
		else:
			return "INVALID";
	except ValueError:
		return "INVALID";
#Check pricint code. nyc precint code lies between 1 tp 123
def check_addrpctcd(x):
       try:
               	if (check_null(x)):
                       return "NULL";
		elif (int(x)>=1 and int(x)<=123):
                       return "VALID","INT";
 		else:
                       return "INVALID";
       except ValueError:
               return "INVALID";
#checks law code which have only 3 values Misdemeanor, violation and felony
def check_lawcd(x):
	try:
		law = ['MISDEMEANOR', 'VIOLATION', 'FELONY'];
		if(check_null(x)):
			return "NULL";
		elif x in law:
			return "VALID","STRING";
		else:	
                        return "INVALID";
	except ValueError:
		return "INVALID";
#check location of occurence
def check_loc(x):
	try:
		occur = ['FRONT OF', 'INSIDE', 'REAR OF', 'OUTSIDE', 'OPPOSITE OF']
		if(check_null(x)):
			return "NULL";
		elif x in occur:
			return "VALID", "STRING";
		else:	
                        return "INVALID";
	except ValueError:
		return "INVALID";

def toCSV(data):
		return ','.join(str(d) for d in data);

if __name__ == "__main__":
	sc=SparkContext();
	data = sc.textFile(sys.argv[1], 1);
	#extracting header
	header = data.first();
	#removing the header from the data
	data = data.filter(lambda x : x != header);
	#partitiong the columns
	data = data.mapPartitions(lambda x : reader(x));
	#extracting the columb
	#data=data.map(lambda x : (x[0],x[15],check_loc(x[15])));
	#csv file out
	output=data.map(lambda x:(x[0],\
	x[1],\
	check_date(x[1]),\
	x[2],\
	check_time(x[2]),\
	x[3],\
	x[4],\
	x[5],\
	x[6],\
	x[7],\
	check_desc(x[7]),\
	x[8],\
	check_id(x[8]),\
	x[9],\
	check_desc(x[9]),\
	x[10],\
	check_action(x[10]),\
	x[11],\
	x[12],\
	x[13],\
	check_boro(x[13]),\
	x[14],\
	check_addrpctcd(x[14]),\
	x[15],\
	x[16],\
	x[17],\
	x[18],\
	x[19],\
	x[20],\
	x[21],\
	x[22],\
	x[23],\
	lat_lon(x[23])));
	
	cleaned=output.filter(lambda x: x[2][0]=='VALID' and \
	x[4][0]=='VALID' and \
	x[10][0]=='VALID' and \
	x[12][0]=='VALID' and \
	x[14][0]=='VALID' and \
	x[16][0]=='VALID' and \
	x[20][0]=='VALID' and \
	x[22][0]=='VALID' and \
	x[32][0]=='VALID').map(lambda x: (x[0],\
	x[1],x[3],x[5],x[6],x[7],x[8],x[9],x[11],x[13],x[15],x[17],x[18],x[19],x[21],x[23],\
	x[24],x[25],x[26],x[27],x[28],x[29],x[30],x[31]));
	
	file=cleaned.map(toCSV);
	file.saveAsTextFile("cleaned.csv");

	#cleaned.saveAsTextFile("cl.out");
	#output.saveAsTextFile("cl.out");
	#count Valid
	#count=data.map(lambda x: (x[2],1)).reduceByKey(lambda x,y:x+y).collect();
	#final=sc.parallelize(count);
	#output the data
	#data.saveAsTextFile('dq.out');
	#final.saveAsTextFile('count.out');
