# DataCleaning

#### Dataset:
NYPD Crime (https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

Download and save the dataset on to your local machine. Save as "Data.csv"


#### Cleaning:

##### General Instructions :

1. Login to Dumbo
2. Setup the following alias :
	* alias hfs='/usr/bin/hadoop fs '
	* export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib
	* export HSJ=hadoop-mapreduce/hadoop-streaming.jar
	* alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'
3. Upload the dataset to Dumbo
	* On MacOS, open Treminal and run :
		scp data.csv your_netid@dumbo.es.its.nyu.edu:/home/your_netid/
	* On Windows, run cmd.exe and run :
		pscp data.csv your_netid@dumbo.es.its.nyu.edu:/home/your_netid/
	* The file data.csv is already available at  /home/nn1024/
4. Upload the file to hadoop from your local using the command:
	* hadoop fs -copyFromLocal data.csv
	
##### Instructions to generate cleaned csv for entire dataset :-
5. Run command: 	
  * spark-submit datacleaning.py data.csv
  * hadoop fs -getmerged cleaned.csv cleaned.csv

##### Instructions to specific column validation:-
5. Run command: 	
  * spark-submit col0.py data.csv #colNumber_of_feature
   
  * hadoop fs -getmerged dq_col0.out dq_col0.out #x[0],valid(x[0]
    
  * hadoop fs -getmerged count_col0.out count_col0.out #count of the valids,invalids and nulls



