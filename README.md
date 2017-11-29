# Big-Data-Project

#### Team Memebrs:-

|Number| Name| net-id |
|---|:---| ---:|
|  1 | Suchetha      | ss11436 |
|  2 | Priyanka      | pb1826  |
|  3 | Niranjhana    | nn1024  |


##### Dataset:
NYPD Crime (https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

##### Report:
https://docs.google.com/a/nyu.edu/document/d/1RAdEj67bFZf52L_suPGEBeB-xOCpwWNbw9TzMAI2ZRo/edit?usp=sharing

##### Cleaning:

Instructions to generate cleaned csv:-
1. Login to Dumbo
2. Run command: 	
..*spark-submit datacleaning.py data.csv
..*hadoop fs -getmerged cleaned.csv cleaned.csv

Instructions to column validation:-
1. Login to Dumbo
2. Run command: 	
..*spark-submit col0.py data.csv
..*# x[0],valid(x[0]
..*hadoop fs -getmerged dq_col0.out dq_col0.out 
..*#count of the valids,invalids and nulls
..*hadoop fs -getmerged count_col0.out count_col0.out 

Our Doodle of information: https://docs.google.com/document/d/1zHUg0jzhoAtUqpGHhvAgiTZu9ijA2SmrYLjd5Fas3jo/edit


