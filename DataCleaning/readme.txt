Report For Cleaning:-
https://docs.google.com/a/nyu.edu/document/d/1RAdEj67bFZf52L_suPGEBeB-xOCpwWNbw9TzMAI2ZRo/edit?usp=sharing

Instructions to generate cleaned csv:-
1. Login to Dumbo
2. Run command: spark-submit datacleaning.py data.csv
				hadoop fs -getmerged cleaned.csv cleaned.csv

Instructions to column validation:-
1. Login to Dumbo
2. Run command: spark-submit col0.py data.csv
				hadoop fs -getmerged dq_col0.out dq_col0.out # 	x[0],valid(x[0]
				hadoop fs -getmerged count_col0.out count_col0.out #count of the valids,invalids and nulls
