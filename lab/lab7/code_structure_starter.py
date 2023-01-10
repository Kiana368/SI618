#!/usr/bin/python

'''
Written by hkarthik@umich.edu

'''

from pyspark import SparkContext
from pyspark.sql import SQLContext



if __name__ == "__main__":

    sc = SparkContext(appName="umsi618f22lab7")
    sqlc = SQLContext(sc)


    # load data using sqlc

    q1 = sqlc.sql('''
    # your query here
    ''')

    q1.coalesce(1).write.csv() # fill the csv function with the appropriate filename and parameters


    q2 = sqlc.sql('''
    # your query here
    ''')

    q2.coalesce(1).write.csv() # fill the csv function with the appropriate filename and parameters

   

    q3 = sqlc.sql('''
    # your query here
    ''')

    q3.coalesce(1).write.csv() # fill the csv function with the appropriate filename and parameters