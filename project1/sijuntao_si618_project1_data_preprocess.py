#!/usr/bin/env python
# coding: utf-8

# # SI 618 Project 1 - Data Preprocessing
# - name: Sijun Tao
# - uniquename: sijuntao

import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime


if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("PySparksi618f22").config("spark.some.config.option", "some-value").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    # --- Data Pre-processing --- #

    # read in data
    expenditure_raw = sqlContext.read.csv("original_data/states.csv", header=True)
    edu_level_raw = sqlContext.read.csv("original_data/education.csv", header=True)
    unemployment_raw = sqlContext.read.csv("original_data/unemployment.csv", header= True)

    
    # select and clean data

    # expenditure
    expenditure = expenditure_raw.select("STATE", "YEAR", "TOTAL_EXPENDITURE", "INSTRUCTION_EXPENDITURE", "SUPPORT_SERVICES_EXPENDITURE")
    expenditure = expenditure.withColumn("YEAR", year(expenditure.YEAR))
    expenditure.registerTempTable("expenditure")

    # education level
    edu = edu_level_raw.select("State", "Area name", "Percent of adults with less than a high school diploma, 1990", "Percent of adults with a high school diploma only, 1990", "Percent of adults completing some college or associate's degree, 1990","Percent of adults with a bachelor's degree or higher, 1990", "Percent of adults with less than a high school diploma, 2000", "Percent of adults with a high school diploma only, 2000", "Percent of adults completing some college or associate's degree, 2000","Percent of adults with a bachelor's degree or higher, 2000", "Percent of adults with less than a high school diploma, 2015-19", "Percent of adults with a high school diploma only, 2015-19", "Percent of adults completing some college or associate's degree, 2015-19","Percent of adults with a bachelor's degree or higher, 2015-19")
    edu = edu.na.drop(subset=["Percent of adults with less than a high school diploma, 1990", "Percent of adults with a high school diploma only, 1990", "Percent of adults completing some college or associate's degree, 1990","Percent of adults with a bachelor's degree or higher, 1990", "Percent of adults with less than a high school diploma, 2000", "Percent of adults with a high school diploma only, 2000", "Percent of adults completing some college or associate's degree, 2000","Percent of adults with a bachelor's degree or higher, 2000", "Percent of adults with less than a high school diploma, 2015-19", "Percent of adults with a high school diploma only, 2015-19", "Percent of adults completing some college or associate's degree, 2015-19","Percent of adults with a bachelor's degree or higher, 2015-19"])
    edu = edu.withColumnRenamed("Area name", "area_name")
    edu = edu.withColumnRenamed("Percent of adults with less than a high school diploma, 1990", "l_h_1990")
    edu = edu.withColumnRenamed("Percent of adults with a high school diploma only, 1990", "e_h_1990")
    edu = edu.withColumnRenamed("Percent of adults completing some college or associate's degree, 1990", "e_c_1990")
    edu = edu.withColumnRenamed("Percent of adults with a bachelor's degree or higher, 1990", "g_c_1990")
    edu = edu.withColumnRenamed("Percent of adults with less than a high school diploma, 2000", "l_h_2000")
    edu = edu.withColumnRenamed("Percent of adults with a high school diploma only, 2000", "e_h_2000")
    edu = edu.withColumnRenamed("Percent of adults completing some college or associate's degree, 2000", "e_c_2000")
    edu = edu.withColumnRenamed("Percent of adults with a bachelor's degree or higher, 2000", "g_c_2000")
    edu = edu.withColumnRenamed("Percent of adults with less than a high school diploma, 2015-19", "l_h_2015")
    edu = edu.withColumnRenamed("Percent of adults with a high school diploma only, 2015-19", "e_h_2015")
    edu = edu.withColumnRenamed("Percent of adults completing some college or associate's degree, 2015-19", "e_c_2015")
    edu = edu.withColumnRenamed("Percent of adults with a bachelor's degree or higher, 2015-19", "g_c_2015")
    edu.registerTempTable("edu")
    edu_years = ["1990", "2000", "2015"]
    for edu_year in edu_years:
        edu_temp = sqlContext.sql("SELECT State as state, area_name, '"+ edu_year + "' as year, l_h_" + edu_year + " as less_high_school, e_h_" + edu_year + " as equal_high_school, e_c_" + edu_year + " as equal_college, g_c_" + edu_year + " as greater_college FROM edu")
        edu_temp.registerTempTable("edu_temp"+edu_year)

    edu_temp = sqlContext.sql("SELECT state, area_name, year, less_high_school, equal_high_school, equal_college, greater_college FROM edu_temp1990 UNION ALL SELECT state, area_name, year, less_high_school, equal_high_school, equal_college, greater_college FROM edu_temp2000 UNION ALL SELECT state, area_name, year, less_high_school, equal_high_school, equal_college, greater_college FROM edu_temp2015")
    edu_temp.registerTempTable("edu")


    # unemployment rate
    unemp_years = []
    for i in range(2000, 2020):
        unemp_years.append("Civilian_labor_force_"+str(i))
        unemp_years.append("Employed_"+str(i))
        unemp_years.append("Unemployed_"+str(i))
        unemp_years.append("Unemployment_rate_"+str(i))

    unemployment = unemployment_raw.na.drop(subset = unemp_years)
    unemployment.registerTempTable("unemp")
    query = "SELECT state, area_name, year, Civilian_labor_force, Employed, Unemployed, Unemployment_rate FROM unemp_temp2000"
    for i in range(2000, 2020):
        unemp_temp = sqlContext.sql("SELECT State as state, Area_name as area_name, "+ str(i) + " as year, Civilian_labor_force_" + str(i) + " as Civilian_labor_force, Employed_" + str(i) + " as Employed, Unemployed_" + str(i) + " as Unemployed, Unemployment_rate_" + str(i) + " as Unemployment_rate FROM unemp")
        unemp_temp.registerTempTable("unemp_temp"+str(i))
        if i != 2000:
            query += " UNION ALL SELECT state, area_name, year, Civilian_labor_force, Employed, Unemployed, Unemployment_rate FROM unemp_temp" + str(i)

    unemp_temp = sqlContext.sql(query)
    unemp_temp.registerTempTable("unemp")


    # USA state and the corresponding abbreviations
    states_abb = sqlContext.read.csv("original_data/states_abb.csv", header=True)
    states_abb.registerTempTable("states_abb")

    
    
    # --- Analysis --- #
    
    # Question 1: Instruction expenditure and education level
    
    q1 = sqlContext.sql(
        '''
        SELECT STATE, (cast(YEAR = 1992 as int)*(-2) + YEAR) as YEAR, cast(INSTRUCTION_EXPENDITURE as int), cast(INSTRUCTION_EXPENDITURE as float)/cast(TOTAL_EXPENDITURE as float) as instruction_expenditure_ratio
        FROM expenditure
        WHERE YEAR = 1992 OR YEAR = 2000 OR YEAR = 2015
        ''')
    q1.registerTempTable('q1_temp')

    q2 = sqlContext.sql(
        '''
        SELECT state, state_full_name, year, AVG(less_high_school) as state_less_high_school, AVG(equal_high_school) as state_equal_high_school, AVG(equal_college) as state_equal_college, AVG(greater_college) as state_greater_college,AVG(equal_college)+AVG(greater_college) as state_high_education_level
        FROM (SELECT state, state_full_name, area_name, year, less_high_school, equal_high_school, equal_college, greater_college
             FROM edu JOIN states_abb ON edu.state = states_abb.abb)
        WHERE year = 1990 OR year = 2000 OR year = 2015
        GROUP BY state, state_full_name, year
        ORDER BY state, year
        ''')
    q2.registerTempTable('q2_temp')

    q3 = sqlContext.sql(
        '''
        SELECT state_full_name as State, q1_temp.YEAR, INSTRUCTION_EXPENDITURE, instruction_expenditure_ratio,state_less_high_school,state_equal_high_school,state_equal_college,state_greater_college,state_high_education_level
        FROM q1_temp JOIN q2_temp ON q1_temp.STATE = q2_temp.state_full_name AND q1_temp.YEAR = q2_temp.year
        ORDER BY state_full_name, q1_temp.YEAR
        ''')
    q3.registerTempTable('q3_temp')

    q3_1 = sqlContext.sql(
        '''
        SELECT State, Year, instruction_expenditure_ratio, state_less_high_school,state_equal_high_school,state_equal_college,state_greater_college
        FROM q3_temp
        WHERE Year = 2000
        ORDER BY instruction_expenditure_ratio
        ''')
    q3_1.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('sijuntao_si618_project_q1-1')

    q3_2 = sqlContext.sql(
        '''
        SELECT State, YEAR, INSTRUCTION_EXPENDITURE, state_equal_college, state_greater_college, state_high_education_level
        FROM q3_temp
        ORDER BY State, YEAR
        ''')
    q3_2.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('sijuntao_si618_project_q1-2')


    # Question 2: Instruction expenditure and unemployment rate

    q4 = sqlContext.sql(
        '''
        SELECT STATE, YEAR, cast(INSTRUCTION_EXPENDITURE as int), cast(INSTRUCTION_EXPENDITURE as float)/cast(TOTAL_EXPENDITURE as float) as instruction_expenditure_ratio
        FROM expenditure
        WHERE YEAR >= 2000 AND YEAR <=2016
        ''')
    q4.registerTempTable('q4_temp')

    q5 = sqlContext.sql(
        '''
        SELECT state, state_full_name, year, SUM(Civilian_labor_force) as state_Civilian_labor_force, SUM(Employed) as state_Employed, SUM(Unemployed) as state_Unemployed, ROUND(SUM(Unemployed)/SUM(Civilian_labor_force), 4) as state_unemployment_ratio, ROUND(SUM(Employed)/SUM(Civilian_labor_force), 4) as state_employment_ratio 
        FROM (SELECT state, state_full_name, area_name, year, REPLACE(Civilian_labor_force, ',', '') as Civilian_labor_force, REPLACE(Employed, ',', '') as Employed, REPLACE(Unemployed, ',', '') as Unemployed, Unemployment_rate
             FROM unemp JOIN states_abb ON unemp.state = states_abb.abb)
        WHERE year >=2000 AND year <=2016
        GROUP BY state, state_full_name, year
        ORDER BY state, year
        ''')
    q5.registerTempTable('q5_temp')

    q6 = sqlContext.sql(
        '''
        SELECT state_full_name as State, q4_temp.YEAR, INSTRUCTION_EXPENDITURE, instruction_expenditure_ratio,state_unemployment_ratio, state_employment_ratio
        FROM q4_temp JOIN q5_temp ON q4_temp.STATE = q5_temp.state_full_name AND q4_temp.YEAR = q5_temp.year
        ORDER BY state_full_name, q4_temp.YEAR
        ''')
    q6.registerTempTable('q6_temp')

    q6_1 = sqlContext.sql(
        '''
        SELECT State, Year, instruction_expenditure_ratio, state_unemployment_ratio
        FROM q6_temp
        ORDER BY instruction_expenditure_ratio
        ''')
    q6_1.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('sijuntao_si618_project_q2-1')

    q6_2 = sqlContext.sql(
        '''
        SELECT State, YEAR, INSTRUCTION_EXPENDITURE, state_unemployment_ratio
        FROM q6_temp
        ORDER BY State, YEAR
        ''')
    q6_2.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('sijuntao_si618_project_q2-2')


    # Question 3: Employment rate and expenditure

    q7 = sqlContext.sql(
        '''
        SELECT State, YEAR, INSTRUCTION_EXPENDITURE,instruction_expenditure_ratio, state_employment_ratio
        FROM q6_temp
        ORDER BY State, YEAR
        ''')
    q7.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('sijuntao_si618_project_q3-1')

    q8 = sqlContext.sql(
        '''
        SELECT STATE, YEAR, cast(SUPPORT_SERVICES_EXPENDITURE as int), cast(SUPPORT_SERVICES_EXPENDITURE as float)/cast(TOTAL_EXPENDITURE as float) as sur_ser_expenditure_ratio
        FROM expenditure
        WHERE YEAR >= 2000 AND YEAR <=2016
        ''')
    q8.registerTempTable("q8_temp")

    q9 = sqlContext.sql(
        '''
        SELECT state_full_name as State, q8_temp.YEAR, SUPPORT_SERVICES_EXPENDITURE, sur_ser_expenditure_ratio, state_employment_ratio
        FROM q8_temp JOIN q5_temp ON q8_temp.STATE = q5_temp.state_full_name AND q8_temp.YEAR = q5_temp.year
        ORDER BY state_full_name, q8_temp.YEAR
        ''')
    q9.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('sijuntao_si618_project_q3-2')





