#!/usr/bin/python

'''
Created by hkarthik@umich.edu

'''

import json
from pyspark import SparkContext



def get_author_category_and_ratings(data):
    
    author_category_and_ratings_list = []

    # your code here
    rating_scores = float(data["review/score"])
    num_ratings = 1
    num_ratings_active = int(data["User_id"] != None)
    num_ratings_inactive = int(data["User_id"] == None)
    
    if data["authors"] == None:
        authors = ["Unknown"]
    else:
        authors = data["authors"].split(', ')
    
    if data["categories"] == None:
        categories = ["Unknown"]
    else:
        categories = data["categories"].split(', ')

    for author in authors:
        for category in categories:
            if((author!='') &(category!='')):
                cur_tuple = ((author, category),(rating_scores, num_ratings, num_ratings_active, num_ratings_inactive))
                author_category_and_ratings_list.append(cur_tuple)

    return author_category_and_ratings_list

    # hint: author_category_and_ratings_list contains tuples with rating scores, number of ratings, and
    #       number of ratings from active and inactive users for each author-category combination



if __name__ == "__main__":


    sc = SparkContext(appName="pyspark_si618f22_avg_rating_score_per_category_per_author")

    input_file = sc.textFile("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw6_data/book_ratings_data_jsonl.json")

    solution =  input_file.map(lambda line: json.loads(line))\
                      .filter(lambda info: (float(info["review/helpfulness"].split('/')[0]) >= 0.25 * float(info["review/helpfulness"].split('/')[1])) & (info["review/summary"]!= None) & (float(info["review/helpfulness"].split('/')[1])!=0))\
                      .flatMap(lambda info: get_author_category_and_ratings(info))\
                      .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3]))\
                      .map(lambda tup: (tup[0], (round(tup[1][0]/tup[1][1],2), tup[1][1], tup[1][2], tup[1][3])))\
                      .sortBy(lambda tup: (tup[0][0], -tup[1][1], tup[0][1]), ascending = True)\
                      .map(lambda tup: '\t'.join(tup[0])+'\t'+str(tup[1][0])+'\t'+str(tup[1][1])+'\t'+str(tup[1][2])+'\t'+str(tup[1][3]))

    solution.collect()
    solution.saveAsTextFile('sijuntao_si618_hw6_output')
