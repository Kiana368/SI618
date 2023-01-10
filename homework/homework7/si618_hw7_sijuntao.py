#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":

    sc = SparkContext(appName="umsi618f22hw7")
    sqlc = SQLContext(sc)


    # load data using sqlc
    offering = sqlc.read.json("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw7_data/offering_cleaned.txt")
    review = sqlc.read.json("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw7_data/review_cleaned.txt")
    offering.registerTempTable("offering")
    review.registerTempTable("review")


    # Q1
    q1 = sqlc.sql('''
        SELECT offering_id,
        avg_rating,
        hotel_class,
        locality,
        name,
        region_id
        FROM offering JOIN (SELECT offering_id,
                            AVG(ratings_overall) as avg_rating
                            FROM review
                            GROUP BY offering_id)as id_rating ON offering.id = id_rating.offering_id
        WHERE hotel_class IS NOT NULL
        ORDER BY avg_rating DESC, offering_id ASC
        LIMIT 100
        ''')
    q1.coalesce(1).write.option("delimiter","\t").option("header", "true").csv("sijuntao_si618_hw7_output_1") 

    
    # Q2
    q2 = sqlc.sql('''
        SELECT locality,
        COUNT(hotel_class) as cnt,
        AVG(ratings_overall) as avg_rating
        FROM offering JOIN review ON offering.id = review.offering_id
        WHERE (hotel_class IS NOT NULL) AND (hotel_class >= 4.0)
        GROUP BY locality
        ORDER BY cnt DESC
        ''')
    q2.coalesce(1).write.option("delimiter","\t").option("header", "true").csv("sijuntao_si618_hw7_output_2")

   
    # Q3
    q3 = sqlc.sql('''
        SELECT first.locality,
        hotel_class,
        avg_rating
        FROM (SELECT locality,
            hotel_class,
            AVG(ratings_overall) as avg_rating
            FROM offering JOIN review ON offering.id = review.offering_id
            WHERE (hotel_class IS NOT NULL) AND (num_helpful_votes >= 2) AND (author_num_helpful_votes > 10)
            GROUP BY locality, hotel_class) as first JOIN
            (SELECT locality,
            AVG(ratings_overall) as avg_rating_locality
            FROM offering JOIN review ON offering.id = review.offering_id
            WHERE (hotel_class IS NOT NULL) AND (num_helpful_votes >= 2) AND (author_num_helpful_votes > 10)
            GROUP BY locality) as second ON first.locality = second.locality
        ORDER BY avg_rating_locality DESC, hotel_class DESC
        ''')
    q3.coalesce(1).write.option("delimiter","\t").option("header", "true").csv("sijuntao_si618_hw7_output_3")
