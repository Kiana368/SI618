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
    lol_dt = sqlc.read.csv("/scratch/siads618f22_class_root/siads618f22_class/shared_data/lab7_data/euw_ranked_team.csv", header=True)

    # Q1
    q1 = sqlc.sql('''
    SELECT CAST(lol_dt.summonerId as int) as summonerId_int,
    matchId,
    (2 * CAST(kill as int) + CAST(assist as int) - 3 * CAST(death as int)) as DF,
    num_match
    FROM lol_dt JOIN (SELECT summonerId,
                    COUNT(*) as num_match
                    FROM lol_dt
                    GROUP BY summonerId) as match_count ON match_count.summonerId = lol_dt.summonerId
    WHERE num_match >= 10
    ORDER BY DF DESC, num_match DESC, summonerId_int ASC, matchId ASC
    ''')

    q1.registerTempTable('q1Summary')
    # q1.show()
    q1.coalesce(1).write.option("delimiter","\t").csv("sijuntao_si618_lab7_output_1") # fill the csv function with the appropriate filename and parameters
   

    # Q2
    q2 = sqlc.sql('''
    SELECT CAST(summonerId as int) as summonerId_int,
    CEIL(AVG(DF/team_DF_no0)) as avg_nor_DF,
    COUNT(*) as num_match
    FROM (SELECT summonerId,
            (2 * CAST(kill as int) + CAST(assist as int) - 3 * CAST(death as int)) as DF,
            (cast((team_DF == 0) as int) + team_DF) as team_DF_no0
            FROM (lol_dt JOIN (SELECT matchId,
                            winner,
                            ABS(SUM(2 * CAST(kill as int) + CAST(assist as int) - 3 * CAST(death as int))) as team_DF
                            FROM lol_dt
                            GROUP BY matchId, winner) as team_df ON team_df.matchId = lol_dt.matchId AND team_df.winner = lol_dt.winner))
    GROUP BY summonerId_int
    HAVING num_match >= 10
    ORDER BY avg_nor_DF DESC, num_match DESC, summonerId_int ASC
    ''')
    
    q2.registerTempTable('q2Summary')
    # q2.show()
    q2.coalesce(1).write.option("delimiter","\t").csv("sijuntao_si618_lab7_output_2") # fill the csv function with the appropriate filename and parameters

   
    # Q3
    pair = sqlc.sql('''
        SELECT a.matchId,
        a.role,
        a.summonerId as s1,
        b.summonerId as s2,
        a.DF as DF1,
        b.DF as DF2,
        ABS(a.DF - b.DF) AS diff_DF
        FROM (SELECT * FROM (SELECT CAST(summonerId as int) as summonerId,
            matchId as matchId,
            CAST(predictedRole as int) as role, 
            (2 * CAST(kill as int) + CAST(assist as int) - 3 * CAST(death as int)) as DF,
            winner
            FROM lol_dt)
            WHERE winner = 1) as a JOIN 
            (SELECT * FROM (SELECT CAST(summonerId as int) as summonerId,
            matchId as matchId,
            CAST(predictedRole as int) as role, 
            (2 * CAST(kill as int) + CAST(assist as int) - 3 * CAST(death as int)) as DF,
            winner
            FROM lol_dt)
            WHERE winner = 0) as b ON a.matchId = b.matchId AND a.role = b.role 
        ORDER BY a.matchId
    ''')
    pair.registerTempTable('pair')

    q3 = sqlc.sql('''
        SELECT pair.matchId,
        role as common_role, 
        diff_DF,
        (case when s1>s2 then s1 else s2 end) as s1, 
        (case when s1<s2 then s1 else s2 end) as s2
        FROM pair JOIN (SELECT matchId, 
                        MAX(diff_DF) as max_DF 
                        FROM pair 
                        GROUP BY matchId) as temp 
                  ON pair.matchId = temp.matchId AND pair.diff_DF = temp.max_DF 
        ORDER BY diff_DF DESC, common_role ASC, matchId ASC, s1 ASC, s2 ASC
        ''')
    q3.registerTempTable('q3Summary')
    # q3.show()
    q3.coalesce(1).write.option("delimiter","\t").csv("sijuntao_si618_lab7_output_3") # fill the csv function with the appropriate filename and parameters