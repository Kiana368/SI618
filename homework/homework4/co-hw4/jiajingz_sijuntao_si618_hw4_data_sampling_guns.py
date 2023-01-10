import pandas as pd
import numpy as np
import datetime
df = pd.read_csv("hw4_guns.csv")
df["month"] =pd.to_datetime(df["created_time"]).dt.month
df_3 = df[df["month"] == 3] # 2969, pick 35 random data points
df_4 = df[df["month"] == 4] # 31, dropped
df_6 = df[df["month"] == 6] # 3000, pick 40 random data points
df_3_35 = df_3.sample(n = 35, random_state = 1)
df_6_40 = df_6.sample(n = 40, random_state = 1)
df_75 = pd.concat([df_3_35, df_6_40])
df_75.to_csv("df_75.csv")
df_75["tweet_text"].to_csv("df_75_tweets.csv", index = False)
df_8 = df_75.sample(n=8, random_state = 1)
df_8["tweet_text"].to_csv("df_8_tweets.csv", index = False)