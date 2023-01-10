#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime


# In[2]:


gun_re = pd.read_csv("jiajingz_sijuntao_si618_hw4_batch_result_guns.csv")
gun_ori = pd.read_csv("hw4_guns.csv")
result_string = []


# In[3]:


gun_re = gun_re.groupby("Input.tweet_text").sum()
n = len(gun_re)


# In[4]:


# 1
supports_gun_control1 = len(gun_re[gun_re["Answer.guns.supports-gun-control"] >=1 ])/n
supports_gun_rights1 = len(gun_re[gun_re["Answer.guns1.supports-gun-rights"] >=1 ])/n
neutral_guns1 = len(gun_re[gun_re["Answer.guns2.neutral/unclear"] >=1 ])/n
result_string.append( f"supports-gun-control1\t{supports_gun_control1}\tsupports-gun-rights1\t{supports_gun_rights1}\tneutral-guns1\t{neutral_guns1}\t")


# In[5]:


supports_gun_control2 = len(gun_re[gun_re["Answer.guns.supports-gun-control"] >=2 ])/n
supports_gun_rights2 = len(gun_re[gun_re["Answer.guns1.supports-gun-rights"] >=2 ])/n
neutral_guns2 = len(gun_re[gun_re["Answer.guns2.neutral/unclear"] >=2 ])/n
result_string.append( f"supports-gun-control2\t{supports_gun_control2}\tsupports-gun-rights2\t{supports_gun_rights2}\tneutral-guns2\t{neutral_guns2}\t")


# In[6]:


supports_gun_control3 = len(gun_re[gun_re["Answer.guns.supports-gun-control"] >=3 ])/n
supports_gun_rights3 = len(gun_re[gun_re["Answer.guns1.supports-gun-rights"] >=3 ])/n
neutral_guns3 = len(gun_re[gun_re["Answer.guns2.neutral/unclear"] >=3 ])/n
result_string.append( f"supports-gun-control3\t{supports_gun_control3}\tsupports-gun-rights3\t{supports_gun_rights3}\tneutral-guns3\t{neutral_guns3}\t\n")


# In[7]:


# 2

gun_re = pd.merge(gun_re, gun_ori, left_on = "Input.tweet_text", right_on = "tweet_text", how = "inner" )
gun_re["month"] = pd.to_datetime(gun_re["created_time"]).dt.month
gun_re["year"] = pd.to_datetime(gun_re["created_time"]).dt.year


# In[8]:


supports_gun_control_majority = gun_re[gun_re["Answer.guns.supports-gun-control"] >=2 ]
supports_gun_rights_majority = gun_re[gun_re["Answer.guns1.supports-gun-rights"] >=2 ]
neutral_majority = gun_re[gun_re["Answer.guns2.neutral/unclear"] >=2 ]
no_majority = gun_re[(gun_re["Answer.guns.supports-gun-control"] <2) & (gun_re["Answer.guns1.supports-gun-rights"] <2) & (gun_re["Answer.guns2.neutral/unclear"] <2)]

for month in gun_re.sort_values("month")["month"].unique():
    result_string.append(str(month) + " " + str(gun_re["year"].unique()[0]))
    supports_gun_control_majority_ratio = len(supports_gun_control_majority[supports_gun_control_majority["month"] == month])/len(gun_re[gun_re["month"] == month])
    supports_gun_rights_majority_ratio = len(supports_gun_rights_majority[supports_gun_rights_majority["month"] == month])/len(gun_re[gun_re["month"] == month])
    neutral_majority_ratio = len(neutral_majority[neutral_majority["month"] == month])/len(gun_re[gun_re["month"] == month])
    no_majority_ratio = len(no_majority[no_majority["month"] == month])/len(gun_re[gun_re["month"] == month])
    result_string.append(f"supports-gun-control-majority\t{supports_gun_control_majority_ratio}\tsupports-gun-rights-majority \t{supports_gun_rights_majority_ratio}\tneutral-majority\t{neutral_majority_ratio}\tno_majority\t{no_majority_ratio}")


# In[9]:


# 3

targeted_supports_gun_control_majority = len(supports_gun_control_majority[supports_gun_control_majority["Answer.targeted.yes"]>=1])/len(supports_gun_control_majority)
targeted_supports_gun_rights_majority = len(supports_gun_rights_majority[supports_gun_rights_majority["Answer.targeted.yes"]>=1])/len(supports_gun_rights_majority)
targeted_neutral_majority = len(neutral_majority[neutral_majority["Answer.targeted.yes"]>=1])/len(neutral_majority)

result_string.append(f"\ntargeted_supports-gun-control-majority\t{targeted_supports_gun_control_majority}\ttargeted_supports-gun-rights-majority\t{targeted_supports_gun_rights_majority}\ttargeted_neutral-majority\t{targeted_neutral_majority}")


# In[10]:


f = open('jiajingz_sijuntao_si618_hw4_computations_guns.txt','w')
for line in result_string:
    print(line, file=f)
f.close()


# In[ ]:




