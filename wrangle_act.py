#!/usr/bin/env python
# coding: utf-8

# # DATA WRANGLE PROJECT

# ## Gathering Data

# In[2]:


import pandas as pd
import requests
import json
import tweepy
import numpy as np


# Gather csv data

# In[2]:


df_1=pd.read_csv('twitter-archive-enhanced.csv')


# Gather tsv data

# In[3]:


r = requests.get('https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv', auth=('user', 'pass'))


# In[4]:


r.headers['content-type']


# In[6]:


with open('predict.tsv', 'wb') as f:
    f.write(r.content)


# In[3]:


df_2=pd.read_csv('predict.tsv',sep='\t')


# Gather twitter data by API

# In[22]:



auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)


# In[23]:


count = 0
fails_dict = {}


# In[24]:


tweet_ids = df_1.tweet_id.values
len(tweet_ids)


# In[26]:


with open('tweet_json.txt', 'w') as outfile:
    # This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in tweet_ids:
        count += 1
        print(str(count) + ": " + str(tweet_id))
        try:
            tweet = api.get_status(tweet_id, tweet_mode='extended')
            print("Success")
            json.dump(tweet._json, outfile)
            outfile.write('\n')
        except tweepy.TweepError as e:
            print("Fail")
            fails_dict[tweet_id] = e
            pass

print(fails_dict)


# In[4]:


import json
df_list=[]


# In[5]:


with open('tweet_json.txt') as f:
    for line in f:
        data=json.loads(line)
        tweet_id=data["id_str"]
        retweet_count = data['retweet_count']
        favorite_count = data['favorite_count']
        df_list.append({'tweet_id' :tweet_id ,'retweet_count': retweet_count,'favorite_count': favorite_count})


# In[6]:


df_3=pd.DataFrame(df_list)


# ## Assessing Data

# In[10]:


df_1.sample(5)


# In[11]:


df_1.describe()


# In[12]:


df_1.info()


# In[22]:


df_1[df_1.duplicated()]


# In[24]:


df_1.rating_numerator.value_counts()


# In[26]:


df_1.rating_denominator.value_counts()


# In[16]:


df_2.sample(5)


# In[13]:


df_2.describe()


# In[14]:


df_2.info()


# In[23]:


df_2[df_2.duplicated()]


# In[12]:


df_3.sample(5)


# In[13]:


df_3.describe()


# In[20]:


df_3.info()


# In[91]:


df_1_clean.iloc[516].expanded_urls


# In[94]:


df_1_clean.iloc[342].text


# In[119]:


df_1_clean[~df_1_clean.expanded_urls.str.contains('(?:http[s]?://twitter.com/|https://vine.co)',na=False)]


# In[114]:


df_1_clean.iloc[406].text


# In[124]:


df_1_clean[df_1_clean['expanded_urls'].isnull()]


# In[130]:


df_1_urls=df_1_clean[~df_1_clean.expanded_urls.str.contains('(?:http[s]?://twitter.com/|https://vine.co)',na=False)]


# In[136]:


df_1_urls.drop(df_1_urls[df_1_urls['expanded_urls'].isnull()].index,axis=0)


# ### Quality
# 1. Column timestamp of df_1 is not datetime type.
# 2. The Source column of df_1 is extracted directly from html and contains extra html tags.
# 3. Tweet_id column of df_1, df_2 is integer type.
# 4. Rating denominator is extracted from text and contains some wrong datas.
# 5. Rating numerator is extracted from text and contains some wrong datas.
# 6. There are some row has URLs is not from vine, twitter either None showing those tweets are sharing news and are not rating dogs.
# 7. After creating column predict_breed providing final predicted breed algorithm of dog of df_3 dataframe, the text is contains ‘_’ and somes are in lowercase, somes are in title.
# 8. Df_1 contains retweets.
# 
# ### Tidiness
# 1. doggo, floofer, pupper, puppo columns in df_1 should be combined into a single column as this is one variable that identify stage of dog.  
# 2. Information about one type of observational unit (tweets) is spread across three different files/dataframes. So these three dataframes should be merged as they are part of the same observational unit.

# ## Cleaning Data

# Make copies of datasets

# In[8]:


df_1_clean=df_1.copy()
df_2_clean=df_2.copy()
df_3_clean=df_3.copy()


# ### Quality

# 1. Column timestamp of df_1 is not datetime type.

# In[9]:


df_1_clean.timestamp = pd.to_datetime(df_1_clean.timestamp)


# Test

# In[10]:


df_1_clean.head()


# 2. The Source column of df_1 is extracted directly from html and contains extra html tags.  
# Use re.split to get text source from tags
# 

# In[11]:


import re
df_1_clean['source']= df_1_clean['source'].apply(lambda x: re.split('[><]',x)[-3])


# test

# In[10]:


df_1_clean['source'].value_counts()


# 3. Tweet_id column of df_1, df_2 is integer type.

# In[12]:


df_1_clean.tweet_id = df_1.tweet_id.astype(str)
df_2_clean.tweet_id = df_2.tweet_id.astype(str)


# 4. Rating denominator is extracted from text and contains some wrong datas.

# In[22]:


df_1_clean['rating']=df_1_clean['text'].str.extract('(((?:\d+\.)?)\d+/10)',expand = True)
#this code is extracting optional decimal numerator with denominator is 10.


# In[23]:


rating.columns = ['rating_numerator', 'rating_denominator']
df_1_clean['rating_numerator'] = rating['rating_numerator'].astype(float)
df_1_clean['rating_denominator'] = rating['rating_denominator'].astype(float)
#split numerator and denominator from rating by '/'


# In[21]:


df_1_clean.info()
#check for column and type


# In[24]:


df_1_clean.fix_numerator=pd.to_numeric(df_1_clean.fix_numerator, errors='coerce',downcast='signed')
df_1_clean.fix_denominator=pd.to_numeric(df_1_clean.fix_denominator, errors='coerce',downcast='signed')
#Change fix_numerator and fix_denominator extracted to integer type for comparing with raw rating_numerator
#and rating_denominator


# In[25]:


df_1_clean[df_1_clean['rating_denominator'] != df_1_clean['fix_denominator']]
#Find denominators which are not 10


# In[33]:


df_1_clean.iloc[45].text


# In[26]:


df_1_clean.iloc[45].fix_numerator


# I would like to change rating_denominator of rows to 10 as fix_denominator column

# In[27]:


df_1_clean.loc[[313,784,1068,1165,1202,1662,2335],'rating_denominator']=10


# Test

# In[28]:


df_1_clean[df_1_clean['rating_denominator'] != df_1_clean['fix_denominator']]


# 5. Rating numerator is extracted from text and contains some wrong datas.

# In[29]:


df_1_clean[df_1_clean['rating_numerator'] != df_1_clean['fix_numerator']]
# Check for raw rating_numerator differing from fix_numerator extracted


# Change all non_null differ rating_numerator equal to fix_numerator

# In[30]:


df_1_clean.loc[45,'rating_numerator']=13.5


# In[31]:


df_1_clean.loc[45]['rating_numerator']
# Check this decimal value


# In[32]:


df_1_clean.loc[340,'rating_numerator']=9.75


# In[33]:


df_1_clean.loc[313,'rating_numerator']=13


# In[34]:


df_1_clean.loc[695,'rating_numerator']=9.75


# In[35]:


df_1_clean.loc[763,'rating_numerator']=11.27


# In[36]:


df_1_clean.loc[784,'rating_numerator']=14


# In[37]:


df_1_clean.loc[1068,'rating_numerator']=14


# In[38]:


df_1_clean.loc[1165,'rating_numerator']=13


# In[39]:


df_1_clean.loc[1202,'rating_numerator']=11


# In[40]:


df_1_clean.loc[1662,'rating_numerator']=10


# In[41]:


df_1_clean.loc[1689,'rating_numerator']=9.50


# In[42]:


df_1_clean.loc[1712,'rating_numerator']=11.26


# In[43]:


df_1_clean.loc[2335,'rating_numerator']=9


# Test remain

# In[44]:


df_1_clean[df_1_clean['rating_numerator'] != df_1_clean['fix_numerator']]


# 6. There are some row has URLs is not from vine, twitter either None showing those tweets are sharing news and are not rating dogs.  
# As we found in access data part, we will drop these row

# In[45]:


df_1_clean.drop([335,444,754,885],inplace=True)


# We will create a function for make a predict_breed to take the breed of dog with highest confident in 3 algorithm

# In[46]:


# Function for finding the biggest value of 3 values
def bigger(a,b):
    if a>=b:
        return a
    else:
        return b
def biggest(a ,b,c):
    return bigger(bigger(a,b),c)


# In[47]:


# Function for creating the predict_breed
def breed(row):
    if row.p1_dog == True or row.p2_dog == True or row.p3_dog == True:
    # This is to check one of prediction is at least a dog type
        if row.p1_dog == True:
            # If first prediction is dog
            a=row.p1_conf
        else:
            a=0
        if row.p2_dog == True:
            # If second prediction is dog
            b= row.p2_conf
        else:
            b= 0
        if row.p3_dog == True:
            # If third prediction is dog 
            c = row.p3_conf
        else:
            c = 0
        d =biggest(a,b,c)
        # Find the biggest confident of 3 and take the value as it
        if row.p1_conf == d:
            return row.p1
        elif row.p2_conf == d:
            return row.p2
        elif row.p3_dog == True:
            return row.p3
        else:
            return None
    else:
    # All prediction are not dog, we take none
        return None


# In[48]:


df_2_clean.loc[:, 'predict_breed'] = df_2_clean.apply(breed, axis = 1)
# Aplly function


# Check table

# In[49]:


df_2_clean.head()


# 7. After creating column predict_breed providing final predicted breed algorithm of dog of df_3 dataframe, the text is contains ‘_’ and somes are in lowercase, somes are in title.

# In[50]:


df_2_clean['predict_breed']=df_2_clean['predict_breed'].apply(lambda x: x.replace('_',' ') if x else None)
# Apply replace() to replace _ to space in column predict_breed


# In[51]:


df_2_clean['predict_breed']=df_2_clean['predict_breed'].str.title()
# predict_breed contain lowercase and title text at same time. Change them all to title


# Check

# In[52]:


df_2_clean.head()


# 8. Df_1 contains retweets.  
# We have to exclude the rows which don't have None value in retweeted_status_id,
# retweeted_status_user_id, retweeted_status_timestamp column
# 

# In[53]:


df_1_clean['retweeted_status_id']=df_1_clean['retweeted_status_id'].astype(str)
# Change retweet_status_id column to string type to get none value
df_1_clean=df_1_clean[df_1_clean['retweeted_status_id'] == 'nan']


# Check

# In[54]:


df_1_clean.info()


# ### Tidiness

# 1. doggo, floofer, pupper, puppo columns in df_1 should be combined into a single column as this is one variable that identify stage of dog.  

# In[55]:


#Creat a function alike .melt to make a new column
#that gives the stage of dog with prioty in order doggo, puppo, pupper, floofer
def stage_dog(row):
    if row.doggo !='None':
        return row.doggo
    elif row.puppo !='None':
        return row.puppo
    elif row.pupper !='None':
        return row.pupper
    elif row.floofer !='None':
        return row.floofer
    else:
        return 'None'
    


# In[56]:


df_1_clean.loc[:,'stage_of_dog']=df_1_clean.apply(stage_dog,axis=1)
# Apply the function


# Check

# In[60]:


df_1_clean.sample(5)


# Test

# In[102]:


df_1_clean.stage_of_dog.value_counts()
# Count value of stage_of_dog column


# Check for each column

# In[103]:


len(df_1_clean[df_1_clean.puppo != 'None'])


# In[107]:


df_1_clean[df_1_clean.puppo != 'None']


# The value is 25 cause as piority, we consider this dog grew up and become doggo 

# In[104]:


len(df_1_clean[df_1_clean.doggo != 'None'])


# In[105]:


len(df_1_clean[df_1_clean.pupper != 'None'])


# The value is 233 cause as piority, we consider these dogs grew up and become doggo

# In[106]:


len(df_1_clean[df_1_clean.floofer != 'None'])


# 2. Information about one type of observational unit (tweets) is spread across three different files/dataframes. So these three dataframes should be merged as they are part of the same observational unit.

# In[66]:


df_final = pd.merge(df_1_clean,df_2_clean, on ='tweet_id', how = 'left')
# Merge df_1 and df_2 by tweet_id and left side to df_1


# In[67]:


df_final=pd.merge(df_final,df_3_clean, on= 'tweet_id',how='left')
# Merge with the df_3


# In[68]:


df_final=df_final.drop(['retweeted_status_id','retweeted_status_user_id','retweeted_status_timestamp',
                       'fix_numerator','fix_denominator','p1','p1_conf','p1_dog',
                       'p2','p2_conf','p2_dog','p3','p3_conf','p3_dog','rating',
                       'doggo','floofer','pupper','puppo'],axis=1)
# Drop some uneccessary columns


# Check

# In[69]:


df_final.head()


# Creat a column providing the final rating in decimal

# In[70]:


df_final['rating']= df_final.rating_numerator/df_final.rating_denominator
#Divide rating_numerator to rating_denominator 


# Check

# In[71]:


df_final.info()


# Missing Value in favourite_count and retweet_count columns are due to deleted tweet posts. We will delete those rows.

# In[72]:


df_final=df_final[~df_final['favorite_count'].isnull()]
# Delete null value rows


# We will change type of columns _ in reply to status id_, _in reply to user id_ to string type and  _favorite count_, _retweet count_, _img num_ to integer type

# In[73]:


df_final.in_reply_to_status_id = df_final.in_reply_to_status_id.astype('str')
df_final.in_reply_to_user_id = df_final.in_reply_to_user_id.astype('str')
df_final.favorite_count = df_final.favorite_count.astype('int')
df_final.retweet_count = df_final.retweet_count.astype('int')


# In[74]:


df_final.img_num = df_final.img_num.fillna(0).astype('int')
# We can not change None value to integer, so we fill None with 0 and change to integer


# Check

# In[75]:


df_final.info()


# In[76]:


df_final.rating.value_counts()
#Check for outliners value in rating column


# The values of 0.0, 42.0, 18.2, 66.6, 1.7, 3.428571, 177.6 are outliners.

# In[79]:


df_final=df_final[~df_final['rating'].isin([42.000000,1.700000,18.200000,3.428571,177.600000,66.600000,0.000000])]
#Delete these value rows


# Check

# In[80]:


df_final.rating.value_counts()


# The value 3.428571 is still due to we can not have exact decimal value

# In[82]:


24/7
# As we checked the different rating_denominator with fix_denominator above,
# we find the value 24/7 (It must be 24 hours and 7 days) is 3.428751
#with tweet_id is 810984652412424192


# In[85]:


df_final=df_final[df_final['tweet_id'] != '810984652412424192']
#Delete this tweet


# Check

# In[86]:


df_final.rating.value_counts()


# In[87]:


df_final.info()


# In[88]:


df_final.to_csv('twitter_archive_master.csv', index=False)
# Save clean dataframe to file csv


# ## Visualizing Data

# In[1]:


import matplotlib.pyplot as plt
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline')


# In[3]:


df=pd.read_csv('twitter_archive_master.csv')


# In[91]:


df.head()


# In[92]:


df.rating.mean()


# ### Insight 1

# What is the histogram of rating?

# In[4]:


df.rating.plot(kind='hist')
plt.xlabel('Rating')
plt.title('We Rate Dog rating histogram');
#Get histogram of rating


# It is a left-skew distribution

# ### Insight 2

# Is golden retriever has better rating than average?

# In[94]:


df.predict_breed.value_counts()
# Find the most breed dog


# In[95]:


df[df.predict_breed == 'Golden Retriever']['rating'].mean()
# Find the mean of Golden Retriever to make hypothesis


# We are interested in testing if the mean rating of Golden Retriever in dataframe is higher than meaning rating of all by bootstrapping
# $$H_0: \mu_{Golden} - \mu_0 = 0 $$
# $$H_1: \mu_{Golden} - \mu_0 > 0$$

# In[5]:


diff = []
for i in range (10000):
    boot = df.sample(len(df), replace = True)
    mean = boot['rating'].mean()
    mean_gol = boot[boot.predict_breed == 'Golden Retriever']['rating'].mean()
    diff.append(mean_gol - mean)
# Bootstrapping 10.000 samples


# In[6]:


means=np.array(diff)


# In[7]:


plt.hist(means)
plt.axvline(x=0, color = 'red')
plt.title('Difference of means confident interval')
plt.xlabel('Difference of rating')
plt.ylabel('Sample');
# make a confident interval


# In[8]:


p_val= (diff > np.array(0)).mean()
p_val
# p-value of alternative hypothesis


# ### Insight 3

# In[100]:


import statsmodels.api as sm


# In[101]:


df['intercept'] =1

lm=sm.OLS(df['favorite_count'],df[['rating','intercept']])
results=lm.fit()
results.summary()
# Make a linear regression of rating and favorite_count


# In[ ]:





# In[ ]:




