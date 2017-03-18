
# coding: utf-8

# # Setup

# In[1]:

# basic python package
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import os
import math

get_ipython().magic(u'matplotlib inline')
import matplotlib.pylab as plt
import numpy as np
import pandas as pd


# In[2]:

# spark related
from pyspark.sql import DataFrameWriter
sql = sqlContext.sql


# In[3]:

# spark context
sc.master


# In[4]:

get_ipython().system(u' ls ../../dataset/profile')


# # Profile

# In[5]:

profile_path = os.path.abspath('../dataset/profile/**/*.csv')


# In[6]:

profile_text = sc.textFile(profile_path)
profile_text.take(5)


# ## Raw data to Spark Dataframe 

# In[7]:

from pyspark.sql.types import *

fields = [
    StructField('customer_id', StringType(), False),
    StructField('birth_time', LongType(), True),
    StructField('gender', StringType(), True),
    StructField('contact_loc', StringType(), True),
    StructField('contact_code', StringType(), True),
    StructField('register_loc', StringType(), True),
    StructField('register_code', StringType(), True),
    StructField('start_time', LongType(), True),
    StructField('aum', DoubleType(), True),
    StructField('net_profit', DoubleType(), True),
    StructField('credit_card_flag', StringType(), True),
    StructField('loan_flag', StringType(), True),
    StructField('deposit_flag', StringType(), True),
    StructField('wealth_flag', StringType(), True),
    StructField('partition_time', LongType(), True)
]

schema = StructType(fields)

def process_profile_line(l):
    p = l.split(',')
    # for LongType
    for i in [1, 7, 14]:
        p[i] = int(p[i])
    # for DoubleType
    for i in [8, 9]:
        p[i] = float(p[i])
    return p

profile_df = profile_text     .map(process_profile_line)     .toDF(schema=schema)
    
profile_df.show(5)


# In[8]:

profile_df.count()


# # Event - CCTXN

# In[9]:

cctxn_text = sc.textFile(os.path.abspath('../dataset/cctxn/**/*.csv'))
cctxn_text.take(2)


# In[10]:

import json

fields = [
    StructField('actor_type', StringType(), False),
    StructField('actor_id', StringType(), True),
    StructField('action_type', StringType(), True),
    StructField('action_time', LongType(), True),
    StructField('object_type', StringType(), True),
    StructField('object_id', StringType(), True),
    StructField('channel_type', StringType(), True),
    StructField('channel_id', StringType(), True),
    StructField('attrs', StringType(), True),
    StructField('theme', StringType(), True),
    StructField('partition_time', LongType(), True)
]

schema = StructType(fields)

def process_profile_line(l):
    p = l.split(',')
    # pre
    pre_fields = p[:8]
    pre_fields[3] = int(pre_fields[3])
    # post
    post_fields = p[-2:]
    post_fields[-1] = int(post_fields[-1])
    # attrs
    attrs = json.loads(','.join(p[8:-2]))
    # concat fields
    fields = pre_fields + [attrs] + post_fields
    return fields

cctxn_df = cctxn_text.map(process_profile_line).toDF(schema=schema)

cctxn_df.show(5)


# ##  Use JSON process "attrs"

# In[11]:

import json

# return to RDD for unstructured data
cctxn_attrs = cctxn_df.select("attrs").rdd.map(lambda r: r.attrs).map(lambda x: json.loads(x))
cctxn_attrs.take(3)


# ## Count the number of each card level 

# In[12]:

cctxn_attrs.filter(lambda x: x['channel']['card_level'] is not None)     .map(lambda x: x['channel']['card_level'])     .map(lambda cl: (cl, 1))     .reduceByKey(lambda x, y: x + y)     .sortBy(lambda x: -x[1])     .collect()


# # Merge data from different theme

# ## ATM 

# In[13]:

atm_df = sc.textFile(os.path.abspath('../dataset/atm/**/*.csv'))     .map(process_profile_line)     .toDF(schema=schema)

atm_df.show(5)


# ## CTI 

# In[14]:

cti_df = sc.textFile(os.path.abspath('../dataset/cti/**/*.csv'))     .map(process_profile_line)     .toDF(schema=schema)

cti_df.show(5)


# ## MyBank 

# In[15]:

mybank_df = sc.textFile(os.path.abspath('../dataset/mybank/**/*.csv'))     .map(process_profile_line)     .toDF(schema=schema)

mybank_df.show(5)


# ## Union df 

# In[16]:

event_df = cctxn_df.union(atm_df).union(cti_df).union(mybank_df)


# In[17]:

event_df.count()


# In[18]:

event_cnt_df = event_df.groupBy('actor_id').pivot('theme', ['cc_txn', 'atm', 'cti', 'mybank']).count()


# In[19]:

event_cnt_df.show(5)


# # Profile with Events

# In[20]:

times = profile_df     .select('partition_time')     .distinct()     .orderBy('partition_time')     .rdd.map(lambda x: x.partition_time)     .collect()

times


# In[21]:

start_profile_df = profile_df.where('partition_time={}'.format(times[0]))
end_profile_df = profile_df.where('partition_time={}'.format(times[1]))


# In[22]:

start_profile_df.show(5)


# In[23]:

start_profile_df.registerTempTable('start_profile')
end_profile_df.registerTempTable('end_profile')
event_cnt_df.registerTempTable('event_cnt')


# In[24]:

sql("""
    select 
        a.*,
        p1.credit_card_flag as start_cc,
        p1.loan_flag as start_loan,
        p1.deposit_flag as start_deposit,
        p1.wealth_flag as start_wealth,
        p2.credit_card_flag as end_cc,
        p2.loan_flag as end_loan,
        p2.deposit_flag as end_deposit,
        p2.wealth_flag as end_wealth
    from event_cnt a
    join start_profile p1
        on a.actor_id=p1.customer_id
    join end_profile p2
        on a.actor_id=p2.customer_id
    order by actor_id
""").registerTempTable('actor_status')


# In[25]:

sql("""
    select * from actor_status
""").show(5)


# # Play with KMeans

# In[26]:

from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans


# In[27]:

dataset = sql("""
    select 
        actor_id,
        log(cast(cc_txn as double) + 0.0001) as cctxn_score,
        log(cast(atm as double) + 0.0001) as atm_score,
        log(cast(cti as double) + 0.0001) as cti_score,
        log(cast(mybank as double) + 0.0001) as mybank_score
    from actor_status
""")

dataset.show()


# ## Feature engineering + KMeans 

# In[28]:

cluster_num = 2
feature_cols = ['cctxn_score', 'atm_score', 'cti_score', 'mybank_score']

# Combine selected columns to generate vector column
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol='features')

# Standard Scaler
scaler = StandardScaler(inputCol="features", outputCol="scaled_features",
                       withStd=True, withMean=True)

# KMeans
kmeans = KMeans(k=cluster_num, featuresCol="scaled_features", seed=1)

# pipeline
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
pipelineModel = pipeline.fit(dataset)


# In[29]:

kmeans_model = pipelineModel.stages[2]
centers = kmeans_model.clusterCenters()
centers


# In[30]:

predicted_df = pipelineModel.transform(dataset)
predicted_df.select(['actor_id', 'cctxn_score', 'atm_score', 'cti_score', 'mybank_score', 'prediction']).show(10)


# In[31]:

X = predicted_df.rdd.map(lambda r: r.scaled_features.toArray()).collect()
y = predicted_df.rdd.map(lambda r: r.prediction).collect()


# In[32]:

X = np.array(X)
y = np.array(y)

X.shape, y.shape


# In[33]:

from sklearn.manifold import TSNE
import matplotlib.cm as cm

colors = cm.rainbow(np.linspace(0, 1, cluster_num))
tsne = TSNE(perplexity=30, n_components=2, init='pca', n_iter=5000)
points = tsne.fit_transform(X)
plt.figure(figsize=(10, 6))
plt.xlabel('comp 1')
plt.ylabel('comp 2')
for p_c in zip(points,  colors[y]):
    plt.scatter(p_c[0][0], p_c[0][1], color=p_c[1])


# # Dataframe save and load

# In[34]:

predicted_df.write.save("df-result", mode="overwrite", format="parquet")


# In[35]:

from pyspark.sql import DataFrameReader

loaded_df = spark.read.load("df-result")


# In[36]:

loaded_df.show(5)


# # Thanks for your attending HackNTU X Cathay 2017!

# In[ ]:



