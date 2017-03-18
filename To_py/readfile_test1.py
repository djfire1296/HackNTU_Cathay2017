
# coding: utf-8

# <h1> Set Up </h1>

# In[1]:

import os
import math

import numpy as np
import pandas as pd
import matplotlib.pylab as plt


# In[2]:

from pyspark import SparkContext
from pyspark import SparkConf


# In[3]:

sc = SparkContext('local[*]', 'PySpark')


# In[4]:

# spark related
from pyspark.sql import DataFrameWriter
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sql = sqlContext.sql


# In[5]:

sc.master


# <h1> Read Files </h1>

# In[7]:

profile_path = os.path.abspath('../dataset/profile/**/*.csv')


# In[8]:

profile_txtRDD = sc.textFile(profile_path)
profile_txtRDD.take(5)


# In[9]:

profile_txtRDD.count()


# In[10]:

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

profile_df = profile_txtRDD     .map(process_profile_line)     .toDF(schema=schema)
    
profile_df.show(5)


# In[11]:

profile_df.count()


# In[12]:

type(profile_df)


# In[13]:

contact_locnum = profile_df.select('contact_loc').rdd.distinct().count()
contact_loclist = profile_df.select('contact_loc').rdd.distinct().collect()
print contact_locnum


# In[14]:

contact_loclist


# In[15]:

clRDD = profile_df.select('contact_loc').rdd.map(lambda loc: (loc.contact_loc, 1)).reduceByKey(lambda x, y: x+y).sortByKey()


# In[16]:

cllist = clRDD.collect()


# In[17]:

cllist


# In[18]:

clRDDnum = clRDD.aggregate(0, lambda x, y: x+y[1], lambda x, y: x+y) == 100


# In[19]:

type(clRDDnum)


# In[20]:

clRDDnum


# In[21]:

clidRDD = profile_df.select('contact_loc', 'customer_id').rdd        .map(lambda df: (df.contact_loc, [df.customer_id])).reduceByKey(lambda x, y: x+y)


# In[22]:

clidRDD.count()


# In[23]:

clidRDD.first()


# In[24]:

clidlist = clidRDD.collect()


# In[25]:

for i in xrange(len(clidlist)):
    print clidlist[i][0]
    print set(clidlist[i][1])


# In[26]:

tol = 0
for i in xrange(len(clidlist)):
    tol += len(list(set(clidlist[i][1])))

print tol


# In[71]:

clccRDD = profile_df.select('contact_loc', 'contact_code').rdd.map(lambda df: (df.contact_loc, [df.contact_code]))                    .reduceByKey(lambda x, y: x+y).map(lambda (x, y): (x, list(set(y))))


# In[72]:

clccRDD.first()


# In[73]:

clccRDD.count()


# In[81]:

clccRDD.aggregate(0, lambda x, y: x + len(y[1]), lambda x, y: x+y)


# In[77]:

ccRDD = profile_df.select('contact_code').distinct().collect()


# In[78]:

ccRDD


# In[79]:

len(ccRDD)


# In[80]:

clccRDD.collect()


# In[83]:

clccfRDD = profile_df.select('contact_loc', 'contact_code').rdd.map(lambda df: (df.contact_code, [df.contact_loc]))                    .reduceByKey(lambda x, y: x+y).map(lambda (x, y): (x, list(set(y))))                    .filter(lambda (x, y): len(y) > 1)


# In[84]:

clccfRDD.count()


# In[85]:

clccfRDD.first()


# In[ ]:




# In[ ]:




# <h1> ATM </h1>

# In[27]:

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

atmschema = StructType(fields)


# In[28]:

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


# In[29]:

atm_df = sc.textFile(os.path.abspath('../dataset/atm/**/*.csv'))     .map(process_profile_line)     .toDF(schema=atmschema)

atm_df.show(5)


# In[30]:

import json

# return to RDD for unstructured data
atm_attrs = atm_df.select("attrs").rdd.map(lambda r: r.attrs).map(lambda x: json.loads(x))
atm_attrs.take(10)


# In[31]:

type(atm_attrs)


# In[32]:

atm_Transtyp = atm_attrs.map(lambda atm: atm['action']['trans_type']).distinct()


# In[33]:

atm_Transtyp.count()


# In[35]:

# atm action: trans_type
atm_Transtyp.take(5)


# In[56]:

atm_checkoneone = atm_attrs                    .map(lambda atm: (atm['channel']['address_zipcode'], [atm['object']['target_acct_nbr']]))                    .reduceByKey(lambda x, y: x+y)                    .map(lambda (x, y): (x, list(set(y))))                    .filter(lambda (x, y): len(y) > 1)


# In[57]:

atm_checkoneone.count()


# In[38]:

atm_df.count()


# In[62]:

atm_checkoneone.take(10)


# In[60]:

# check the data validation which every atm just located in one place
atm_checkoneonea = atm_attrs                    .map(lambda atm: (atm['object']['target_acct_nbr'], [atm['channel']['address_zipcode']]))                    .reduceByKey(lambda x, y: x+y)                    .map(lambda (x, y): (x, list(set(y))))                    .filter(lambda (x, y): len(y) > 1)


# In[61]:

atm_checkoneonea.count()


# In[86]:

atm_loc = atm_attrs.map(lambda atm: atm['channel']['address_zipcode']).distinct()


# In[87]:

atm_loc.count()


# In[88]:

atm_loc.take(20)


# In[89]:

atm_loc.filter(lambda loc: loc == 437583).count()


# In[90]:

re_atm_loc = atm_attrs.map(lambda atm: (atm['channel']['address_zipcode'], 1)).reduceByKey(lambda x, y: x+y)


# In[91]:

re_atm_loc.take(10)


# In[92]:

re_atm_loc.aggregate(0, lambda x, y: x+y[1], lambda x, y: x+y)


# In[95]:

re_atm_loc.filter(lambda (c, n): c == 639201).take(2)


# In[ ]:



