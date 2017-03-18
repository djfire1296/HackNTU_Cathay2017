
# coding: utf-8

# <style>
#     .ul{
#         line-height: 250%;
#     }
# </style>
# 
# <font size="4">
#     <ul>
#         <li> calculate age by birth_time </li>
#         <li> check is any people move contact_loc </li>
#     </ul>
# </font>
# 

# In[1]:

# basic libraries
import os
import math

import numpy as np
import pandas as pd
import matplotlib.pylab as plt


# In[2]:

# pyspark related
from pyspark import SparkContext
from pyspark import SparkConf


# In[3]:

sc = SparkContext('local[*]', 'PySpark')


# In[4]:

# spark sql related
from pyspark.sql import DataFrameWriter
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sql = sqlContext.sql


# In[5]:

sc.master


# In[ ]:




# <h1> Read Profile </h1>

# In[6]:

profile_path = os.path.abspath('../dataset/profile/**/*.csv')


# In[7]:

profile_txtRDD = sc.textFile(profile_path)
profile_txtRDD.take(5)


# In[8]:

profile_txtRDD.count()


# In[9]:

profiletxtnum = _
print profiletxtnum


# In[10]:

from pyspark.sql.types import *


# In[11]:

# field of profile
profilefields = [
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


# In[14]:

# construct profile's schema
profileschema = StructType(profilefields)


# In[15]:

def process_profile_line(l):
    p = l.split(',')
    # for LongType
    for i in [1, 7, 14]:
        p[i] = int(p[i])
    # for DoubleType
    for i in [8, 9]:
        p[i] = float(p[i])
    return p


# In[16]:

profile_df = profile_txtRDD                 .map(process_profile_line)                 .toDF(schema=profileschema)

profile_df.show(5)


# In[ ]:




# <h1> Construct The Map of Contact Location and Contact Code </h1>

# In[44]:

clRDD = profile_df.select('contact_loc')


# In[45]:

ccRDD = profile_df.select('contact_code')


# In[46]:

clccRDD = profile_df.select('contact_loc', 'contact_code')


# <h2> Calculate Numbers </h2>

# In[29]:

# clnum = clRDD.distinct().count()
clnum = 11
print "total " + str(clnum) + " different contact location"


# In[31]:

# ccnum = ccRDD.distinct().count()
clnum = 24
print "total " + str(ccnum) + " different contact code"


# In[33]:

# clccnum = clccRDD.distinct().count()
clccnum = 25
print "total " + str(clccnum) + " different pair of (location, code)"


# <h2> Breif Survey </h2>

# In[47]:

# calculate total amount of every location
cal_clRDD = clRDD.rdd                 .map(lambda c: (c.contact_loc, 1))                 .reduceByKey(lambda x, y: x+y)                 .sortByKey()


# In[48]:

calcldict = cal_clRDD.collectAsMap()


# In[51]:

print calcldict


# In[ ]:




# <h2> Check whether code is 1-1 to loc </h2>

# In[22]:

check_codeTolocRDD = clccRDD.rdd                         .map(lambda c: (c.contact_code, [c.contact_loc]))                         .reduceByKey(lambda x, y: x+y)                         .filter(lambda (x, y): len(y) > 1)


# In[34]:

# check = check_codeTolocRDD.count()
check = 1

if check == 0:
    print "1-1 checked"
else:
    print "total " + str(check) + " code have at least two location"
    print check_codeTolocRDD.take(5)


# <h2> Construct Map </h2>

# In[37]:

maplocTocodeRDD = clccRDD.rdd                     .map(lambda c: (c.contact_loc, [c.contact_code]))                     .reduceByKey(lambda x, y: x+y)


# In[39]:

maplocTocodedict = maplocTocodeRDD.collectAsMap()


# In[43]:

for x in maplocTocodedict.keys():
    print x
    print maplocTocodedict[x]
    print "total " + str(len(maplocTocodedict[x])) + " different code\n"


# In[42]:

# final check
tol = 0
for x in maplocTocodedict.keys():
    tol += len(maplocTocodedict[x])

if tol == clccnum:
    print "Final check complete!"
else:
    print "Something goes wrong! Check it again"


# In[ ]:




# <h1> Check Migration </h1>

# In[52]:

idclRDD = profile_df.select('customer_id', 'contact_loc')


# In[ ]:



