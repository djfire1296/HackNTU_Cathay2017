
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

# In[50]:

# basic libraries
import os
import math
import time

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

# In[8]:

profile_path = os.path.abspath('../../dataset/profile/**/*.csv')


# In[9]:

profile_txtRDD = sc.textFile(profile_path)
profile_txtRDD.take(5)


# In[10]:

# profile_txtRDD.count()


# In[11]:

profiletxtnum = _
print profiletxtnum


# In[12]:

from pyspark.sql.types import *


# In[13]:

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

# In[17]:

clRDD = profile_df.select('contact_loc')


# In[18]:

ccRDD = profile_df.select('contact_code')


# In[19]:

clccRDD = profile_df.select('contact_loc', 'contact_code')


# <h2> Calculate Numbers </h2>

# In[22]:

# clnum = clRDD.distinct().count()
clnum = 11
print "total " + str(clnum) + " different contact location"


# In[23]:

# ccnum = ccRDD.distinct().count()
ccnum = 24
print "total " + str(ccnum) + " different contact code"


# In[99]:

# clccnum = clccRDD.distinct().count()
clccnum = 25
print "total " + str(clccnum) + " different pair of (location, code)"


# <h2> Breif Survey </h2>

# In[25]:

# calculate total amount of every location
cal_clRDD = clRDD.rdd                 .map(lambda c: (c.contact_loc, 1))                 .reduceByKey(lambda x, y: x+y)                 .sortByKey()


# In[26]:

calcldict = cal_clRDD.collectAsMap()


# In[27]:

print calcldict


# In[ ]:




# <h2> Check whether code is 1-1 to loc </h2>

# In[89]:

check_codeTolocRDD = clccRDD.distinct().rdd                         .map(lambda c: (c.contact_code, [c.contact_loc]))                         .reduceByKey(lambda x, y: x+y)                         .filter(lambda (x, y): len(y) > 1)


# In[90]:

check = check_codeTolocRDD.count()
# check = 1

if check == 0:
    print "1-1 checked"
else:
    print "total " + str(check) + " code have at least two location"
    print check_codeTolocRDD.take(5)


# <h2> Construct Map </h2>

# In[95]:

maplocTocodeRDD = clccRDD.distinct().rdd                     .map(lambda c: (c.contact_loc, [c.contact_code]))                     .reduceByKey(lambda x, y: x+y)


# In[96]:

maplocTocodedict = maplocTocodeRDD.collectAsMap()


# In[97]:

for x in maplocTocodedict.keys():
    print x
    print maplocTocodedict[x]
    print "total " + str(len(maplocTocodedict[x])) + " different code\n"


# In[98]:

# final check
tol = 0
for x in maplocTocodedict.keys():
    tol += len(maplocTocodedict[x])

if tol == clccnum:
    print "Final check complete!"
else:
    print "Something goes wrong! Check it again"


# In[ ]:




# <h1> Find Partition </h1>

# In[34]:

times = profile_df     .select('partition_time')     .distinct()     .orderBy('partition_time')     .rdd.map(lambda x: x.partition_time)     .collect()

times


# In[45]:

byPartitionDFlist = []


# In[46]:

def makeDFbytime(df, time):
    timedf = df.select('*')                 .where("partition_time = {}".format(time))
    return timedf


# In[47]:

for i in xrange(len(times)):
    byPartitionDFlist.append(makeDFbytime(profile_df, times[i]))


# In[48]:

byPartitionDFlist[0].first()


# In[43]:

locToid_timedict = {}


# In[55]:

def init_locToid_time(d, times, bpl):
    for i in xrange(len(times)):
        d.setdefault(times[i], {})
        bpldict = bpl[i].select('contact_loc', 'customer_id')                         .distinct().rdd                         .map(lambda li: (li.contact_loc, [li.customer_id]))                         .reduceByKey(lambda x, y: x+y)                         .collectAsMap()
        d[times[i]] = bpldict
    return d


# In[56]:

starttime = time.time()

locToid_timedict = init_locToid_time(locToid_timedict, times, byPartitionDFlist)

print "total time: " + str(time.time() - starttime)


# In[57]:

locToid_timedict


# <h1> Read ATM </h1>

# In[58]:

atm_path = os.path.abspath("../../dataset/atm/**/*.csv")


# In[59]:

atm_txtRDD = sc.textFile(atm_path)
atm_txtRDD.take(5)


# In[61]:

# atmnum = atm_txtRDD.count()
atmnum = 1934
print "total " + str(atmnum) + " atm datas"


# In[62]:

featurefields = [
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


# In[63]:

featureschema = StructType(featurefields)


# In[66]:

import json

def process_profile_line_feature(l):
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


# In[67]:

atm_df = atm_txtRDD     .map(process_profile_line_feature)     .toDF(schema=featureschema)

atm_df.show(5)


# In[69]:

atmtimes = atm_df                 .select('partition_time')                 .distinct()                 .orderBy('partition_time')                 .rdd.map(lambda x: x.partition_time)                 .collect()

atmtimes


# In[70]:

atmbyPartitionDFlist = []


# In[73]:

for i in xrange(len(times)):
    atmbyPartitionDFlist.append(makeDFbytime(atm_df, times[i]))


# In[74]:

atmbyPartitionDFlist[0].first()


# In[75]:

atmToid_timedict = {}


# In[ ]:

def init_atmToid_time(d, times, bpl):
    for i in xrange(len(atmtimes)):
        d.setdefault(atmtimes[i], {})
        bpldict = bpl[i].select('actor_id', 'customer_id')                         .distinct().rdd                         .map(lambda li: (li.contact_loc, [li.customer_id]))                         .reduceByKey(lambda x, y: x+y)                         .collectAsMap()
        d[times[i]] = bpldict
    return d


# In[ ]:




# <h1> Construct The Map of ATM and Address Zipcode </h1>

# In[115]:

cidatRDD = atm_df.select('channel_id', 'attrs').rdd                         .map(lambda c: (c.channel_id, c.attrs))                         .map(lambda (i, a): (i, json.loads(a)))


# In[123]:

cidatRDD.take(2)


# <h2> Check whether cid is 1-1 to Code </h2>

# In[116]:

# check 1-1 of channel_id to zipcode
check_cidTocodeRDD = cidatRDD                         .map(lambda (i, a): (i, a['channel']['address_zipcode']))                         .distinct()                         .map(lambda (x, y): (x, [y]))                         .reduceByKey(lambda x, y: x+y)                         .filter(lambda (x, y): len(y) > 1)


# In[117]:

check = check_cidTocodeRDD.count()

if check == 0:
    print "every cid have only one code, 1-1 checked"
else:
    print "total " + str(check) + " cid have at least two code"
    print check_cidTocodeRDD.take(5)


# <h2> Construct Map </h2>

# In[131]:

mapcodeTocidRDD = cidatRDD                     .map(lambda (x, y): (y['channel']['address_zipcode'], x))                     .distinct()                     .map(lambda (x, y): (x, [y]))                     .reduceByKey(lambda x, y: x+y)


# In[132]:

mapcodeTociddict = mapcodeTocidRDD.collectAsMap()


# In[133]:

for x in mapcodeTociddict.keys():
    print x
    print mapcodeTociddict[x]
    print "total " + str(len(mapcodeTociddict[x])) + " atm in the area\n"


# In[128]:

atmnum = atm_df.select('channel_id').distinct().count()
print "total " + str(atmnum) + " atm"


# In[134]:

# final check
tol = 0

for x in mapcodeTociddict.keys():
    tol += len(mapcodeTociddict[x])

if tol == atmnum:
    print "Final check complete!"
else:
    print "Something goes wrong! Check it again"


# In[ ]:




# <h1> Combine Location and ATM </h1>

# In[130]:

# maplocTocodedict: dictionary with location as key and code list as value
# locToid_timedict: dictionary with location as key and id list as value and partition by time
# mapcodeTociddict: dictionary with code as key and channel id list as value
# codeTolocdict: dictionary with code as key and location list as value
# locTociddict: dictionary with location as key and channel id list as value
# mapcidToiddict: dictionary with channel id as key and id list as value


# In[144]:

codeTolocdict = {}

for x in maplocTocodedict.keys():
    for i in xrange(len(maplocTocodedict[x])):
        cl = codeTolocdict.setdefault(maplocTocodedict[x][i], [])
        cl.append(x)


# In[145]:

codeTolocdict


# In[146]:

# check the code which have two location
codeTolocdict[u'457653'] = [u'J']


# In[148]:

locTociddict = {}

for x in mapcodeTociddict.keys():
    if x in codeTolocdict.keys():
        lcid = locTociddict.setdefault(codeTolocdict[x][0], [])
        for i in xrange(len(mapcodeTociddict[x])):
            lcid.append(mapcodeTociddict[x][i])
    else:
        lcid = locTociddict.setdefault(u'O', [])
        for i in xrange(len(mapcodeTociddict[x])):
            lcid.append(mapcodeTociddict[x][i])


# In[165]:

locTociddict


# In[155]:

mckey = mapcodeTociddict.keys()
clkey = codeTolocdict.keys()

ftol = 0
ttol = 0

for c in mckey:
    if c in clkey:
        ttol += 1
    else:
        ftol += 1
print (ttol, ftol)


# In[157]:

# checkpoint of ttol and ftol's 
if len(mapcodeTociddict.keys()) == (ftol + ttol):
    print "Final check complete!"
else:
    print "Something goes wrong! Check it again"


# In[156]:

# final check
tol = 0

for x in locTociddict.keys():
    tol += len(locTociddict[x])

if tol == atmnum:
    print "Final check complete!"
else:
    print "Something goes wrong! Check it again"


# <h2> Construct Relationship of channel id to channel id </h2>

# In[159]:

acchRDD = atm_df.select('actor_id', 'channel_id')


# In[160]:

map_calidTocidRDD = acchRDD.rdd                     .map(lambda i: ((i.actor_id, i.channel_id), 1))                     .reduceByKey(lambda x, y: x+y)


# In[171]:

map_calidTocidRDD.take(3)


# In[167]:

map_calidTocidRDD.aggregate(0, lambda x, y: x+y[1], lambda x, y: x+y)


# In[161]:

mapcidToidRDD = map_calidTocidRDD                     .map(lambda (x, y): x)                     .map(lambda (x, y): (y, x))                     .distinct()                     .map(lambda (x, y): (x, [y]))                     .reduceByKey(lambda x, y: x+y)


# In[163]:

mapcidToiddict = mapcidToidRDD.collectAsMap()


# In[164]:

mapcidToiddict


# In[168]:

mapidTociddict = {}

for x in mapcidToiddict.keys():
    for i in xrange(len(mapcidToiddict[x])):
        cidd = mapidTociddict.setdefault(mapcidToiddict[x][i], [])
        cidd.append(x)


# In[169]:

mapidTociddict


# <h1> Produce The Dot Code </h1>

# In[173]:

# convert to UTF-8
mapidTociddictencode = {}

for x in mapidTociddict.keys():
    for i in xrange(len(mapidTociddict[x])):
        encidd = mapidTociddictencode.setdefault(x.encode("UTF-8"), [])
        encidd.append(mapidTociddict[x][i].encode("UTF-8"))


# In[174]:

mapidTociddictencode


# In[176]:

for x in mapidTociddictencode.keys():
    for i in xrange(len(mapidTociddictencode[x])):
        print x + " -> " + mapidTociddictencode[x][i]


# In[ ]:



