
# coding: utf-8

# In[2]:

# basic libraries
import os
import math

import numpy as np
import pandas as pd
import matplotlib.pylab as plt


# In[3]:

# pyspark related
from pyspark import SparkContext
from pyspark import SparkConf


# In[4]:

sc = SparkContext('local[*]', 'PySpark')


# In[5]:

# spark sql related
from pyspark.sql import DataFrameWriter
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sql = sqlContext.sql


# In[6]:

sc.master


# In[ ]:




# <h1> Read File </h1>

# In[7]:

allRDD = sc.textFile("../../dataset/santander/train_ver2.csv")


# In[8]:

# allnum = allRDD.count()
allnum = 13647310
print "total " + str(allnum) + " datas"


# In[9]:

header = allRDD.first()
print header


# In[10]:

dataRDD = allRDD.filter(lambda d: d != header)
dataRDD.take(5)


# In[11]:

dataRDD = dataRDD.map(lambda d: d.replace(' ',''))
dataRDD.take(5)


# In[12]:

# findlossRDD = dataRDD.map(lambda d: d.split(',')).filter(lambda d: len(d) != 48)


# In[13]:

# findlossRDD.count()
findlossnum = 875157


# In[14]:

# distinctflRDD = findlossRDD.map(lambda d: len(d)).distinct()


# In[15]:

# distinctflRDD.collect() # [49]


# In[16]:

def data_cleaning(d):
    ds = d.split(',')
    if len(ds) == 48:
        del ds[2:24]
    else:
        del ds[2:25]
    
    ds[0] = ds[0].encode('utf-8')
    ds[1] = ds[1].encode('utf-8')
    
    for i in xrange(2,26):
        try:
            ds[i] = int(ds[i]) if ds[i] != '' and ds[i] != 'NA' else 0
        except ValueError:
            print ds
    return ds


# In[17]:

finalheader = ["fecha_dato","ncodpers","ind_ahor_fin_ult1","ind_aval_fin_ult1",
               "ind_cco_fin_ult1","ind_cder_fin_ult1","ind_cno_fin_ult1","ind_ctju_fin_ult1",
               "ind_ctma_fin_ult1","ind_ctop_fin_ult1","ind_ctpp_fin_ult1","ind_deco_fin_ult1",
               "ind_deme_fin_ult1","ind_dela_fin_ult1","ind_ecue_fin_ult1","ind_fond_fin_ult1",
               "ind_hip_fin_ult1","ind_plan_fin_ult1","ind_pres_fin_ult1","ind_reca_fin_ult1",
               "ind_tjcr_fin_ult1","ind_valo_fin_ult1","ind_viv_fin_ult1","ind_nomina_ult1",
               "ind_nom_pens_ult1","ind_recibo_ult1"]


# In[18]:

def operate_field_code():
    print "StructField(\'" + finalheader[0] + "\', StringType(), False),"
    print "StructField(\'" + finalheader[1] + "\', StringType(), False),"
    
    for i in xrange(2, len(finalheader)):
        if i != len(finalheader) - 1:
            print "StructField(\'" + finalheader[i] + "\', IntegerType(), False),"
        else:
            print "StructField(\'" + finalheader[i] + "\', IntegerType(), False)"


# In[19]:

operate_field_code()


# In[20]:

from pyspark.sql.types import *


# In[21]:

finaldatafield = [
    StructField('fecha_dato', StringType(), False),
    StructField('ncodpers', StringType(), False),
    StructField('ind_ahor_fin_ult1', IntegerType(), False),
    StructField('ind_aval_fin_ult1', IntegerType(), False),
    StructField('ind_cco_fin_ult1', IntegerType(), False),
    StructField('ind_cder_fin_ult1', IntegerType(), False),
    StructField('ind_cno_fin_ult1', IntegerType(), False),
    StructField('ind_ctju_fin_ult1', IntegerType(), False),
    StructField('ind_ctma_fin_ult1', IntegerType(), False),
    StructField('ind_ctop_fin_ult1', IntegerType(), False),
    StructField('ind_ctpp_fin_ult1', IntegerType(), False),
    StructField('ind_deco_fin_ult1', IntegerType(), False),
    StructField('ind_deme_fin_ult1', IntegerType(), False),
    StructField('ind_dela_fin_ult1', IntegerType(), False),
    StructField('ind_ecue_fin_ult1', IntegerType(), False),
    StructField('ind_fond_fin_ult1', IntegerType(), False),
    StructField('ind_hip_fin_ult1', IntegerType(), False),
    StructField('ind_plan_fin_ult1', IntegerType(), False),
    StructField('ind_pres_fin_ult1', IntegerType(), False),
    StructField('ind_reca_fin_ult1', IntegerType(), False),
    StructField('ind_tjcr_fin_ult1', IntegerType(), False),
    StructField('ind_valo_fin_ult1', IntegerType(), False),
    StructField('ind_viv_fin_ult1', IntegerType(), False),
    StructField('ind_nomina_ult1', IntegerType(), False),
    StructField('ind_nom_pens_ult1', IntegerType(), False),
    StructField('ind_recibo_ult1', IntegerType(), False)
]


# In[22]:

finalschema = StructType(finaldatafield)


# In[23]:

finaldata_df = dataRDD.map(data_cleaning).toDF(schema=finalschema)

finaldata_df.show(3)


# In[24]:

jandata_df = finaldata_df.select('ncodpers').show()


# In[25]:

jandata_df = finaldata_df.select('ncodpers').where(finaldata_df['fecha_dato'] == '2015-01-28')


# In[26]:

# jandata_df.count()


# In[27]:

jandanum = 625457


# In[28]:

janalldata_df = finaldata_df.select('*').filter("fecha_dato = '2015-01-28'")


# In[29]:

janalldata_df.show(3)


# In[30]:

janalldata_df.registerTempTable('January')
finaldata_df.registerTempTable('final')


# In[37]:

distinctdf = sqlContext.sql(
                """
                    SELECT j.*
                    FROM January j
                    LEFT JOIN final f
                    WHERE j.ncodpers = f.ncodpers
                    ORDER BY j.ncodpers
                """)


# In[38]:

import time

starttime = time.time()

distinctdf.show(5)

print time.time() - starttime


# In[ ]:



