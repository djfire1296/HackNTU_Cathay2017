
# coding: utf-8

# In[1]:

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



