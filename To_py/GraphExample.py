
# coding: utf-8

# In[1]:

import os
import math
import time

import numpy as np
import pandas as pd
import matplotlib.pylab as plt


# In[2]:

sc.master


# In[3]:

# spark related
from pyspark.sql import DataFrameWriter
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sql = sqlContext.sql


# In[4]:

# graph related
from graphframes import *


# In[5]:

# Vertex DataFrame
v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
], ["id", "name", "age"])


# In[6]:

# Edge DataFrame
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])


# In[7]:

# Create a GraphFrame
g = GraphFrame(v, e)


# In[8]:

from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()


# In[9]:

g.inDegrees.show()


# In[10]:

g.edges.filter("relationship = 'follow'").count()


# In[11]:

g.vertices.show()


# In[12]:

g.edges.show()


# In[13]:

# Find the youngest user's age in the graph.
# This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()


# In[14]:

# Count the number of "follows" in the graph.
# This queries the edge DataFrame.
numFollows = g.edges.filter("relationship = 'follow'").count()


# In[15]:

# Search for pairs of vertices with edges in both directions between them.
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()


# In[16]:

# More complex queries can be expressed by applying filters.
motifs.filter("b.age > 30").show()


# In[17]:

from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import IntegerType


# In[18]:

chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")


# In[19]:

# Query on sequence, with state (cnt)
#  (a) Define method for updating state given the next element of the motif.
sumFriends =  lambda cnt,relationship: when(relationship == "friend", cnt+1).otherwise(cnt)


# In[20]:

#  (b) Use sequence operation to apply method to sequence of elements in motif.
#      In this case, the elements are the 3 edges.
condition =  reduce(lambda cnt,e: sumFriends(cnt, col(e).relationship), ["ab", "bc", "cd"], lit(0))


# In[21]:

#  (c) Apply filter to DataFrame.
chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()


# In[22]:

# Select subgraph of users older than 30, and edges of type "friend"
v2 = g.vertices.filter("age > 30")
e2 = g.edges.filter("relationship = 'friend'")
g2 = GraphFrame(v2, e2)


# In[23]:

g2.edges.show()


# In[24]:

# Select subgraph based on edges "e" of type "follow"
# pointing from a younger user "a" to an older user "b".
paths = g.find("(a)-[e]->(b)")  .filter("e.relationship = 'follow'")  .filter("a.age < b.age")


# In[25]:

# "paths" contains vertex info. Extract the edges.
e2 = paths.select("e.src", "e.dst", "e.relationship")


# In[26]:

# Construct the subgraph
g2 = GraphFrame(g.vertices, e2)


# In[27]:

# Search from "Esther" for users of age < 32.
paths = g.bfs("name = 'Esther'", "age < 32")
paths.show()


# In[28]:

# Specify edge filters or max path lengths.
g.bfs("name = 'Esther'", "age < 32",  edgeFilter="relationship != 'friend'", maxPathLength=3)


# In[29]:

# Save vertices and edges as Parquet to some location.
# g.vertices.write.parquet("hdfs://myLocation/vertices")
# g.edges.write.parquet("hdfs://myLocation/edges")

# Load the vertices and edges back.
# sameV = sqlContext.read.parquet("hdfs://myLocation/vertices")
# sameE = sqlContext.read.parquet("hdfs://myLocation/edges")

