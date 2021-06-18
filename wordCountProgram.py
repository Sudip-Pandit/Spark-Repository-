#!/usr/bin/env python
# coding: utf-8

# ### A simple word count program in pyspark

# In[1]:
#import spark where it is installed
import findspark
findspark.init()
# In[5]:
#SparkSession and SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder                    .master("local")                    .appName("word count")                    .getOrCreate()
# In[7]:
# define sc
sc = spark.sparkContext
# In[8]:
sc.appName
# In[9]:
sc.binaryFiles
# In[10]:
#import the data
# data is in home directory (/home/su-1223/mm.txt)
firstRdd = sc.textFile("file:/home/su-1223/mm.txt")
# In[11]:
#action
firstRdd.collect()
# In[12]:
#flatmap
flatMapRdd = firstRdd.flatMap(lambda x: x.split(" "))
# In[13]:
flatMapRdd.take(5)
# In[14]:
flatMapRdd.collect()
# In[20]:
#use map function
rddMapFunction = firstRdd.flatMap(lambda x: x.split(" ")).map(lambda x:(x,1))
# In[21]:
rddMapFunction.take(6)
# In[23]:
rddMapFunction.collect()
# In[24]:
sc.stop()

