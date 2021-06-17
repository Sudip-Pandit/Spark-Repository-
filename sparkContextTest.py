#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Two min read: How to test sparkcontext!


# In[ ]:


import findspark
findspark.init()


# In[3]:


import pyspark


# In[4]:


sc = pyspark.SparkContext(appName = "Test")


# In[5]:


sc.master


# In[6]:


sc.appName

