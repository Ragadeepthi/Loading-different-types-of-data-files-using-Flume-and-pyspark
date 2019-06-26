
# coding: utf-8

# In[2]:


## Set Python - Spark environment.
import os
import sys

os.environ["SPARK_HOME"] = "/usr/hdp/current/spark2-client"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.5.0  pyspark-shell'
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.4-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")


# In[3]:


## Create SparkContext, SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Cute5_ProductTransaction").setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


# In[4]:


spark


# In[5]:


spark.sparkContext.getConf().getAll()


# In[6]:


# Loading required libraries


# In[7]:


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col, countDistinct


# ### 1. Copy the data files from HDFS (“/user/gauthamp/CUTE/B49/Data/“) into the linux machine. 
#  hdfs dfs -copyToLocal /user/gauthamp/CUTE/B49/Data/productData.csv
#  
#  hdfs dfs -copyToLocal /user/gauthamp/CUTE/B49/Data/customerData.xml
#  
#  hdfs dfs -copyToLocal /user/gauthamp/CUTE/B49/Data/orderData.json
#  
# 

# ### 2. Load the Product data in to HDFS using flume. 
# flume-ng agent --conf ./Conf/ -f Conf/flume-conf.properties -Dflume.root.logger=DEBUG,console -n CuteAgent_2383

# ***3. In the flume properties file, rename the Agent, Source, Sink and Channel with below naming convention.
# 
# 1. The Agent name should be: CuteAgent_<enroll ID>
# 2. Source name should be: CuteSource_<enroll ID>
# 3. Sink name should be: CuteSink_<enroll ID>
# 4. Channel name should be: CuteChannel_<enroll ID>
#     
# Changed Accordingly

# ### 4. Read the Product data loaded into hdfs using flume and create Sparkdata frame. 

# #### **Product data consists of the data with order transactions.**
# 
# 1. Row ID - Row Id of the transaction
# 2. Product ID - Product ID
# 3. Category - Category the product belongs to
# 4. Sub-Category - Sub-Category the product belongs to
# 5. Product Name - Name of the product 

# In[8]:


## Reading product data and creating a dataframe
productdata = spark.read.format("csv")       .option("header", "true")       .option("inferSchema", "True")       .load("/user/2383B49/product/data/FlumeData.*")


# In[9]:


productdata.printSchema()


# In[10]:


productdata.show(4)


# ### 5. Read the Customer Data from the XML file and create spark dataframe. 

# #### Customerdata consists of the customer transaction data.
# 6. Row ID - Row Id of the transaction
# 7. Customer ID - Customer ID
# 8. Customer Name - Customer Name
# 9. Segment - To which segment the order belongs to
# 10. City - Order belongs to which city
# 11. State - Order belongs to which state
# 12. Country - Order belongs to which Country
# 13. Postal Code - Order pinched
# 14. Market - Which market the order belongs to
# 15. Region - order belongs to which region 

# In[11]:


## Reading product data and creating a dataframe
cust_data = spark.read.format("xml")       .option("rowTag",'custtransdata')       .load("file:///home/2383B49/Cute5/cust_order_data/customerData.xml")   


# In[12]:


cust_data.printSchema()


# ### 6.Read the Order Data from the JSON file and create a spark dataframe.

# #### Orderdata consists of the order transaction details. 
# 16. Row ID - Row Id of the transaction 
# 17. Order ID - Transaction order ID
# 18. Order Date - Order placed Date
# 19. Ship Date - Order Shipped Date
# 20. Ship Mode - Mode of shipment of the order
# 21. Sales - Sale amount of the product
# 22. Quantity - Number of items ordered
# 23. Discount - Discount on the product
# 24. Profit - Profit on the order
# 25. Shipping Cost - shipping cost of the order
# 26. Order Priority - Shipping priority of the order 

# In[13]:


#Reading order data and creating a dataframe
order_data = spark.read.format("json")       .option("header", "False")       .option("inferSchema", "True")       .load("file:///home/2383B49/Cute5/cust_order_data/orderData.json")


# In[14]:


order_data.printSchema()


# In[15]:


order_data.show(4)


# ### 7.Create a final merged data frame by merging Customer Data, Product Data and Order Data data frames (Created in Step-4/5/6) on RowID.  

# In[124]:


#joining the above three product, customer, order dataframes
trans_data =productdata.join(cust_data,'RowID', 'inner').join(order_data,on='RowID')


# In[125]:


trans_data.printSchema()


# In[126]:


trans_data.show(1)


# ### 8.Drop the “RowID” from the merged data frame.

# In[127]:


trans_data = trans_data.drop('RowID')


# ### 9. “Ship Date” and “Order Date” columns has date values with both backslash and hyphens. Convert all the values in one format (I.e. dd/ MM/yyyy). Using “regexp_replace” function replace hyper (“-“) with backslash (“/“)  

# In[128]:


trans_data = trans_data.withColumn("ShipDate", regexp_replace("ShipDate", "-", "/"))
trans_data.show(10)


# In[129]:


trans_data = trans_data.withColumn("OrderDate", regexp_replace("OrderDate", "-", "/"))
trans_data.show(10)


# ### 10.Convert the columns “Ship Date” and “Order Date” into date type. 

# In[130]:


trans_data = trans_data.withColumn("ShipDate",to_date(unix_timestamp('ShipDate', 'dd/mm/yyyy').cast("timestamp")))
trans_data.show(2)

#trans_data.printSchema()


# In[131]:


trans_data = trans_data.withColumn("OrderDate",to_date(unix_timestamp('OrderDate', 'dd/mm/yyyy').cast("timestamp")))
trans_data.show(2)
#trans_data.printSchema()


# ### 11.Print the number of rows and columns of the merged data frame and print the count of NULL values for each column. 

# In[132]:


print("Number of total rows in dataframe = {}" .format(trans_data.count()))
print("Number of total columns in dataframe = {}" .format(len(trans_data.columns)))


# In[133]:


trans_data.select([count(when(col(c).isNull(), c)).alias(c) for c in trans_data.columns]).show()


# ### 12. Postal Code column has NULL values in the form of “nan”. Replace the “nan” value with “None” 

# In[134]:


trans_data = trans_data.withColumn('PostalCode', when(trans_data.PostalCode == 'nan', None))


# ### 13.Check if there are any null values in the data frame and remove the columns with more than 50 % rows has null values. 
# 

# In[135]:


trans_data.select([count(when(col(c).isNull(), c)).alias(c) for c in trans_data.columns]).show()


# In[136]:


#Dropping PostalCode Column as it is having more than 50% of the Null values 
trans_data = trans_data.drop('PostalCode')


# ### 14.After Step-13 remove records if there are null values in any of the rows. 

# In[137]:


trans_data.select([count(when(col(c).isNull(), c)).alias(c) for c in trans_data.columns]).show()


# ### 15. Derive the new feature by subtracting Ship Date from Order Date and identify if there are any transactions where Order Date is greater than ship date and ignore those transactions. 

# In[138]:


trans_data = trans_data.withColumn("DaysTakenToDispatch", 
              datediff("ShipDate",
                       "OrderDate"))


# In[139]:


trans_data.show(10)


# In[140]:


#checking whether new datediff column is having any negative values
trans_data.filter(col("DaysTakenToDispatch") >= 0).count()


# ### 16. Derive the features Day of the month, Month and Year from the Order Date column. 

# In[141]:


trans_data = trans_data.withColumn("year", year("OrderDate")).withColumn("month", month("OrderDate")).withColumn("dayofmonth", dayofmonth("OrderDate"))
trans_data.show(5)


# ### 17.Convert the Category, Sub Category and Product name columns into upper case and trim the extreme ends. 

# In[142]:


from pyspark.sql.functions import upper, col, trim
trans_data = trans_data.withColumn('Category', upper(col('Category'))).withColumn('SubCategory', upper(col('SubCategory'))).withColumn('ProductName', upper(col('ProductName')))


# In[151]:


#trans_data.select(trim(trans_data['Category']).alias('Category'),trim(trans_data['SubCategory']).alias('SubCategory'),trim(trans_data['ProductName']).alias('ProductName'))
trans_data= trans_data.withColumn('Category', trim(col('Category'))).withColumn('SubCategory', trim(col('SubCategory'))).withColumn('ProductName', trim(col('ProductName')))


# In[152]:


trans_data.show(4)


# ### 18.Get the top 10 Product names which are purchased very frequently across stores. 

# In[170]:


dfWithCount = trans_data.groupBy("ProductID").agg(count("ProductID").alias("itemCount")).sort(desc('itemCount'))
dfWithCount.show(10)


# ### 19. Get the top 10 orders which has the highest Sale Amount. 

# In[174]:


dfWithCount = trans_data.groupBy("OrderID","Sales").agg(max("Sales")).sort(desc('max(Sales)'))
dfWithCount.show(10)


# ### 20. Get the Top 10 most visiting customers based on frequency of purchase (Based on Orders).

# In[175]:


dfWithCount = trans_data.groupBy("CustomerID","CustomerName").agg(count("CustomerID")).sort(desc("count(CustomerID)"))
dfWithCount.show(10)

