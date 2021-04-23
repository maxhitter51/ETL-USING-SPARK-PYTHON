from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession

"""
Creating Spark Session

Here our aim is to find companies with volume having 10000000 and above

"""
spark = SparkSession.builder.master("local").appName("AnalysisOnAjantPharma").getOrCreate()

conf = SparkConf().setMaster("local").setAppName("MaxVolumes")
sc = SparkContext.getOrCreate()


#FILTER FUNCTION
def vol1(row):

    if row[6] > 10000000:
        return(row)

#EXTRACT THE DATA IN PROPER FORMAT
spth1="AJANTPHARM.csv"
s_df = spark.read.format("CSV").option("header","true").option("inferSchema", "true").load(spth1)

#TRANSFORMING THE DATA ACCORDING TO BUSINESS REQUIREMENT
s_df=s_df.toDF("Date","Open","High","Low","Last","Close","Volume","Turnover").rdd.map(lambda row: (row[0], row[1], row[2],row[3],row[4],row[5],row[6]))

#APPLYING FILTER TO GET TOP COMAPNIES
s_df=s_df.filter(lambda row: vol1(row))
print(s_df.take(12))

#LOAD THE DATA IN TEXT FILE

s_df.saveAsTextFile('top12companies.txt')
