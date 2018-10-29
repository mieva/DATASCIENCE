from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('solution_task5').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from collections import Counter
from pyspark.sql.types import *


path1="C:\Users\Michela\Desktop\ieva\DATASCIENCE\LARRYS_TESTS\solutions_test_file1.csv"
path2="C:\Users\Michela\Desktop\ieva\DATASCIENCE\LARRYS_TESTS\solutions_test_file2.csv"

## import files
df1=spark.read.csv(path1,inferSchema=True, header=True)
df1.show()

df2=spark.read.csv(path2,inferSchema=True, header=True)
df2.show()

## Join by user_id
dfjoined=df1.join(df2, df1.user_id == df2.user_id)
#dfjoined.show()

## Some cleaning
dfclean=dfjoined.select("gender", "category", "comment_text")
dfclean.show()

## Tokenization
tokenizer = Tokenizer(inputCol="comment_text",outputCol="tokenized_comment_text")
dftokenized = tokenizer.transform(dfclean)
dftokenized.show()

# Define a UDF to unpack list of array
unpack_udf = udf(lambda l: [item for sublist in l for item in sublist], returnType=ArrayType(StringType()))


# Put togheter words by category and gender in a list of arrays and unpack 
# to have a single array with all the words by category and gender and be able to know the total number of words 
# by his lenght
df_gc=dftokenized.groupBy("category", "gender").agg(collect_list("tokenized_comment_text").alias("allwords"))\
	.withColumn("allwords", unpack_udf("allwords"))
#df_gc.show()


# Explode the df by the array of words to count the number of occurrence for each word
dfexploded=df_gc.select("category","gender", explode("allwords").alias("words"))
#dfexploded.show(1)

## Compute num_all
# Count number of occurrence per word (first numerator)

import pyspark.sql.functions as func

#dfcounted=dfexploded.groupBy("words").count().selectExpr('count AS num_all') ## toglie la colonna words,pero' funziona
dfcounted=dfexploded.groupBy("words").count().select("words", func.col("count").alias("num_all"))
#dfcounted.show(1)

## calcolo il sum_all (first denominator) 
sum_all=dfcounted.selectExpr("sum(num_all)").collect()[0][0]
print(sum_all)

## Compute f_all
df_all=dfcounted.withColumn("f_all", (col("num_all").cast("float")/sum_all))

## Compute num_gc
# Count number of occurrence per word (second numerator)
dfcounted2=dfexploded.groupBy("category","gender","words").count().select("category","gender","words", func.col("count").alias("num_gc"))
#dfcounted2.show(1)

## calcolo il sum_gc (second denominator) summing over count
df_gc=dfcounted2.groupBy("category","gender").sum()\
.select("category","gender", func.col("sum(num_gc)").alias("sum_gc"))
#df_gc.show(1)


## Join dfcounted2 and df_gc to calculate f_gc
cond = [dfcounted2.gender == df_gc.gender, dfcounted2.category == df_gc.category]
df_gc_freq=dfcounted2.join(df_gc, cond)\
.select(dfcounted2.category, dfcounted2.gender, "words", "num_gc", "sum_gc")
#df_gc_freq.show(1)

## Compute f_gc
df_f_gc=df_gc_freq.withColumn("f_gc", (col("num_gc").cast("float")/col("sum_gc")))
#df_f_gc.show(1)

# Join the two dfs to have all info togheter and compute R
dffinal=df_all.join(df_f_gc, df_all.words == df_f_gc.words).select(df_all.words, "category", "gender",\
 "num_all", "f_all", "f_gc")
#dffinal.show(1)

# Compute R and some cleaning for possible duplicate
dfwithfrequency=dffinal.withColumn("R", col("f_gc")/col("f_all"))
#dfwithfrequency=dffinal.withColumn("R", col("f_gc")/col("f_all")).dropDuplicates()
#dfwithfrequency.show(5)

# Select the the top words order by R according selection
(dfwithfrequency.filter("gender = 'f' and category = 'People & Blogs' and num_all > 4").select("words", "f_all", "f_gc", "R", "gender", "category", "num_all").orderBy("R", ascending = False)).show(3)

