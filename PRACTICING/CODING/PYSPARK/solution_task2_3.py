from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('solution_task2_3').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")


import pyspark.sql.functions as fn 

path1="C:\Users\Michela\Desktop\ieva\DATASCIENCE\LARRYS_TESTS\solutions_test_file1.csv"
path2="C:\Users\Michela\Desktop\ieva\DATASCIENCE\LARRYS_TESTS\solutions_test_file2.csv"

## import files
df1=spark.read.csv(path1,inferSchema=True, header=True)
df1.show()

df2=spark.read.csv(path2,inferSchema=True, header=True)
df2.show()

########################## Solution task2
## Join by user_id
dfjoined=df1.join(df2, df1.user_id == df2.user_id)
dfjoined.show()

## Some cleaning
dfclean=dfjoined.select("gender", "category", "comment_text")
dfclean.show()

########################## Solution task3
# In order to compute the percentage per category I need to know the total number of male and female 
# that commented

# Compute the total number of male that commented for all categories
total_m=dfjoined.groupBy("gender").count().collect()[0][1]
print(total_m)

# Compute the total number of male that commented for all categories
total_f=dfjoined.groupBy("gender").count().collect()[1][1]
print(total_f)

# Compute the total number of male and female that leaved a comment per each category
dfsumcategory=dfjoined.groupBy("category").pivot("gender").count()
dfsumcategory.show()

# Final compute the percentages with some formatting for a better readible result
dfsumcategory\
.withColumn('m_perc', fn.format_string("%.2f%%", dfsumcategory.m.cast("float")/total_m * 100))\
.withColumn('f_perc', fn.format_string("%.2f%%\n", dfsumcategory.f.cast("float")/total_f* 100))\
.orderBy('m','f', ascending = False).show()






