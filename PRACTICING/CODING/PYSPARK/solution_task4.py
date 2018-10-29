from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('solution_task4').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

from pyspark.ml.feature import Tokenizer, CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover
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

## Split comments in list of words by Tokenization
tokenizer = Tokenizer(inputCol="comment_text",outputCol="tokenized_comment_text")
dftokenized = tokenizer.transform(dfclean)
dftokenized.show()

## Put toghether all the comments in a list of array per category and gender
wordsData=dftokenized.groupBy("gender","category")\
.agg(collect_list("tokenized_comment_text").alias("message"))
#wordsData.show()

## In order to calculate the most common words define a udf to unpack the list of array and a udf to perform 
## most common calculation using the python operator Counter and put in a kind of dictionary [words, number of occurrence]

# Define a UDF to unpuck list of array
unpack_udf = udf(\
	lambda l: [item for sublist in l for item in sublist], returnType=ArrayType(StringType())\
	)

# We need to specify the schema of the return object
schema_count = ArrayType(StructType([\
    StructField("word", StringType(), False),\
    StructField("count", IntegerType(), False)\
]))

# Define a UDF to find the 3 most common words
count_udf = udf(\
    lambda s: Counter(s).most_common()[:3],\
    schema_count\
)

## Putting all together
dffinal=wordsData\
.withColumn("message", unpack_udf("message")).withColumn("message", count_udf("message"))
#dffinal.show()

## final selection by gender and category
print(dffinal.filter("gender = 'f' and category = 'People & Blogs'")\
	.select('gender', 'category', 'message').collect())

# IMPORTANT TIPS: the result is not so significant...We need to considerer a StopWordsRemover 
# (Stop words are words which should be excluded from the input, typically because the 
# words appear frequently and don't carry as much meaning) after tokenization.

## StopWordRemover implementation
## remove 20 most occuring documents, documents with non numeric characters, and documents with <= 3 characters
cv_tmp = CountVectorizer(inputCol="tokenized_comment_text", outputCol="tmp_vectors")
cv_tmp_model = cv_tmp.fit(dftokenized)


top20 = list(cv_tmp_model.vocabulary[0:20])
more_then_3_charachters = [word for word in cv_tmp_model.vocabulary if len(word) <= 3]
contains_digits = [word for word in cv_tmp_model.vocabulary if any(char.isdigit() for char in word)]

stopwords = []  #Add additional stopwords in this list

# Combine the three stopwords
stopwords = stopwords + top20 + more_then_3_charachters + contains_digits

# Remove stopwords from the tokenized list
remover = StopWordsRemover(inputCol="tokenized_comment_text", outputCol="filtered", stopWords = stopwords)
dftokenized = remover.transform(dftokenized)
#dftokenized.show(3)

## REPEAT THE PROCEDURE
wordsData=dftokenized.groupBy("gender","category")\
.agg(collect_list("filtered").alias("message"))
#wordsData.show()

dffinal=wordsData\
.withColumn("message", unpack_udf("message")).withColumn("message", count_udf("message"))
#dffinal.show()

print(dffinal.filter("gender = 'f' and category = 'People & Blogs'")\
	.select('gender', 'category', 'message').collect())

## Now the results make more sense!




