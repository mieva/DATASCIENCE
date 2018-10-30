import sys
import pandas as pd
import numpy as np

try:
#read data
  rows_list = []

  while True:
      line = sys.stdin.readline()
      if not line:
   	     break

      row = (int(x) for x in line.split('\t'))
      rows_list.append(row)

  df = pd.DataFrame(rows_list, columns=('userid', 'courseid', 'videoposition'))
  df_grouped = df.groupby(df.courseid)
  df_aggr = df_grouped['videoposition'].value_counts()
  #df_aggr.columns = ["course", "videopos", "consuption"]

# print output
  print(df_aggr)

except:
  #In case of an exception, write the stack trace to stdout so that we
  #can see it in Hive, in the results of the UDF call.
  print(sys.exc_info())
