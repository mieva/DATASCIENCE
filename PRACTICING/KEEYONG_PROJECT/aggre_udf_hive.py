import sys
import logging
import pandas as pd
import numpy as np

SEP = '\t'

_logger = logging.getLogger(__name__)

try:

  # read the data set
  def read_input(input_data):
      for line in input_data:
          yield line.strip().split(SEP)

  def main():
      logging.basicConfig(level=logging.INFO, stream=sys.stderr)
      data = read_input(sys.stdin)
      df = pd.DataFrame(data, columns=('userid', 'courseid', 'videoposition'))

      # group by courseid
      ##df_grouped = df.groupby('courseid')
      ##ds_aggr = df_grouped['videoposition'].value_counts()
      df['freq'] = df.groupby('courseid')['videoposition'].transform('count')
	
      # write a file
      ##print(ds_aggr)
      print(df)

  if __name__ == '__main__':
     main()

except:
   #In case of an exception, write the stack trace to stdout so that we
   #can see it in Hive, in the results of the UDF call.
   print(sys.exc_info()) 

