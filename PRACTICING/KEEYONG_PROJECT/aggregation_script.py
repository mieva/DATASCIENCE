import sys
import pandas as pd
import numpy as np

# read the data set
mb = pd.read_table("data_medium.tsv", sep='\t')
mb.columns = ["userid", "courseid", "videoposition"]
# mb.head(10)

# group by courseid
mb_grouped = mb.groupby(mb.courseid)
mb_aggr = mb_grouped['videoposition'].value_counts()

# check
# mb_aggr.head(10)

# write a file
mb_aggr.to_csv('Output.tsv', sep='\t')
