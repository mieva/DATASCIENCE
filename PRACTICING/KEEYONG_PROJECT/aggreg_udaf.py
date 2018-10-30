import sys
import logging
from itertools import groupby
from operator import itemgetter
import pandas as pd
import numpy as np

SEP = '\t'
NULL = '\\N'

_logger = logging.getLogger(__name__)


def read_input(input_data):
    for line in input_data:
        yield line.strip().split(SEP)


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    data = read_input(sys.stdin)
    for course_id, group in groupby(data, itemgetter(1)):
        _logger.info("Reading group {}...".format(course_id))
        #group = [(int(rowid), course_id, np.nan if price == NULL else float(price))
	group = [(int(user_id), course_id, int(video_position))
                 for user_id, course_id, video_position in group]
        df = pd.DataFrame(group, columns=('userid', 'courseid', 'videoposition'))
        #output = [vtype, df['price'].mean(), df['price'].std()]
	output = [courseid, df['videoposition'].value_counts()]
        print(SEP.join(str(o) for o in output))


if __name__ == '__main__':
    main()
