import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from CleanData import *
from Statistics import *
from JoinData import *

def main(argv):
    df_clean_data = getCleanData().cache()
    generateStats(df_clean_data)
    joinData(df_clean_data)

if __name__ == "__main__":
    main(sys.argv)


