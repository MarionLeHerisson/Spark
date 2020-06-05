import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from CleanData import *
from Statistics import *

def main(argv):
    df_clean_data = getCleanData().cache()
    generateStats(df_clean_data)

if __name__ == "__main__":
    main(sys.argv)


