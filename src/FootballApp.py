import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from CleanData import *

def main(argv):
    df_clean_data = getCleanData()

if __name__ == "__main__":
    main(sys.argv)


