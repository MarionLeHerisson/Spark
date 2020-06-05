import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from CleanData import *
from Statistics import *
from JoinData import *

if __name__ == "__main__":
    main(sys.argv)

class FootballApp():

    def main(self, argv):
        dataCleaner = CleanData()
        statsGenerator = Statistics()
        dataJoiner = JoinData()

        df_clean_data = dataCleaner.getCleanData(argv[1]).cache()
        statsGenerator.generateStats(df_clean_data)
        dataJoiner.joinData(df_clean_data)
