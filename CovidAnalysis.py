from hdfs import Config, InsecureClient
from pyspark import SparkConf, SparkContext, SQLContext
import sys
from pyspark.sql.functions import *
from pyspark.sql import column
from pyspark.sql.functions import col

import os, math
import subprocess
from pyspark.sql.functions import lit
from pyspark.sql import Row

## Constants
APP_NAME = "Covid 19 Analyzer"
HDFS_RAWFILE_DIR = "/ASSIGNMENT/"
HDFS_OUTPUT_DIR = "/OUT/"
HDFS_BASE_URL = "hdfs://shibli001:9000"


def get_month(mon_code):
    switcher = {
        month: "Month",
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    return switcher.get(mon_code, "nothing")


def get_inf_rate(cases, test_perf):
    rate = (cases / test_perf) * 100
    return rate


def get_death_rate(deaths, cases):
    d_rate = (deaths / cases) * 100
    return d_rate


if __name__ == "__main__":

    # Folder creation for placing all the spark data
    cmd_a = "mkdir -p " + "/tmp/SPARK_PROCESS/"
    os.system(cmd_a)

    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME).set("spark.local.dir", "/tmp/SPARK_PROCESS/")

    # https://spark.apache.org/docs/latest/configuration.html
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    client = Config().get_client('shiblihdfs')
    files = client.list(HDFS_RAWFILE_DIR)
    totalfilecount = len(files)

    # if there is no file found in the dir
    if totalfilecount == 0:
        print("There is no files to be processed, application exiting...")
        sys.exit(0)

    filecount = 0

    for filename in files:
        print(filename)
        if filename.find("Covid_Analysis_DataSet.csv") >= 0:
            filecount = filecount + 1
            raw_data = sc.textFile(HDFS_BASE_URL + HDFS_RAWFILE_DIR + filename)

    if filecount == 0:
        print("There is no files to be processed by Spark, application exiting...")
        os.system.exit(0)

    csv_data = raw_data.map(lambda l: l.split(":"))

    row_labeled_data = csv_data.map(lambda p: Row(
        month=get_month(p[3]),
        year=p[4],
        coun_code=p[10],
        inf_rate=get_inf_rate(p[5], p[7]),
        death_rate=get_death_rate(p[6], p[5])
    ))
    interactions_labeled_df = sqlContext.createDataFrame(row_labeled_data)
    interaction_df_4 = interactions_labeled_df.select("month", "year", "count_code", "inf_rate", "death_rate")
    interaction_df_4.show()
    interaction_df_4.coalesce(1).write.mode("overwrite").csv(HDFS_BASE_URL + HDFS_OUTPUT_DIR + "MY_COVID_19.csv")
    # shutting down
    sc.stop()
