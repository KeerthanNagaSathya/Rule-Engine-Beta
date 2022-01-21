from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config


class Ingest:

    def __init__(self, spark):
        logging.info("Ingest constructor")
        self.spark = spark

    def ingest_config(self):
        json_df = self.spark.read.option("multiline", "true").json("data/rules.json")
        logging.info("reading test json from file")
        json_df.printSchema()
        return json_df

    def ingest_atm_file(self):
        # Reading the source atm file and loading into a dataframe

        atm_df = self.spark.read.option("Header", "true").option("InferSchema", "true").csv("data/atm.csv")
        logging.info("Reading atm transactions csv file")
        return atm_df
