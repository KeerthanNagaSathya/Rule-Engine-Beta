from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
import datetime


class Transform:

    logging.config.fileConfig("resources/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def parse_json(self, json_df):
        logging.debug("Parsing Json")
        self.json_df = json_df

        rulesExplodedDf = self.json_df.select(explode("rules").alias("rules")).select("rules.*")
        logging.debug(rulesExplodedDf.printSchema())
        logging.debug(rulesExplodedDf.show(truncate=False))

        parentDf = rulesExplodedDf.select("id", "name", "description", "is_valid", "valid_from", "valid_till",
                                          explode("then").alias("then")) \
            .select("id", "name", "description", "is_valid", "valid_from", "valid_till", "then.*")
        logging.debug(parentDf.printSchema())
        logging.debug(parentDf.show(truncate=False))

        childDf = rulesExplodedDf.select("id", explode("when").alias("when")) \
            .select("id", "when.*")
        logging.debug(childDf.printSchema())
        logging.debug(childDf.show(truncate=False))

        return parentDf, childDf

        # drop all the rows having null values
