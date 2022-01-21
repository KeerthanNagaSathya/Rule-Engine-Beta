from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
import datetime

class Transform:

    def __init__(self, spark):
        self.spark = spark

    def parse_json(self, json_df):
        logging.info("Parsing JSOn")
        self.json_df = json_df

        rulesExplodedDf = self.json_df.select(explode("rules").alias("rules")).select("rules.*")
        rulesExplodedDf.printSchema()
        rulesExplodedDf.show(truncate=False)

        parentDf = rulesExplodedDf.select("id", "name", "description", "is_valid", "valid_from", "valid_till",
                                          explode("then").alias("then")) \
            .select("id", "name", "description", "is_valid", "valid_from", "valid_till", "then.*")
        parentDf.printSchema()
        parentDf.show(truncate=False)

        childDf = rulesExplodedDf.select("id", explode("when").alias("when")) \
            .select("id", "when.*")
        childDf.printSchema()
        childDf.show(truncate=False)

        return parentDf, childDf

        # drop all the rows having null values
