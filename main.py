from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
import ingest
import transform as t
from pyspark.sql.functions import *
import datetime
import query_gen


class PipeLine:
    logging.config.fileConfig("resources/logging.conf")

    def __init__(self):
        self.spark = None
        logging.info("Pipeline has started")

    def create_spark_session(self):
        self.spark = SparkSession \
            .builder \
            .appName("Rules Engine") \
            .master("local[*]") \
            .enableHiveSupport() \
            .getOrCreate()

        logging.info("spark session created")

    def run_pipeline(self):
        logging.info("run Pipeline")

        ingestion = ingest.Ingest(self.spark)
        json_df = ingestion.ingest_config()

        # collecting the dataframe back to the driver to pass it as a list for forming the query
        transformation = t.Transform(self.spark)
        pdf, cdf = transformation.parse_json(json_df)

        pdf.printSchema()
        cdf.printSchema()

        pdf_collect = pdf.collect()
        cdf_collect = cdf.collect()

        # Reading the source atm file and loading into a dataframe
        atm = ingestion.ingest_atm_file()
        atm.createOrReplaceTempView("atm_transactions")

        # Creating an object of class query gen as q
        q = query_gen.query_gen(self.spark)

        # Generating a window query for the atm table to get the total amount, min time and max time
        window_query = q.window_column_generator(pdf_collect, cdf_collect, "atm_transactions")

        with open("output/queries.txt", "w") as f:
            logging.info(f"Opened a file < {f} for writing queries into it")
            f.write(window_query)
            f.write("\n")

        # Applying the window query transformation to the source dataframe and storing it result in atm2 dataframe
        atm2 = self.spark.sql(window_query).withColumn("min_time", to_timestamp("min_time")).withColumn("max_time",
                                                                                                        to_timestamp(
                                                                                                            "max_time"))
        atm2.printSchema()
        atm2.show()

        atm2.createOrReplaceTempView("atm_cumulative")

        q.rules_pipeline(pdf_collect, cdf_collect, "atm_cumulative")

        f.close()
        logging.info(f"Closed the file <{f}>.")

        # Applying the rules query transformation to the cumulative atm dataframe
        '''atm2.createOrReplaceTempView("atm_cumulative")
        rules_query = q.rules_generator(json_df_collect, "atm_cumulative")
        self.spark.sql(rules_query).show()'''


if __name__ == '__main__':
    logging.info('Application started ')
    pipeline = PipeLine()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
