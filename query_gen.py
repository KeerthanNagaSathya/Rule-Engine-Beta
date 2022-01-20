from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
import ingest
from pyspark.sql.functions import *
import datetime


class query_gen():

    def __init__(self, spark):
        self.spark = spark

    def window_column_generator(self, test_collect, table_name):
        ''''''"sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount"''''''

        amtQuery = f" sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount"
        logging.info(f"amtQuery > {amtQuery}")

        ''''''"max(time) over (partition by id, date, txn_source_code order by date) as max_time"''''''

        maxtimeQuery = f" max(time) over (partition by id, date, txn_source_code order by date) as max_time"
        logging.info(f"maxtimeQuery > {maxtimeQuery}")

        mintimeQuery = f" min(time) over (partition by id, date, txn_source_code order by date) as min_time"
        logging.info(f"mintimeQuery > {mintimeQuery}")

        select_query = f"select id, date, time, txn_source_code, amount, is_ttr, {amtQuery}, {mintimeQuery}, {maxtimeQuery} from {table_name}" + " order by id"
        logging.info(f"select_query >  {select_query}")
        return select_query

    def rules_generator(self, df, table_name):
        where_query = " where"
        logging.info("Looping through the json list")

        for row in df:
            # print(row["field_name"] + ' > ' + row["field_value"] + ' > ' + row["join"] + ' > ' + row["operator"] + ' > ')

            fname = row["field_name"]
            fvalue = row["field_value"]
            fjoin = row["join"]
            foperator = row["operator"]

            if not (row["join"] and row["join"].strip()) != "":

                logging.info("Join is empty")

                if fvalue.isnumeric():
                    fvalue = int(fvalue)
                    '''df.filter(col("state") == = "OH")'''
                    where_query = where_query + f" {fname} {foperator} {fvalue}"
                    logging.info(f"query > {where_query}")
                else:
                    '''df.filter(col("state") == = "OH")'''
                    where_query = where_query + f" {fname} {foperator} '{fvalue}'"
                    logging.info(f"query > {where_query}")

            else:

                logging.info("Join is not empty")

                if fvalue.isnumeric():
                    fvalue = int(fvalue)
                    where_query = where_query + f" {fname} {foperator} {fvalue}  {fjoin}"
                    logging.info(f"query > {where_query}")
                else:
                    where_query = where_query + f" {fname} {foperator} '{fvalue}'  {fjoin}"
                    logging.info(f"query > {where_query}")

        ttr_check = "true"
        # time_diff = " and max_time - min_time > 30"
        where_query = f"select id, date, time, txn_source_code, amount, total_amount, is_ttr, ((bigint(to_timestamp(" \
                      f"max_time)))-(bigint(to_timestamp(min_time))))/(60) as time_diff from {table_name}" + where_query \
                      + " and ((bigint(to_timestamp(max_time)))-(bigint(to_timestamp(min_time))))/(60) <= 30 order by id "
        logging.info(f"where query >  {where_query}")
        return where_query