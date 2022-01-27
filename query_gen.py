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

    def window_column_generator(self, pdf, cdf, table_name):
        ''''''"sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount"''''''

        print("This is {} {} {} {}"
              .format("one", "two", "three", "four"))

        amtQuery = " sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount"
        logging.info("amtQuery > {}".format(amtQuery))

        ''''''"max(time) over (partition by id, date, txn_source_code order by date) as max_time"''''''

        maxtimeQuery = " max(time) over (partition by id, date, txn_source_code order by date) as max_time"
        logging.info("maxtimeQuery > {}".format(maxtimeQuery))

        mintimeQuery = " min(time) over (partition by id, date, txn_source_code order by date) as min_time"
        logging.info("mintimeQuery > {}".format(mintimeQuery))

        select_query = "select id, date, time, txn_source_code, amount, is_ttr, {}, {}, {} from {}".format(
            amtQuery, mintimeQuery, maxtimeQuery, table_name) + " order by id"
        logging.info("select_query > {}".format(select_query))
        return select_query

    def rules_pipeline(self, pdf, cdf, table_name):
        rule_success = False

        for i in pdf:

            where_query = " where"
            logging.info("Looping through the json list")

            p_id = i["id"]
            p_name = i["name"]
            p_desc = i["description"]
            p_is_valid = i["is_valid"]
            p_valid_from = i["valid_from"]
            p_valid_till = i["valid_till"]
            p_field_name = i["field_name"]
            p_field_value = i["field_value"]

            if p_is_valid == "true":
                if p_valid_till != 1:  # This needs to be replaced with if current date is in between valid from and
                    # valid till
                    logging.info("Rule {} is valid and is being checked".format(p_id))

                    for j in cdf:
                        # print(row["field_name"] + ' > ' + row["field_value"] + ' > ' + row["join"] + ' > ' + row[
                        # "operator"] + ' > ')
                        c_id = j["id"]
                        c_name = j["field_name"]
                        c_value = j["field_value"]
                        c_join = j["join"]
                        c_operator = j["operator"]

                        if int(p_id) == int(c_id):

                            if not (j["join"] and j["join"].strip()) != "":

                                logging.info("Join is empty")

                                if c_value.isnumeric():
                                    c_value = int(c_value)
                                    '''df.filter(col("state") == = "OH")'''
                                    where_query = where_query + " {} {} {}".format(c_name, c_operator, c_value)
                                    logging.info("query > {}".format(where_query))
                                else:
                                    '''df.filter(col("state") == = "OH")'''
                                    where_query = where_query + " {} {} '{}'".format(c_name, c_operator, c_value)
                                    logging.info("query > {}".format(where_query))

                            else:

                                logging.info("Join is not empty")

                                if c_value.isnumeric():
                                    c_value = int(c_value)
                                    where_query = where_query + " {} {} {}  {}".format(c_name, c_operator, c_value,
                                                                                       c_join)
                                    logging.info("query > {}".format(where_query))
                                else:
                                    where_query = where_query + " {} {} '{}'  {}".format(c_name, c_operator, c_value,
                                                                                         c_join)
                                    logging.info("query > {}".format(where_query))

                    ttr_check = "true"
                    # time_diff = " and max_time - min_time > 30"
                    where_query = "select id, date, time, txn_source_code, amount, total_amount, is_ttr, ((bigint(" \
                                  "to_timestamp(" \
                                  "max_time)))-(bigint(to_timestamp(min_time))))/(60) as time_diff from {}".format(
                                    table_name) + where_query \
                                  + "and ((bigint(to_timestamp(max_time)))-(bigint(to_timestamp(min_time))))/(60) <= " \
                                    "30 order by id "
                    logging.info("where query > {}".format(where_query))
                    rule_success = True

                else:
                    logging.info("Rule {} and {} are out of range and is skipped".format(p_valid_from, p_valid_till))
                    rule_success = False

            else:
                logging.info("Rule {} is not valid and is skipped".format(p_id))
                rule_success = False

            if rule_success:
                with open("output/queries.txt", "a") as f:
                    f.write(where_query)
                    f.write("\n\n")
                tempDf = self.spark.sql(where_query)
                tempDf.show()
