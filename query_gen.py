from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config


class query_gen:

    logging.config.fileConfig("resources/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def window_column_generator(self, pdf, cdf, table_name):
        ''''''"sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount"''''''

        print("This is {} {} {} {}"
              .format("one", "two", "three", "four"))

        amtQuery = " sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount"
        logging.debug("amtQuery > {}".format(amtQuery))

        ''''''"max(time) over (partition by id, date, txn_source_code order by date) as max_time"''''''

        maxtimeQuery = " max(time) over (partition by id, date, txn_source_code order by date) as max_time"
        logging.debug("maxtimeQuery > {}".format(maxtimeQuery))

        mintimeQuery = " min(time) over (partition by id, date, txn_source_code order by date) as min_time"
        logging.debug("mintimeQuery > {}".format(mintimeQuery))

        select_query = "select id, date, time, txn_source_code, amount, is_ttr, {}, {}, {} from {}".format(
            amtQuery, mintimeQuery, maxtimeQuery, table_name) + " order by id"
        logging.debug("select_query > {}".format(select_query))
        return select_query

    def rules_pipeline(self, pdf, cdf, table_name):
        rule_success = False
        check_rule = False
        check_rule_id = 0
        where_query = " where"
        select_query = """ with atm_window as (select id, date, txn_source_code, city, amount, is_ttr, time, sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount, max(time) over (partition by id, date, txn_source_code order by date) as max_time, min(time) over (partition by id, date, txn_source_code order by date) as min_time from {}) select * from atm_window""".format(table_name)

        for i in pdf:

            logging.debug("Looping through the json list")

            if check_rule:
                where_query = where_query + " and"
                logging.debug("where_query > {}".format(where_query))

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
                    logging.debug("Rule {} is valid and is being checked".format(p_id))

                    for j in cdf:
                        # print(row["field_name"] + ' > ' + row["field_value"] + ' > ' + row["join"] + ' > ' + row[
                        # "operator"] + ' > ')
                        c_id = j["id"]
                        c_name = j["field_name"]
                        c_value = j["field_value"]
                        c_join = j["join"]
                        c_operator = j["operator"]

                        if check_rule:
                            validate_rule_id = check_rule_id
                            logging.debug("check_rule is true and validate_rule_id is {}".format(validate_rule_id))
                        else:
                            validate_rule_id = int(p_id)
                            logging.debug("check_rule is false therefore follow the queue and validate_rule_id is {}".format(validate_rule_id))

                        if int(c_id) == validate_rule_id:

                            if not (j["join"] and j["join"].strip()) != "":

                                logging.debug("Join is empty")

                                if c_value.isnumeric():
                                    c_value = int(c_value)
                                    '''df.filter(col("state") == = "OH")'''
                                    if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                        where_query = where_query + " {} {} {}%".format(c_name, c_operator, c_value)
                                    else:
                                        where_query = where_query + " {} {} {}".format(c_name, c_operator, c_value)
                                    logging.debug("query > {}".format(where_query))
                                else:
                                    '''df.filter(col("state") == = "OH")'''
                                    if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                        where_query = where_query + " {} {} '{}%'".format(c_name, c_operator, c_value)
                                    else:
                                        where_query = where_query + " {} {} '{}'".format(c_name, c_operator, c_value)
                                    logging.debug("query > {}".format(where_query))

                            else:

                                logging.debug("Join is not empty")

                                if c_value.isnumeric():
                                    c_value = int(c_value)
                                    if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                        where_query = where_query + " {} {} {}%  {}".format(c_name, c_operator, c_value,
                                                                                       c_join)
                                    else:
                                        where_query = where_query + " {} {} {}  {}".format(c_name, c_operator, c_value,
                                                                                            c_join)
                                    logging.debug("query > {}".format(where_query))
                                else:
                                    if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                        where_query = where_query + " {} {} '{}%'  {}".format(c_name, c_operator, c_value,
                                                                                         c_join)
                                    else:
                                        where_query = where_query + " {} {} '{}'  {}".format(c_name, c_operator,
                                                                                              c_value,
                                                                                              c_join)
                                    logging.debug("query > {}".format(where_query))

                    ttr_check = "true"
                    logging.debug("where query > {}".format(where_query))
                    rule_success = True

                else:
                    logging.debug("Rule {} and {} are out of range and is skipped".format(p_valid_from, p_valid_till))
                    rule_success = False

            else:
                logging.debug("Rule {} is not valid and is skipped".format(p_id))
                rule_success = False

            if rule_success:
                logging.debug("Rule {} is success and the field name is {} ".format(p_id, p_field_name))
                if p_field_name == "lookup":
                    check_rule = True
                    check_rule_id = int(p_field_value)
                    logging.debug("check_rule_id is {} and check_rule is {} ".format(check_rule_id, check_rule))
                else:
                    where_query = where_query + " and ((bigint(to_timestamp(max_time)))-(bigint(to_timestamp(" \
                                                "min_time))))/(60) <= " \
                                                "30 order by id "
                    total_query = select_query + where_query
                    logging.debug("total_query > {}".format(total_query))
                    with open("output/queries.txt", "a") as f:
                        f.write(total_query)
                        f.write("\n\n")

                    tempDf = self.spark.sql(total_query)
                    tempDf.show()
                    # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")

