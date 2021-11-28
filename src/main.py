from pyspark.sql import SparkSession
import pyspark.sql.functions as SparkFunctions
from pyspark import SparkContext
from pyspark.sql.dataframe import DataFrame
from kafka import *
import os
from kafka import *



def punkt2():
    SparkContext.setSystemProperty('spark.executor.memory', '8g')
    sc = SparkContext("local", "App Name")

    spark = SparkSession.builder.appName("parquetFile").getOrCreate()

    df_enriched = spark.read.parquet("data/datasets/agg_usage.parquet")

    df_operators = spark.read.option("header", True).csv("data/datasets/parent_operator.csv").withColumnRenamed(
        "parent_operator_code", "parent_operator_code_b")
    df_enriched = df_enriched.join(df_operators.select(["parent_operator_code_b", "group_operator_code"]),
                                   ["parent_operator_code_b"], 'full').filter(df_enriched.client_id.isNotNull())

    def get_data_traffic(df: DataFrame) -> DataFrame:
        df_4g = df.select(
            ["call_type_key", "roaming_type_key", "client_id", "time_key", "network_type",
             "rounded_data_volume"]).filter(
            (df.call_type_key.isin(["G", "Y", "X"])) & (
                df.roaming_type_key.isin(["X", "R", "H"])) & (
                    df.network_type == "L")).groupBy("client_id", "time_key").agg(
            SparkFunctions.sum("rounded_data_volume").alias("data_4g_mb"))
        df_all = df.select(
            ["call_type_key", "roaming_type_key", "client_id", "time_key", "network_type",
             "rounded_data_volume"]).filter(
            (df.call_type_key.isin(["G", "Y", "X"])) & (
                df.roaming_type_key.isin(["X", "R", "H"]))).groupBy("client_id", "time_key").agg(
            SparkFunctions.sum("rounded_data_volume").alias("data_all_mb"))
        df_3g = df.select(
            ["call_type_key", "roaming_type_key", "client_id", "time_key", "network_type",
             "rounded_data_volume"]).filter(
            (df.call_type_key.isin(["G", "Y", "X"])) & (
                df.roaming_type_key.isin(["X", "R", "H"])) & (
                    df.network_type == "G")).groupBy("client_id", "time_key").agg(
            SparkFunctions.sum("rounded_data_volume").alias("data_3g_mb"))

        df_data_traffic = df_4g.join(df_all, ['client_id', 'time_key'], 'full').join(df_3g, ['client_id', 'time_key'],
                                                                                     'full')

        BYTES_IM_MB = 1024.0 ** 2
        for field in ("data_4g_mb", "data_all_mb", "data_3g_mb"):
            df_data_traffic = df_data_traffic.withColumn(field, SparkFunctions.col(field) / BYTES_IM_MB)

        return df_data_traffic

    def get_custom_column(df: DataFrame, condition: str, column_to_sum: str, custom_name: str) -> DataFrame:
        return df.filter(condition).groupBy("client_id", "time_key").agg(
            SparkFunctions.sum(column_to_sum).alias(custom_name))

    def get_voice_traffic(df: DataFrame) -> DataFrame:
        df_voice_traffic = None
        special_condition = "(call_type_key == 'V') and (call_direction_ind == {}) and (connection_type_key == 3) and (group_operator_code == 'BLN')"
        rows = [("call_direction_ind == 1", "num_of_call", "voice_in_cnt"), (
            special_condition.format(1),
            "num_of_call", "voice_onnet_in_cnt"),
                ("call_direction_ind == 1", "num_of_call", "voice_intercity_in_cnt"), (
                    "(location_type_key == '3') and (call_direction_ind == 1)", "actual_call_dur_sec",
                    "voice_international_in_sec"),
                ("(location_type_key == '3') and (call_direction_ind == 2)", "actual_call_dur_sec",
                 "voice_intercity_out_sec"),
                ("call_direction_ind == 2", "actual_call_dur_sec", "voice_out_sec"),
                (
                "(call_direction_ind == 2) and (charge_amt_rur > 0)", "actual_call_dur_sec", "voice_out_nopackage_sec"),
                (special_condition.format(2), "num_of_call", "voice_onnet_out_cnt"),
                ("(location_type_key == '3') and (call_direction_ind == 2)", "actual_call_dur_sec",
                 "voice_international_out_cnt"), ("call_direction_ind == 2", "num_of_call", "voice_out_cnt"),
                (special_condition.format(2), "actual_call_dur_sec", "voice_onnet_out_sec"),
                ("call_direction_ind == 1", "actual_call_dur_sec", "voice_in_sec"),
                (special_condition.format(1), "actual_call_dur_sec", "voice_onnet_in_sec"),
                ("(location_type_key == '1') and (call_direction_ind == 1)", "actual_call_dur_sec",
                 "voice_intercity_in_sec"),
                ("(location_type_key == '1') and (call_direction_ind == 2)", "num_of_call", "voice_intercity_out_cnt"),
                ("(location_type_key == '3') and (call_direction_ind == 1)", "actual_call_dur_sec",
                 "voice_international_in_cnt"), (
                    "(location_type_key == '3') and (call_direction_ind == 2)", "actual_call_dur_sec",
                    "voice_international_out_sec")]
        for condition, column_to_sum, custom_name in rows:
            one_column = get_custom_column(df, condition, column_to_sum, custom_name)
            if df_voice_traffic is None:
                df_voice_traffic = one_column
            else:
                df_voice_traffic = df_voice_traffic.join(one_column, ['client_id', 'time_key'], 'full')
        return df_voice_traffic

    def get_client_profile(data_traffic: DataFrame, voice_traffic: DataFrame) -> DataFrame:
        merged_frame = data_traffic.join(voice_traffic, ['client_id', 'time_key'], 'full')
        merged_frame = merged_frame.withColumn("time_month", SparkFunctions.col("time_key")[0:7])
        df_client_profile = None
        all_columns = merged_frame.columns
        all_columns.remove("client_id")
        all_columns.remove("time_month")
        all_columns.remove("time_key")
        for column in all_columns:
            one_column = merged_frame.groupBy("client_id", "time_month").agg(
                SparkFunctions.sum(column).alias(column))
            if df_client_profile is None:
                df_client_profile = one_column
            else:
                df_client_profile = df_client_profile.join(one_column, ['client_id', 'time_month'], 'full')
        return df_client_profile

    def get_useful_info(df: DataFrame) -> DataFrame:
        df_useful = df.withColumn("time_month", SparkFunctions.col("time_key")[0:7])
        df_all_data = df_useful.select(
            ["call_type_key", "roaming_type_key", "client_id", "time_month", "network_type",
             "rounded_data_volume"]).filter(
            (df_useful.call_type_key.isin(["G", "Y", "X"])) & (
                df_useful.roaming_type_key.isin(["X", "R", "H"]))).groupBy("client_id", "time_month").agg(
            SparkFunctions.sum("rounded_data_volume").alias("data_all_mb"))
        df_voice_in_sec = df_useful.filter("call_direction_ind == 1").groupBy("client_id", "time_month").agg(
            SparkFunctions.sum("actual_call_dur_sec").alias("voice_in_sec"))
        df_voice_out_sec = df_useful.filter("call_direction_ind == 2").groupBy("client_id", "time_month").agg(
            SparkFunctions.sum("actual_call_dur_sec").alias("voice_out_sec"))

        df_useful = df_all_data.join(df_voice_in_sec, ['client_id', 'time_month'], 'full').join(df_voice_out_sec,
                                                                                                ['client_id',
                                                                                                 'time_month'],
                                                                                                'full')
        return df_useful

    def df_to_dict_by_id(df) -> dict:
        list_rows = map(lambda row: row.asDict(), df.collect())
        dict_persons = {row['client_id']: row for row in list_rows}
        return dict_persons

    # data_traffic = get_data_traffic(df_enriched)
    # voice_traffic = get_voice_traffic(df_enriched)
    # client_profile = get_client_profile(data_traffic, voice_traffic)
    # data_traffic.write.csv("data/data_traffic.csv")
    # voice_traffic.write.csv("data/voice_traffic.csv")
    # client_profile.write.csv("data/client_profile.csv")
    kek = get_useful_info(df_enriched)
    kafka(df_to_dict_by_id(kek))
    # kek.write.parquet("data/eblan.parquet")
    # kek.show()


print(punkt2())
