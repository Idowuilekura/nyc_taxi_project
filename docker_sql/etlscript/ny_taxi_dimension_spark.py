import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("hello").config("spark.master", "local[*]").config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def model(dbt, spark):
    dbt.config(
        schema = "staging"
    )
    df_dim = dbt.source("ny_taxi_raw", "dimension_taxi_table")
    taxi_zone_lookup = dbt.ref("taxi_zone_lookup")

    taxi_zone_lookup = (
    taxi_zone_lookup
    .select(
        taxi_zone_lookup["LocationID"].alias("locationid"),
        taxi_zone_lookup["Borough"].alias("borough"),
        taxi_zone_lookup["Zone"].alias("zone"),
        taxi_zone_lookup["service_zone"].alias("service_zone")
    )
)
    df_dim_with_seed = df_dim.join(taxi_zone_lookup, df_dim.locationid == taxi_zone_lookup.locationid, "inner")
    return df_dim_with_seed
# import dbt_utils

# @dbt_utils.test