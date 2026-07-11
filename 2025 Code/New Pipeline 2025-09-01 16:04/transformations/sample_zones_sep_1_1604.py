import dlt
from pyspark.sql.functions import col, sum


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dlt.table
def sample_zones_sep_1_1604():
    # Read from the "sample_trips" table, then sum all the fares
    return (
        spark.read.table("sample_trips_sep_1_1604")
        .groupBy(col("pickup_zip"))
        .agg(
            sum("fare_amount").alias("total_fare")
        )
    )
