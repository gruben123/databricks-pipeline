import dlt
from pyspark.sql.functions import col, to_date, sum, avg, desc

# you do not call each of the python methods individually
# the databricks DLT UI runtime engine that triggers the functions
# It reads your file.
# Then it maps out the dependencies based on your dlt.read() or dlt.read_stream() statements.

#=============================================================
#BRONZE LAYER: Data Ingestion
#=============================================================
#We ingest data directly from the Unity Catalog table/location

#dlt.table() creates a streaming table or materialize view from Python dlt=delta live table
@dlt.table(name="walmart_bronze", comment="Raw Walmart sales dataset ingested directly from the workspace")
def walmart_bronze():
    #reading as a stream is a best practice for DLT to process new data incrementally
    #significantly improves performance and fewer compute costs over time
    return spark.readStream.table("workspace.walmart.walmart")


#===============================================================
#SILVER LAYER
#===============================================================
#Here, we apply data quality expectations, fix data types, and drop metadata columns
@dlt.table(
    name="walmart_silver",
    comment="Silver table: Cleansed Walmart data with proper data types and quality checks"
)
#DLT Expectation (Data quality checks)
@dlt.expect_or_drop("valid_sales", "Weekly_Sales > 0") #drop rows with negative or zero sales
@dlt.expect("valid_store_id", "Store is not null") #just flag rows if missing store id
def walmart_silver():
    return(
        dlt.read_stream("walmart_bronze")
        .withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))  #cast the Date to an actual Date type
        .select("Store", "Date", "Weekly_Sales", "Holiday_Flag", "Temperature", "Fuel_Price", "CPI", "Unemployment","temp_category")
        #this function is exclusively for delta live tables different from spark.readStream.table()
    )

#===============================================================
#GOLD LAYER
#===============================================================
#Creating a summarized view that a BI tool or business analyst would use
@dlt.table(
    name="walmart_gold_store_performance",
    comment="Gold table: Aggregated total sales and average temperature per store by weather category"
)
def store_sales_summary():
    #Gold tables often read as static batches rather than streams to compute full aggregation
    return(
        dlt.read("walmart_silver")  #instead of read_stream because gold layer does full aggregations rather than incremental transformations and will make it a materialized view
        .groupBy("Store", "temp_category")
        .agg(sum("Weekly_Sales").alias("Total_Sales"), avg("Temperature").alias("Avg_Temperature"))
        .orderBy(desc("Total_Sales"))
    )