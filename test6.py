import sys
import os
import re
from datetime import datetime
from urllib.parse import urlparse
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, when, expr
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType, DecimalType

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum

# ------------------ Arguments ------------------
required_args = [
    'JOB_NAME',
    'spark_warehouse',
    'target_database',
    'input_path',
    'input_file_name',
    'target_table',
    'load_type',
    'log_bucket'
]

optional_args = ['year']

args = getResolvedOptions(sys.argv, required_args + optional_args)

# ------------------ Spark Session ------------------
spark = SparkSession.builder \
    .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog') \
    .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO') \
    .config('spark.sql.catalog.glue_catalog.warehouse', args['spark_warehouse']) \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)

LOG_BUCKET = args["log_bucket"]

# ------------------ Logging ------------------
def write_log(table_name, message):
    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_table = re.sub(r'[^A-Za-z0-9_]+', '_', table_name)
    log_key = f"logs/{safe_table}_{timestamp}.txt"

    resp = s3.list_objects_v2(Bucket=LOG_BUCKET, Prefix="logs/", MaxKeys=1)
    if 'Contents' not in resp:
        s3.put_object(Bucket=LOG_BUCKET, Key="logs/", Body=b'')

    s3.put_object(
        Bucket=LOG_BUCKET,
        Key=log_key,
        Body=message.encode('utf-8')
    )

# ------------------ Table Creation ------------------
def create_table_if_not_exists(database, table):
    full_table = f"glue_catalog.{database}.{table}"
    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        return
    except:
        pass

    schema = StructType([
        StructField("state", StringType(), True),
        StructField("CLUB_N", IntegerType(), True),
        StructField("PRODUCT_TYPE", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("BRANCH_N", IntegerType(), True),
        StructField("YEAR_N", IntegerType(), True),
        StructField("eff_d", DateType(), True),
        StructField("QUARTER_N", StringType(), True),
        StructField("month_num", IntegerType(), True),
        StructField("GOAL_N", DecimalType(9, 2), True),
        StructField("Yearly_Goal_Total", DecimalType(9, 2), True),
        StructField("YTD_GOAL_TOTAL", DecimalType(9, 2), True),
        StructField("Quarterly_Goal_Total", DecimalType(9, 2), True),
    ])

    spark.createDataFrame([], schema) \
        .writeTo(full_table) \
        .partitionedBy("YEAR_N", "QUARTER_N") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.compression-codec", "zstd") \
        .create()

# ------------------ Read + Transform ------------------
def read_and_clean_csv(path):
    df = spark.read.option("header", True).csv(path)

    cols = [c.strip().replace('"', '') for c in df.columns]
    df = df.toDF(*cols)

    for c in df.columns:
        df = df.withColumn(
            c,
            when(df[c].cast(StringType()).isNotNull(),
                 regexp_replace(trim(col(c)), '"', ''))
            .otherwise(col(c))
        )

    # Casting
    df = df \
        .withColumn("state", col("state").cast(StringType())) \
        .withColumn("CLUB_N", col("CLUB_N").cast("int")) \
        .withColumn("BRANCH_N", col("BRANCH_N").cast("int")) \
        .withColumn("YEAR_N", col("YEAR_N").cast("int")) \
        .withColumn("month_num", col("month_num").cast("int")) \
        .withColumn("GOAL_N", col("GOAL_N").cast("decimal(9,2)"))

    # eff_d
    df = df.withColumn(
        "eff_d",
        expr("make_date(YEAR_N, month_num, 1)")
    )

    # Quarter
    df = df.withColumn(
        "QUARTER_N",
        expr("""
            CASE 
                WHEN month_num BETWEEN 1 AND 3 THEN '1'
                WHEN month_num BETWEEN 4 AND 6 THEN '2'
                WHEN month_num BETWEEN 7 AND 9 THEN '3'
                WHEN month_num BETWEEN 10 AND 12 THEN '4'
            END
        """)
    )

    # ------------------ Windows ------------------

    yearly_goal_window = Window.partitionBy(
        "state", "CLUB_N", "PRODUCT_TYPE", "Region", "BRANCH_N", "YEAR_N"
    )

    ytd_goal_window = Window.partitionBy(
        "state", "CLUB_N", "PRODUCT_TYPE", "Region", "BRANCH_N", "YEAR_N"
    ).orderBy("month_num").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    quarterly_goal_window = Window.partitionBy(
        "state", "CLUB_N", "PRODUCT_TYPE", "Region", "BRANCH_N", "YEAR_N", "QUARTER_N"
    )

    # Aggregations
    df = df \
        .withColumn("Yearly_Goal_Total", _sum("GOAL_N").over(yearly_goal_window)) \
        .withColumn("YTD_GOAL_TOTAL", _sum("GOAL_N").over(ytd_goal_window)) \
        .withColumn("Quarterly_Goal_Total", _sum("GOAL_N").over(quarterly_goal_window))

    return df

# ------------------ Load Helpers ------------------
def truncate_table(table):
    spark.sql(f"DELETE FROM {table}")

def delete_year_records(table, y):
    spark.sql(f"DELETE FROM {table} WHERE YEAR_N = {y}")

def merge_into_table(df, table):
    df.createOrReplaceTempView("staging_data")

    spark.sql(f"""
        MERGE INTO {table} t
        USING staging_data s
        ON t.CLUB_N = s.CLUB_N
        AND t.BRANCH_N = s.BRANCH_N
        AND t.YEAR_N = s.YEAR_N
        AND t.month_num = s.month_num
        AND t.state = s.state
        AND t.PRODUCT_TYPE = s.PRODUCT_TYPE
        AND t.Region = s.Region
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# ------------------ Main ------------------
def main():
    try:
        input_path = args['input_path'].rstrip('/') + "/" + args['input_file_name']
        table = args['target_table']
        load_type = args['load_type'].lower()
        year_arg = args.get("year")

        iceberg_table = f"glue_catalog.{args['target_database']}.{table}"

        create_table_if_not_exists(args['target_database'], table)

        df = read_and_clean_csv(input_path)

        if load_type == "full":
            truncate_table(iceberg_table)
            df.writeTo(iceberg_table).append()

        elif load_type == "incremental":

            if not year_arg or str(year_arg).strip().upper() == "NA":
                merge_into_table(df, iceberg_table)
            else:
                yr = int(year_arg)
                delete_year_records(iceberg_table, yr)
                df.filter(col("YEAR_N") == yr).writeTo(iceberg_table).append()

        else:
            raise ValueError("Invalid load_type")

    except Exception as e:
        write_log(table, f"Job failed: {e}")
        raise

if __name__ == "__main__":
    main()