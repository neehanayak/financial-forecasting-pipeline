"""
data_loader.py

This module is responsible for:
1. Creating a Dataproc Serverless Spark session
2. Loading raw asset price data from GCS CSV
3. Writing cleaned data into an Iceberg table
4. Reading data back from the Iceberg table
"""

from __future__ import annotations

from google.cloud.dataproc_v1 import EnvironmentConfig, ExecutionConfig, Session
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from pyspark.sql import DataFrame, SparkSession


def create_serverless_spark_session(
    project_id: str,
    region: str,
    service_account: str,
    iceberg_catalog: str,
    iceberg_warehouse: str,
    subnet_uri: str | None = None,
) -> SparkSession:
    """
    Create a Dataproc Serverless Spark session with Iceberg catalog settings.
    """
    session = Session()

    session.runtime_config = {
        "version": "3.0",
        "properties": {
            "spark.driver.cores": "4",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
            "spark.executor.cores": "4",
            "spark.executor.memory": "4g",
            "spark.executor.memoryOverhead": "1g",
            "spark.executor.instances": "2",
            "spark.dataproc.engine": "default",
            f"spark.sql.catalog.{iceberg_catalog}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{iceberg_catalog}.type": "hadoop",
            f"spark.sql.catalog.{iceberg_catalog}.warehouse": iceberg_warehouse,
        },
    }

    execution_config = ExecutionConfig()
    execution_config.service_account = service_account

    if subnet_uri:
        execution_config.subnetwork_uri = subnet_uri

    environment_config = EnvironmentConfig()
    environment_config.execution_config = execution_config
    session.environment_config = environment_config

    spark = (
        DataprocSparkSession.builder
        .projectId(project_id)
        .location(region)
        .dataprocSessionConfig(session)
        .getOrCreate()
    )

    print("Serverless Spark session created successfully.")
    return spark


def load_asset_prices_from_gcs_csv(
    spark: SparkSession,
    csv_file_path: str,
) -> DataFrame:
    """
    Load raw asset price data from a CSV file in GCS.
    """
    print(f"Reading CSV data from: {csv_file_path}")

    raw_asset_price_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csv_file_path)
    )

    return raw_asset_price_df


def write_cleaned_data_to_iceberg(
    cleaned_asset_price_df: DataFrame,
    iceberg_catalog: str,
    iceberg_schema: str,
    iceberg_table: str,
) -> None:
    """
    Write cleaned Spark DataFrame into an Iceberg table.
    """
    fully_qualified_table_name = f"{iceberg_catalog}.{iceberg_schema}.{iceberg_table}"

    print(f"Writing cleaned data to Iceberg table: {fully_qualified_table_name}")

    cleaned_asset_price_df.writeTo(fully_qualified_table_name).using("iceberg").createOrReplace()

    print("Iceberg write completed successfully.")


def read_asset_prices_from_iceberg(
    spark: SparkSession,
    iceberg_catalog: str,
    iceberg_schema: str,
    iceberg_table: str,
) -> DataFrame:
    """
    Read asset price data back from an Iceberg table.
    """
    fully_qualified_table_name = f"{iceberg_catalog}.{iceberg_schema}.{iceberg_table}"

    print(f"Reading from Iceberg table: {fully_qualified_table_name}")

    iceberg_asset_price_df = spark.read.table(fully_qualified_table_name)

    return iceberg_asset_price_df
