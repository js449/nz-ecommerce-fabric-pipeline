# ===============================================================================
# SOURCE 2: NZTA TRANSIT API INGESTION & SCHEMA ENFORCEMENT
# Strategy: Pure PySpark Execution for Fabric Notebook
# Target: Lakehouse Silver Delta Table (silver_nzta_road_incidents)
# ===============================================================================

import urllib.request
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql.functions import col, from_json, explode, current_timestamp

# -------------------------------------------------------------------------------
# 1. DEFINE STRICT PYSPARK SCHEMA CONTRACT
# -------------------------------------------------------------------------------
incident_schema = StructType([
    StructField("id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("region", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("description", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("created_at", StringType(), True)
])

api_response_schema = StructType([
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("incidents", ArrayType(incident_schema), True)
])

# -------------------------------------------------------------------------------
# 2. FETCH RAW JSON FROM API & CONVERT TO SPARK DATAFRAME
# -------------------------------------------------------------------------------
# Mock payload representing the raw REST API JSON response structure
mock_json_response = """
{
  "status": "success",
  "timestamp": "2026-09-06T00:00:00Z",
  "incidents": [
    {
      "id": "NZTA-2026-8812",
      "location": "SH1 Desert Road",
      "region": "Waikato",
      "severity": "Severe",
      "description": "Heavy snowfall and ice causing road closure between Rangipo and Waiouru.",
      "latitude": -39.281,
      "longitude": 175.742,
      "created_at": "2026-09-05T21:30:00Z"
    },
    {
      "id": "NZTA-2026-9041",
      "location": "SH7 Lewis Pass",
      "region": "Canterbury",
      "severity": "Moderate",
      "description": "Single-lane traffic due to minor slip clearing operations.",
      "latitude": -42.378,
      "longitude": 172.401,
      "created_at": "2026-09-05T23:00:00Z"
    }
  ]
}
"""

# Read raw JSON payload into a single-row Spark DataFrame
raw_df = spark.read.json(sc.parallelize([mock_json_response]))

# -------------------------------------------------------------------------------
# 3. ENFORCE SCHEMA & FLATTEN NESTED INCIDENTS ARRAY
# -------------------------------------------------------------------------------
silver_api_df = raw_df \
    .select(from_json(col("value"), api_response_schema).alias("data")) \
    .select(explode(col("data.incidents")).alias("incident")) \
    .select(
        col("incident.id").alias("incident_id"),
        col("incident.location").alias("location_name"),
        col("incident.region").alias("region"),
        col("incident.severity").alias("impact_level"),
        col("incident.description").alias("description"),
        col("incident.latitude").alias("latitude"),
        col("incident.longitude").alias("longitude"),
        col("incident.created_at").cast("timestamp").alias("alert_created_at"),
        current_timestamp().alias("ingested_at")
    )

# -------------------------------------------------------------------------------
# 4. WRITE TO LAKEHOUSE SILVER DELTA TABLE WITH V-ORDER OPTIMIZATION
# -------------------------------------------------------------------------------
silver_api_df.write \
    .format("delta") \
    .mode("append") \
    .option("spark.sql.parquet.vorder.enabled", "true") \
    .saveAsTable("silver_nzta_road_incidents")

print("NZTA API ingestion completed successfully using Pure PySpark!")