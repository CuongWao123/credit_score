
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import json
from typing import Dict, Any
from datetime import datetime
import logging

# =========================
# Configuration
# =========================
class Config:
    """Centralized configuration"""
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = "broker-1:19092,broker-2:19092"
    KAFKA_TOPIC = "dbz.public.applications"
    STARTING_OFFSETS = "latest"  
    MAX_OFFSETS_PER_TRIGGER = "50"
    
    # Bureau API settings
    BUREAU_API_URL = "http://bureau-api:6666"
    BUREAU_API_TIMEOUT = 60
    
    # MinIO/S3A settings
    MINIO_ENDPOINT = "minio:9000"
    MINIO_ACCESS_KEY = "minioAccessKey"
    MINIO_SECRET_KEY = "minioSecretKey"
    MINIO_BUCKET = "applications-bureau-data"
    
    # Separate tables for better performance
    APPLICATIONS_PATH = f"s3a://{MINIO_BUCKET}/applications"
    BUREAU_CREDITS_PATH = f"s3a://{MINIO_BUCKET}/bureau_credits"
    CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints/separate_tables"
    
    # Schema file
    SCHEMA_FILE = "/opt/spark/work-dir/schema.json"
    
    # Processing settings
    TRIGGER_INTERVAL = "120 seconds"
    PARTITION_COLUMN = "processing_date"
    
    # Spark settings
    APP_NAME = "Kafka-Bureau-MinIO-Pipeline"
    LOG_LEVEL = "INFO"


# =========================
# Logging Setup
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =========================
# Schema Loader
# =========================
class SchemaLoader:
    """Load and parse Debezium CDC schema from JSON"""
    
    @staticmethod
    def load_from_json(json_file_path: str) -> StructType:
        """Load Spark schema from JSON file"""
        try:
            with open(json_file_path, 'r') as f:
                schema_json = json.load(f)
            return SchemaLoader._parse_schema(schema_json)
        except Exception as e:
            logger.error(f"Failed to load schema from {json_file_path}: {e}")
            raise
    
    @staticmethod
    def _parse_schema(schema_dict: dict):
        """Recursively parse JSON schema to Spark StructType"""
        if isinstance(schema_dict, dict):
            schema_type = schema_dict.get("type")
            
            if schema_type == "struct":
                fields = []
                for field in schema_dict.get("fields", []):
                    field_name = field["name"]
                    field_type = (SchemaLoader._parse_schema(field["type"]) 
                                 if isinstance(field["type"], dict) 
                                 else SchemaLoader._get_spark_type(field["type"]))
                    field_nullable = field.get("nullable", True)
                    fields.append(StructField(field_name, field_type, field_nullable))
                return StructType(fields)
            
            elif schema_type == "array":
                element_type = SchemaLoader._parse_schema(schema_dict["elementType"])
                return ArrayType(element_type)
            
            elif schema_type == "map":
                key_type = SchemaLoader._get_spark_type(schema_dict["keyType"])
                value_type = SchemaLoader._parse_schema(schema_dict["valueType"])
                return MapType(key_type, value_type)
            
            else:
                return SchemaLoader._get_spark_type(schema_type)
        
        return SchemaLoader._get_spark_type(schema_dict)
    
    @staticmethod
    def _get_spark_type(type_str: str) -> DataType:
        """Convert string type to Spark DataType"""
        type_mapping = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "byte": ByteType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "binary": BinaryType()
        }
        return type_mapping.get(str(type_str).lower(), StringType())
    
    @staticmethod
    def get_bureau_credit_schema() -> ArrayType:
        """Define schema for bureau credits array from API"""
        return ArrayType(
            StructType([
                StructField("reco_id_curr", IntegerType(), True),
                StructField("reco_bureau_id", IntegerType(), True),
                StructField("credit_status", StringType(), True),
                StructField("credit_currency", StringType(), True),
                StructField("days_credit", IntegerType(), True),
                StructField("credit_day_overdue", IntegerType(), True),
                StructField("days_credit_enddate", IntegerType(), True),
                StructField("days_enddate_fact", IntegerType(), True),
                StructField("credit_limit_max_overdue", DoubleType(), True),
                StructField("credit_prolong_count", IntegerType(), True),
                StructField("credit_sum", DoubleType(), True),
                StructField("credit_sum_debt", DoubleType(), True),
                StructField("credit_sum_limit", DoubleType(), True),
                StructField("credit_sum_overdue", DoubleType(), True),
                StructField("credit_type", StringType(), True),
                StructField("days_credit_update", IntegerType(), True),
                StructField("annuity_payment", DoubleType(), True),
                StructField("months_balance", IntegerType(), True),
                StructField("stat_for_bureau", StringType(), True)
            ])
        )


# =========================
# Bureau API Client
# =========================
class BureauAPIClient:
    """Client for Bureau Credit API"""
    
    @staticmethod
    def get_bureau_data(application_id: int) -> Dict[str, Any]:
        """
        Fetch bureau credits from API with complete metadata
        
        Args:
            application_id: Application ID (reco_id_curr)
            
        Returns:
            Dict with bureau_credits array and API metadata
        """
        import time
        start_time = time.time()
        
        if application_id is None:
            return {
                "bureau_credits": [],
                "bureau_count": 0,
                "api_status": "invalid_id",
                "api_http_code": 0,
                "api_response_time_ms": 0,
                "api_url": None,
                "api_timestamp": int(time.time() * 1000)
            }
        
        url = f"{Config.BUREAU_API_URL}/bureau/{application_id}"
        
        try:
            response = requests.get(
                url, 
                timeout=(10, Config.BUREAU_API_TIMEOUT),
                headers={'Connection': 'close'}
            )
            response_time_ms = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                bureau_data = response.json()
                credits = bureau_data.get("data", []) or []
                logger.info(f"✅ API Success for ID {application_id}: {len(credits)} credits, {response_time_ms}ms")
                return {
                    "bureau_credits": credits,
                    "bureau_count": len(credits),
                    "api_status": "success",
                    "api_http_code": 200,
                    "api_response_time_ms": response_time_ms,
                    "api_url": url,
                    "api_timestamp": int(time.time() * 1000)
                }
            
            elif response.status_code == 404:
                logger.info(f"⚠️  API 404 for ID {application_id}: No bureau data, {response_time_ms}ms")
                return {
                    "bureau_credits": [],
                    "bureau_count": 0,
                    "api_status": "not_found",
                    "api_http_code": 404,
                    "api_response_time_ms": response_time_ms,
                    "api_url": url,
                    "api_timestamp": int(time.time() * 1000)
                }
            
            else:
                logger.warning(f"❌ API {response.status_code} for ID {application_id}, {response_time_ms}ms")
                return {
                    "bureau_credits": [],
                    "bureau_count": 0,
                    "api_status": f"http_error_{response.status_code}",
                    "api_http_code": response.status_code,
                    "api_response_time_ms": response_time_ms,
                    "api_url": url,
                    "api_timestamp": int(time.time() * 1000)
                }
        
        except requests.exceptions.Timeout as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"⏱️  TIMEOUT for ID {application_id} after {response_time_ms}ms")
            return {
                "bureau_credits": [],
                "bureau_count": 0,
                "api_status": "timeout",
                "api_http_code": 0,
                "api_response_time_ms": response_time_ms,
                "api_url": url,
                "api_timestamp": int(time.time() * 1000)
            }
        
        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"❌ ERROR for ID {application_id}: {str(e)}")
            return {
                "bureau_credits": [],
                "bureau_count": 0,
                "api_status": "error",
                "api_http_code": 0,
                "api_response_time_ms": response_time_ms,
                "api_url": url,
                "api_timestamp": int(time.time() * 1000)
            }


# =========================
# Spark Session Factory
# =========================
class SparkSessionFactory:
    """Factory for creating configured Spark sessions"""
    
    @staticmethod
    def create() -> SparkSession:
        """Create Spark session with MinIO/S3A configuration"""
        spark = (SparkSession.builder
                 .appName(Config.APP_NAME)
                 .config("spark.jars.packages", ",".join([
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                     "org.apache.hadoop:hadoop-aws:3.3.4",
                     "com.amazonaws:aws-java-sdk-bundle:1.12.367"
                 ]))
                 # MinIO/S3A configuration
                 .config("spark.hadoop.fs.s3a.endpoint", f"http://{Config.MINIO_ENDPOINT}")
                 .config("spark.hadoop.fs.s3a.access.key", Config.MINIO_ACCESS_KEY)
                 .config("spark.hadoop.fs.s3a.secret.key", Config.MINIO_SECRET_KEY)
                 .config("spark.hadoop.fs.s3a.path.style.access", "true")
                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                 .getOrCreate())
        
        spark.sparkContext.setLogLevel(Config.LOG_LEVEL)
        logger.info(f"✅ Spark session created: {Config.APP_NAME}")
        return spark


# =========================
# Step 1: Read from Kafka
# =========================
class KafkaReader:
    """Read streaming data from Kafka"""
    
    @staticmethod
    def read_stream(spark: SparkSession) -> DataFrame:
        """Read and parse Debezium CDC events from Kafka"""
        logger.info(f"📥 Step 1: Reading from Kafka topic: {Config.KAFKA_TOPIC}")
        
        raw_df = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS)
                  .option("subscribe", Config.KAFKA_TOPIC)
                  .option("startingOffsets", Config.STARTING_OFFSETS)
                  .option("maxOffsetsPerTrigger", Config.MAX_OFFSETS_PER_TRIGGER)
                  .option("failOnDataLoss", "false")
                  .load())
        
        kv_df = raw_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS kafka_value",
            "timestamp AS kafka_timestamp",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset"
        )
        
        logger.info(f"Loading schema from: {Config.SCHEMA_FILE}")
        debezium_schema = SchemaLoader.load_from_json(Config.SCHEMA_FILE)
        
        parsed_df = (kv_df
                     .withColumn("data", from_json(col("kafka_value"), debezium_schema))
                     .select(
                         "data.payload.after.*",
                         col("data.payload.op").alias("cdc_operation"),
                         col("data.payload.ts_ms").alias("cdc_timestamp_ms"),
                         col("data.payload.source.db").alias("source_database"),
                         col("data.payload.source.schema").alias("source_schema"),
                         col("data.payload.source.table").alias("source_table"),
                         col("kafka_key"),
                         col("kafka_timestamp").cast(TimestampType()).alias("kafka_timestamp"),
                         col("kafka_topic"),
                         col("kafka_partition"),
                         col("kafka_offset")
                     )
                     .filter(col("cdc_operation").isin("c", "r", "u")))
        
        logger.info("✅ Step 1 Complete: Kafka data parsed")
        return parsed_df


# =========================
# Step 2: Call Bureau API & Create Bureau DataFrame
# =========================
class BureauEnricher:
    """Enrich application data with bureau credits"""
    
    @staticmethod
    def enrich(df: DataFrame) -> DataFrame:
        """
        Call Bureau API and add bureau data to applications DataFrame
        
        Args:
            df: DataFrame with application data from Kafka
            
        Returns:
            DataFrame with bureau credits array added
        """
        logger.info("📞 Step 2: Calling Bureau API for all applications...")
        
        # Define UDF to get bureau data
        @udf(returnType=StructType([
            StructField("bureau_credits", SchemaLoader.get_bureau_credit_schema(), True),
            StructField("bureau_count", IntegerType(), True),
            StructField("api_status", StringType(), True),
            StructField("api_http_code", IntegerType(), True),
            StructField("api_response_time_ms", IntegerType(), True),
            StructField("api_url", StringType(), True),
            StructField("api_timestamp", LongType(), True)
        ]))
        def get_bureau_credits_udf(application_id):
            """UDF wrapper for Bureau API client"""
            try:
                if application_id is None:
                    return ([], 0, "null_id", 0, 0, None, 0)
                
                result = BureauAPIClient.get_bureau_data(int(application_id))
                return (
                    result.get("bureau_credits", []),
                    result.get("bureau_count", 0),
                    result.get("api_status", "unknown"),
                    result.get("api_http_code", 0),
                    result.get("api_response_time_ms", 0),
                    result.get("api_url", None),
                    result.get("api_timestamp", 0)
                )
            except Exception as e:
                logger.error(f"UDF error for ID {application_id}: {str(e)}")
                import time
                return ([], 0, "udf_error", 0, 0, None, int(time.time() * 1000))
        
        # Call API and get bureau data
        enriched_df = (df
                      .withColumn("bureau_api_response", get_bureau_credits_udf(col("reco_id_curr")))
                      .select(
                          col("*"),
                          col("bureau_api_response.bureau_credits").alias("bureau_credits_array"),
                          col("bureau_api_response.bureau_count").alias("bureau_count"),
                          col("bureau_api_response.api_status").alias("bureau_api_status"),
                          col("bureau_api_response.api_http_code").alias("bureau_api_http_code"),
                          col("bureau_api_response.api_response_time_ms").alias("bureau_api_response_time_ms"),
                          col("bureau_api_response.api_url").alias("bureau_api_url"),
                          col("bureau_api_response.api_timestamp").alias("bureau_api_timestamp")
                      )
                      .drop("bureau_api_response"))
        
        logger.info("✅ Step 2 Complete: Bureau API calls completed")
        return enriched_df
    
    @staticmethod
    def create_bureau_df(enriched_df: DataFrame) -> DataFrame:
        """
        Create separate Bureau Credits DataFrame from enriched applications
        Explodes bureau_credits_array to create 1 row per credit
        
        Args:
            enriched_df: Applications DataFrame with bureau_credits_array
            
        Returns:
            Bureau Credits DataFrame (exploded)
        """
        logger.info("📊 Creating separate Bureau DataFrame...")
        
        bureau_df = (enriched_df
                    .filter(col("bureau_count") > 0)  # Only records with credits
                    .select(
                        col("reco_id_curr"),
                        explode(col("bureau_credits_array")).alias("credit")
                    )
                    .select(
                        col("reco_id_curr"),
                        col("credit.reco_bureau_id").alias("reco_bureau_id"),
                        col("credit.credit_status").alias("credit_status"),
                        col("credit.credit_currency").alias("credit_currency"),
                        col("credit.days_credit").alias("days_credit"),
                        col("credit.credit_day_overdue").alias("credit_day_overdue"),
                        col("credit.days_credit_enddate").alias("days_credit_enddate"),
                        col("credit.days_enddate_fact").alias("days_enddate_fact"),
                        col("credit.credit_limit_max_overdue").alias("credit_limit_max_overdue"),
                        col("credit.credit_prolong_count").alias("credit_prolong_count"),
                        col("credit.credit_sum").alias("credit_sum"),
                        col("credit.credit_sum_debt").alias("credit_sum_debt"),
                        col("credit.credit_sum_limit").alias("credit_sum_limit"),
                        col("credit.credit_sum_overdue").alias("credit_sum_overdue"),
                        col("credit.credit_type").alias("credit_type"),
                        col("credit.days_credit_update").alias("days_credit_update"),
                        col("credit.annuity_payment").alias("bureau_annuity_payment"),
                        col("credit.months_balance").alias("months_balance"),
                        col("credit.stat_for_bureau").alias("stat_for_bureau")
                    ))
        
        logger.info("✅ Bureau DataFrame created (exploded)")
        return bureau_df


# =========================
# Step 3: Write to MinIO (Separate Tables)
# =========================
class MinIOWriter:
    """Write applications and bureau credits as separate tables"""
    
    @staticmethod
    def write_separate_tables(enriched_df: DataFrame) -> object:
        """
        Write kafka_df and api_df as SEPARATE tables for better performance
        
        Output structure:
        - applications/     : 1 row per application (from Kafka + API metadata)
        - bureau_credits/   : N rows per application (exploded bureau credits)
        
        JOIN later in query engine (DuckDB/Trino/Spark SQL) for better performance!
        
        Args:
            enriched_df: Applications with bureau_credits_array from API
            
        Returns:
            Streaming query object
        """
        logger.info(f"💾 Step 3: Writing separate tables to MinIO")
        logger.info(f"   📁 Applications → {Config.APPLICATIONS_PATH}")
        logger.info(f"   📁 Bureau Credits → {Config.BUREAU_CREDITS_PATH}")
        
        def process_batch(batch_df, batch_id):
            """
            Process each micro-batch:
            1. Write applications table (kafka_df)
            2. Write bureau_credits table (api_df)
            NO JOIN in Spark!
            """
            import sys
            
            logger.info(f"\n{'='*100}")
            logger.info(f"📦 Processing Batch #{batch_id}")
            logger.info(f"{'='*100}")
            sys.stdout.flush()
            
            if batch_df.isEmpty():
                logger.info("⏳ Empty batch, waiting for data...")
                return
            
            # Cache input batch
            batch_df.persist()
            
            # ========================================
            # TABLE 1: Applications
            # ========================================
            logger.info("🔄 Table 1: Writing applications...")
            sys.stdout.flush()
            
            applications_df = (batch_df
                              .drop("bureau_credits_array")  # Remove array
                              .withColumn("processing_timestamp", current_timestamp())
                              .withColumn("processing_date", current_date())
                              .withColumn("year", year(col("processing_date")))
                              .withColumn("month", month(col("processing_date")))
                              .withColumn("day", dayofmonth(col("processing_date")))
                              .withColumn("data_quality_score",
                                         when(col("bureau_api_status") == "success", 100)
                                         .when(col("bureau_api_status") == "not_found", 80)
                                         .otherwise(0)))
            
            applications_df.persist()
            app_count = applications_df.count()
            
            # Write applications table
            (applications_df.write
             .format("parquet")
             .mode("append")
             .partitionBy("year", "month", "day")
             .save(Config.APPLICATIONS_PATH))
            
            logger.info(f"✅ Applications table: {app_count} rows written")
            sys.stdout.flush()
            
            # Show sample
            logger.info("\n📊 Sample applications data:")
            applications_df.select(
                "reco_id_curr",
                "target",
                "income",
                "bureau_count",
                "bureau_api_status",
                "bureau_api_response_time_ms"
            ).show(5, truncate=False)
            sys.stdout.flush()
            
            # ========================================
            # TABLE 2: Bureau Credits
            # ========================================
            logger.info("🔄 Table 2: Writing bureau_credits...")
            sys.stdout.flush()
            
            bureau_df = BureauEnricher.create_bureau_df(batch_df)
            
            if not bureau_df.rdd.isEmpty():
                bureau_enriched = (bureau_df
                                  .withColumn("processing_timestamp", current_timestamp())
                                  .withColumn("processing_date", current_date())
                                  .withColumn("year", year(col("processing_date")))
                                  .withColumn("month", month(col("processing_date")))
                                  .withColumn("day", dayofmonth(col("processing_date"))))
                
                bureau_enriched.persist()
                bureau_count = bureau_enriched.count()
                
                # Write bureau_credits table
                (bureau_enriched.write
                 .format("parquet")
                 .mode("append")
                 .partitionBy("year", "month", "day")
                 .save(Config.BUREAU_CREDITS_PATH))
                
                logger.info(f"✅ Bureau credits table: {bureau_count} rows written")
                sys.stdout.flush()
                
                # Show sample
                logger.info("\n📊 Sample bureau_credits data:")
                bureau_enriched.select(
                    "reco_id_curr",
                    "reco_bureau_id",
                    "credit_sum",
                    "credit_currency",
                    "credit_status",
                    "days_credit"
                ).show(5, truncate=False)
                sys.stdout.flush()
                
                bureau_enriched.unpersist()
            else:
                bureau_count = 0
                logger.info(f"⚠️  No bureau credits in this batch")
                sys.stdout.flush()
            
            # ========================================
            # Summary
            # ========================================
            logger.info(f"\n{'='*100}")
            logger.info(f"✅ Batch #{batch_id} Summary:")
            logger.info(f"   📊 Applications: {app_count} rows")
            logger.info(f"   📊 Bureau Credits: {bureau_count} rows")
            logger.info(f"   💡 Ratio: {bureau_count/app_count if app_count > 0 else 0:.2f} credits per application")
            logger.info(f"{'='*100}\n")
            sys.stdout.flush()
            
            # Cleanup
            batch_df.unpersist()
            applications_df.unpersist()
        
        # Start streaming
        query = (enriched_df.writeStream
                 .foreachBatch(process_batch)
                 .option("checkpointLocation", Config.CHECKPOINT_PATH)
                 .trigger(processingTime=Config.TRIGGER_INTERVAL)
                 .start())
        
        logger.info("✅ Step 3 Complete: Streaming to separate tables (NO JOIN)")
        return query


# =========================
# Monitoring
# =========================
class StreamMonitor:
    """Monitor streaming query progress"""
    
    @staticmethod
    def log_progress(query):
        """Log streaming query progress"""
        import time
        
        print("\n" + "=" * 100)
        print("📊 Streaming Query Status")
        print("=" * 100)
        print(f"Query ID: {query.id}")
        print(f"Status: ACTIVE")
        print("\n💡 Monitoring progress (Ctrl+C to stop)...")
        print("=" * 100 + "\n")
        
        try:
            while query.isActive:
                time.sleep(10)
                progress = query.lastProgress
                
                if progress:
                    num_input_rows = progress.get('numInputRows', 0)
                    batch_id = progress.get('batchId', 0)
                    
                    if num_input_rows > 0:
                        print(f"📈 Batch #{batch_id} | Rows: {num_input_rows} | "
                              f"Time: {datetime.now().strftime('%H:%M:%S')}")
                else:
                    print("⏳ Waiting for data from Kafka...")
                    
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping streaming query...")
            query.stop()
            print("✅ Query stopped")


# =========================
# Main Pipeline
# =========================
class ETLPipeline:
    """Main ETL Pipeline"""
    
    def __init__(self):
        self.spark = None
        self.query = None
    
    def run(self):
        """Execute the complete ETL pipeline"""
        try:
            print("\n" + "=" * 100)
            print("🚀 Starting Kafka → Bureau API → MinIO ETL Pipeline (SEPARATE TABLES)")
            print("=" * 100)
            print("\nPipeline Steps:")
            print("  1. Read from Kafka (Debezium CDC)")
            print("  2. Call Bureau API to enrich applications")
            print("  3. Write to MinIO as 2 separate tables:")
            print("     📁 applications/     - 1 row per application")
            print("     📁 bureau_credits/   - N rows (exploded credits)")
            print("\n💡 Benefits:")
            print("  ✅ 3-5x faster (no JOIN overhead in Spark)")
            print("  ✅ JOIN later in query engine for flexibility")
            print("  ✅ Each table optimized independently")
            print("=" * 100 + "\n")
            
            self.spark = SparkSessionFactory.create()
            
            # Step 1: Read from Kafka
            kafka_df = KafkaReader.read_stream(self.spark)
            
            # Step 2: Call Bureau API and enrich applications
            enriched_df = BureauEnricher.enrich(kafka_df)
            
            # Step 3: Write to separate tables (NO JOIN)
            self.query = MinIOWriter.write_separate_tables(enriched_df)
            
            # Monitor
            StreamMonitor.log_progress(self.query)
            
        except Exception as e:
            logger.error(f"❌ Pipeline error: {str(e)}")
            raise
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.query and self.query.isActive:
            self.query.stop()
            logger.info("✅ Streaming query stopped")
        
        if self.spark:
            self.spark.stop()
            logger.info("✅ Spark session closed")


# =========================
# Entry Point
# =========================
def main():
    """Main entry point"""
    pipeline = ETLPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()