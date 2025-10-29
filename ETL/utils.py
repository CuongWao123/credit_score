
# Configure logging

import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
import logging
from typing import Optional
import boto3
from botocore.client import Config



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinIOClient:
    """Client for reading data from MinIO"""
    
    def __init__(self, 
                 endpoint: str = "localhost:9000",
                 access_key: str = "minio",
                 secret_key: str = "minio123",
                 bucket: str = "applications-bureau-data",
                 use_ssl: bool = False):
        """
        Initialize MinIO client
        
        Args:
            endpoint: MinIO endpoint (host:port)
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket: Bucket name
            use_ssl: Use SSL/TLS
        """
        self.endpoint = endpoint
        self.bucket = bucket
        
        # Create S3 client for MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f"{'https' if use_ssl else 'http'}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        logger.info(f"MinIO client initialized: {endpoint}/{bucket}")
    
    def get_latest_partition(self, prefix: str) -> Optional[str]:
        """
        Get the latest partition path (year=YYYY/month=MM/day=DD)
        
        Args:
            prefix: Base prefix (e.g., 'applications/' or 'bureau_credits/')
            
        Returns:
            Latest partition path (e.g., 'applications/year=2025/month=10/day=28/')
        """
        try:
            logger.info(f"🔍 Finding latest partition in: {prefix}")
            
            # List all objects with prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No objects found with prefix: {prefix}")
                return None
            
            # Extract all partition paths (year=YYYY/month=MM/day=DD)
            partitions = set()
            for obj in response['Contents']:
                key = obj['Key']
                # Extract partition from key like: applications/year=2025/month=10/day=28/file.parquet
                parts = key.split('/')
                
                # Find year, month, day parts
                year_part = next((p for p in parts if p.startswith('year=')), None)
                month_part = next((p for p in parts if p.startswith('month=')), None)
                day_part = next((p for p in parts if p.startswith('day=')), None)
                
                if year_part and month_part and day_part:
                    partition = f"{prefix}{year_part}/{month_part}/{day_part}/"
                    partitions.add(partition)
            
            if not partitions:
                logger.warning(f"No partitions found in: {prefix}")
                return None
            
            # Sort partitions and get latest
            sorted_partitions = sorted(partitions, reverse=True)
            latest = sorted_partitions[0]
            
            logger.info(f"✅ Latest partition found: {latest}")
            return latest
            
        except Exception as e:
            logger.error(f"Error finding latest partition: {e}")
            raise
    
    def list_objects(self, prefix: str) -> list:
        """
        List all objects with given prefix
        
        Args:
            prefix: Object prefix (folder path)
            
        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No objects found with prefix: {prefix}")
                return []
            
            # Filter only parquet files
            objects = [
                obj['Key'] for obj in response['Contents']
                if obj['Key'].endswith('.parquet')
            ]
            
            logger.info(f"Found {len(objects)} parquet files in {prefix}")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            raise
    
    def read_parquet_to_dataframe(self, 
                                  prefix: str,
                                  use_latest_partition: bool = True,
                                  specific_date: Optional[str] = None) -> pd.DataFrame:
        """
        Read all parquet files from prefix into single DataFrame
        
        Args:
            prefix: Base prefix (e.g., 'applications/' or 'bureau_credits/')
            use_latest_partition: Auto-detect latest partition (default: True)
            specific_date: Read specific date 'YYYY-MM-DD' (overrides use_latest_partition)
            
        Returns:
            Pandas DataFrame
            
        Examples:
            # Read latest partition (default)
            df = client.read_parquet_to_dataframe("applications/")
            
            # Read all data (no partition filter)
            df = client.read_parquet_to_dataframe("applications/", use_latest_partition=False)
            
            # Read specific date
            df = client.read_parquet_to_dataframe("applications/", specific_date="2025-10-28")
        """
        # Determine actual prefix to read
        if specific_date:
            # Convert YYYY-MM-DD to year=YYYY/month=MM/day=DD
            year, month, day = specific_date.split('-')
            actual_prefix = f"{prefix}year={year}/month={month}/day={day}/"
            logger.info(f"📅 Reading data from specific date: {specific_date}")
        elif use_latest_partition:
            actual_prefix = self.get_latest_partition(prefix)
            if not actual_prefix:
                raise ValueError(f"No partitions found in: {prefix}")
        else:
            # Read all data (no partition filter)
            actual_prefix = prefix
            logger.info(f"📥 Reading ALL data from: {prefix}")
        
        logger.info(f"📥 Reading parquet files from: {actual_prefix}")
        
        # List all parquet files
        parquet_files = self.list_objects(actual_prefix)
        
        if not parquet_files:
            raise ValueError(f"No parquet files found in: {prefix}")
        
        # Read all files and concatenate
        dfs = []
        for i, file_key in enumerate(parquet_files, 1):
            try:
                # Get object from MinIO
                response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
                parquet_bytes = response['Body'].read()
                
                # Read parquet from bytes
                parquet_buffer = BytesIO(parquet_bytes)
                df_part = pq.read_table(parquet_buffer).to_pandas()
                
                dfs.append(df_part)
                logger.info(f"  ✓ Read file {i}/{len(parquet_files)}: {file_key} ({len(df_part):,} rows)")
                
            except Exception as e:
                logger.warning(f"  ✗ Error reading {file_key}: {e}")
                continue
        
        if not dfs:
            raise ValueError(f"No data could be read from: {prefix}")
        
        # Concatenate all DataFrames
        df_combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"✅ Combined DataFrame: {len(df_combined):,} rows, {len(df_combined.columns)} columns")
        
        return df_combined
    
    def read_all_partitions(self, prefix: str) -> pd.DataFrame:
        """
        Read ALL data from ALL partitions (vét cạn toàn bộ)
        
        Args:
            prefix: Base prefix (e.g., 'applications/' or 'bureau_credits/')
            
        Returns:
            Pandas DataFrame with all data from all partitions
            
        Example:
            # Read all data from all dates
            all_data = client.read_all_partitions("applications/")
        """
        logger.info(f"🗂️ Reading ALL partitions from: {prefix}")
        
        return self.read_parquet_to_dataframe(
            prefix=prefix,
            use_latest_partition=False
        )
    
    def get_all_partitions(self, prefix: str) -> list:
        """
        Get list of all available partitions
        
        Args:
            prefix: Base prefix (e.g., 'applications/' or 'bureau_credits/')
            
        Returns:
            List of partition paths sorted by date (newest first)
            
        Example:
            partitions = client.get_all_partitions("applications/")
            # Returns: ['applications/year=2025/month=10/day=28/', 
            #           'applications/year=2025/month=10/day=27/', ...]
        """
        try:
            logger.info(f"📋 Listing all partitions in: {prefix}")
            
            # List all objects with prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No objects found with prefix: {prefix}")
                return []
            
            # Extract all partition paths
            partitions = set()
            for obj in response['Contents']:
                key = obj['Key']
                parts = key.split('/')
                
                # Find year, month, day parts
                year_part = next((p for p in parts if p.startswith('year=')), None)
                month_part = next((p for p in parts if p.startswith('month=')), None)
                day_part = next((p for p in parts if p.startswith('day=')), None)
                
                if year_part and month_part and day_part:
                    partition = f"{prefix}{year_part}/{month_part}/{day_part}/"
                    partitions.add(partition)
            
            # Sort partitions (newest first)
            sorted_partitions = sorted(partitions, reverse=True)
            
            logger.info(f"✅ Found {len(sorted_partitions)} partitions")
            for partition in sorted_partitions:
                logger.info(f"   - {partition}")
            
            return sorted_partitions
            
        except Exception as e:
            logger.error(f"Error listing partitions: {e}")
            raise
    def write_parquet_to_bucket(self, 
                           df: pd.DataFrame,
                           bucket_name: str,
                           file_path: str,
                           compression: str = "snappy",
                           create_bucket: bool = True) -> str:
        """
        Write DataFrame to MinIO bucket as Parquet
        Args:
            df: DataFrame to write
            bucket_name: Target bucket name
            file_path: Full file path in bucket (e.g., 'folder/data.parquet' or 'year=2025/month=10/day=28/data.parquet')
            compression: Parquet compression ('snappy', 'gzip', 'brotli', 'none')
            create_bucket: Auto-create bucket if not exists (default: True)
        Returns:
            S3 URI of written file (e.g., 's3://bucket-name/folder/data.parquet')
        """
        try:
            logger.info(f"📦 Writing DataFrame to MinIO...")
            logger.info(f"   Bucket: {bucket_name}")
            logger.info(f"   Path: {file_path}")
            logger.info(f"   Shape: {len(df):,} rows × {len(df.columns)} columns")
            
            # Check/Create bucket
            if create_bucket:
                try:
                    self.s3_client.head_bucket(Bucket=bucket_name)
                    logger.info(f"   ✅ Bucket '{bucket_name}' exists")
                except:
                    logger.info(f"   📦 Creating bucket '{bucket_name}'...")
                    self.s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"   ✅ Bucket created")
            
            # Convert DataFrame to Parquet bytes
            logger.info(f"   🔄 Converting to Parquet (compression={compression})...")
            parquet_buffer = BytesIO()
            df.to_parquet(
                parquet_buffer, 
                engine='pyarrow', 
                compression=compression, 
                index=False
            )
            parquet_buffer.seek(0)
            parquet_bytes = parquet_buffer.getvalue()
            
            # Get file size
            file_size_mb = len(parquet_bytes) / (1024 * 1024)
            logger.info(f"   📊 Parquet size: {file_size_mb:.2f} MB")
            
            # Upload to MinIO
            logger.info(f"   📤 Uploading...")
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=file_path,
                Body=parquet_bytes,
                ContentType='application/octet-stream'
            )
            
            s3_uri = f"s3://{bucket_name}/{file_path}"
            logger.info(f"✅ Write completed!")
            logger.info(f"   📍 S3 URI: {s3_uri}")
            logger.info(f"   📊 Size: {file_size_mb:.2f} MB")
            
            return s3_uri
            
        except Exception as e:
            logger.error(f"❌ Failed to write to MinIO: {e}")
            raise

class DataJoiner:
    """General-purpose DataFrame joiner"""
    
    @staticmethod
    def join_tables(df1: pd.DataFrame, 
                   df2: pd.DataFrame,
                   join_key: str = "reco_id_curr",
                   join_type: str = "left") -> pd.DataFrame:
        """
        Join 2 dataframes on given key
        
        Args:
            df1: First DataFrame
            df2: Second DataFrame
            join_key: Column to join on
            join_type: Join type (left, inner, right, outer)
            
        Returns:
            Joined DataFrame
        """
        logger.info(f"🔄 Joining tables on '{join_key}' (join_type={join_type})...")
        logger.info(f"   DataFrame 1: {len(df1):,} rows, {len(df1.columns)} columns")
        logger.info(f"   DataFrame 2: {len(df2):,} rows, {len(df2.columns)} columns")
        
        # Add suffix to avoid column name conflicts
        joined_df = df1.merge(
            df2,
            on=join_key,
            how=join_type,
            suffixes=('_1', '_2')
        )
        
        logger.info(f"✅ JOIN completed: {len(joined_df):,} rows, {len(joined_df.columns)} columns")
        
        # Calculate some statistics
        unique_keys_df1 = df1[join_key].nunique()
        unique_keys_df2 = df2[join_key].nunique()
        unique_keys_joined = joined_df[join_key].nunique()
        
        logger.info(f"   📊 Statistics:")
        logger.info(f"      - Unique keys in df1: {unique_keys_df1:,}")
        logger.info(f"      - Unique keys in df2: {unique_keys_df2:,}")
        logger.info(f"      - Unique keys in joined: {unique_keys_joined:,}")
        logger.info(f"      - Row multiplication: {len(joined_df) / len(df1):.2f}x")
        
        return joined_df


class ExcelExporter:
    """Export DataFrame to Excel with formatting"""
    
    @staticmethod
    def export_to_excel(df: pd.DataFrame, 
                       output_path: str,
                       sheet_name: str = "Joined Data",
                       include_summary: bool = True) -> None:
        """
        Export DataFrame to Excel file
        
        Args:
            df: DataFrame to export
            output_path: Output file path
            sheet_name: Excel sheet name
            include_summary: Create summary sheet
        """
        logger.info(f"💾 Exporting to Excel: {output_path}")
        logger.info(f"   Rows: {len(df):,}, Columns: {len(df.columns)}")
        
        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Write main data
            df.to_excel(writer, sheet_name=sheet_name, index=False, freeze_panes=(1, 0))
            
            # Get worksheet
            worksheet = writer.sheets[sheet_name]
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)  # Max width 50
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Create summary sheet if requested
            if include_summary:
                ExcelExporter._create_summary_sheet(df, writer)
        
        logger.info(f"✅ Excel file created successfully!")
        logger.info(f"   Location: {output_path}")
    
    @staticmethod
    def _create_summary_sheet(df: pd.DataFrame, writer: pd.ExcelWriter) -> None:
        """Create summary statistics sheet"""
        
        summary_data = {
            'Metric': [
                'Total Rows',
                'Total Columns',
                'Total Applications',
                'Applications with Bureau Data',
                'Applications without Bureau Data',
                'Total Bureau Credits',
                'Date Range (Processing Date)',
                'Export Timestamp'
            ],
            'Value': [
                f"{len(df):,}",
                f"{len(df.columns)}",
                f"{df['reco_id_curr'].nunique():,}",
                f"{df[df['reco_bureau_id'].notna()]['reco_id_curr'].nunique():,}",
                f"{df[df['reco_bureau_id'].isna()]['reco_id_curr'].nunique():,}",
                f"{df['reco_bureau_id'].notna().sum():,}",
                f"{df['processing_date_app'].min()} to {df['processing_date_app'].max()}" if 'processing_date_app' in df.columns else "N/A",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        
        logger.info("   ✓ Summary sheet created")

