import netCDF4 as nc
import numpy as np
import pandas as pd
from datetime import datetime
import time
import os
import random
import logging
import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Bucket containing input data
INPUT_BUCKET_NAME = os.environ["BUCKET_NAME"]
LOCATION = os.environ["INPUT_LOCATION"]

# Local output files
TMP_FILE_NAME = "/tmp/tmp.nc"
LOCAL_OUTPUT_FILE = "/tmp/dataframe.parquet"

# Bucket for output data
OUTPUT_BUCKET = os.environ["BUCKET_NAME"]
OUTPUT_PREFIX = os.environ["OUTPUT_LOCATION"]

# Create 100 random coordinates
random.seed(10)
coords = [(random.randint(1000, 2500), random.randint(1000, 2500)) for _ in range(100)]

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")
bucket = s3_resource.Bucket(INPUT_BUCKET_NAME)


def date_to_partition_name(date):
    d = datetime.strptime(date, "%Y%m%d")
    return d.strftime("%Y/%m/%d/")


def download_file_with_retry(key, local_file, max_retries=3):
    for attempt in range(max_retries):
        try:
            bucket.download_file(key, local_file)
            return
        except ClientError as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Download failed for {key}, retrying... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(2 ** attempt)  # Exponential backoff


def upload_file_with_retry(local_file, bucket, s3_key, max_retries=3):
    for attempt in range(max_retries):
        try:
            s3_client.upload_file(local_file, bucket, s3_key)
            return
        except S3UploadFailedError as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Upload failed for {s3_key}, retrying... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(2 ** attempt)  # Exponential backoff


def lambda_handler(event, context):
    try:
        # Get date from input
        date = str(event)
        logger.info(f"Raw input date: {date}")

        # Initialize output dataframe
        COLUMNS_NAME = ["time", "point_id", "DSSF_TOT", "FRACTION_DIFFUSE"]
        df = pd.DataFrame(columns=COLUMNS_NAME)

        prefix = LOCATION + date_to_partition_name(date)
        logger.info(f"Loading files from prefix: {prefix}")

        # List input files (weather files)
        objects = list(bucket.objects.filter(Prefix=prefix))

        if not objects:
            logger.warning(f"No objects found for prefix: {prefix}")
            return {"statusCode": 200, "body": "No data to process"}

        for obj in objects:
            key = obj.key
            logger.info(f"Processing: {key}")

            try:
                download_file_with_retry(key, TMP_FILE_NAME)

                with nc.Dataset(TMP_FILE_NAME) as dataset:
                    lats, lons = zip(*coords)
                    data_1 = dataset["DSSF_TOT"][0][lats, lons]
                    data_2 = dataset["FRACTION_DIFFUSE"][0][lats, lons]

                    nb_points = len(lats)
                    data_time = dataset.__dict__["time_coverage_start"]
                    time_list = [data_time] * nb_points
                    point_id_list = list(range(nb_points))
                    tuple_list = list(zip(time_list, point_id_list, data_1, data_2))

                    new_data = pd.DataFrame(tuple_list, columns=COLUMNS_NAME)
                    df = pd.concat([df, new_data])

            except Exception as e:
                logger.error(f"Error processing file {key}: {str(e)}")

        if df.empty:
            logger.warning("No data processed successfully")
            return {"statusCode": 200, "body": "No data processed successfully"}

        # Replace masked values by NaN (np.nan for NumPy 2.0+)
        df = df.applymap(lambda x: np.nan if isinstance(x, np.ma.core.MaskedConstant) else x)

        # Save to parquet
        logger.info(f"Writing result to tmp parquet file: {LOCAL_OUTPUT_FILE}")
        df.to_parquet(LOCAL_OUTPUT_FILE)

        # Copy result to S3
        s3_output_name = OUTPUT_PREFIX + date + ".parquet"
        upload_file_with_retry(LOCAL_OUTPUT_FILE, OUTPUT_BUCKET, s3_output_name)

        logger.info(f"Successfully processed and uploaded data for date: {date}")
        return {"statusCode": 200, "body": f"Successfully processed data for {date}"}

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        return {"statusCode": 500, "body": f"Error processing data: {str(e)}"}
