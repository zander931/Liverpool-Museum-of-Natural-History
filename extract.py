"""An extract Python script that connects to S3 and downloads files relevant to the project."""

import os
from os import environ as ENV
import logging
import re
import csv
from boto3 import client

from logger_config import setup_logging


def connect_to_s3():
    """Connects to S3."""
    s3 = client("s3", aws_access_key_id=ENV["AWS_ACCESS_KEY"],
                aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"])
    logging.info("Successfully connected to s3 bucket.")


def list_objects(s3_client, bucket_name: str) -> list[str]:
    """Returns a list of object names in a specific bucket."""
    objects = s3_client.list_objects(Bucket=bucket_name)['Contents']
    return [o["Key"] for o in objects]


def download_objects(s3_client, bucket_name: str, objects: list[str]):
    """Downloads objects from a bucket."""
    for o in objects:
        s3_client.download_file(bucket_name, o, f"static_data/{o}")
        logging.info(f"Downloaded file from s3 bucket: {o}")


def check_objects(objects: list[str]) -> bool:
    """Check if the object is relevant to the project."""
    new_contents = []
    pattern = r'^(lmnh_exhibition_.*\.json|lmnh_hist_data_.*\.csv)$'
    for o in objects:
        if re.match(pattern, o):
            new_contents.append(o)
    return new_contents


def combine_csv(contents: list[str], output_file: str):
    """Combine all kiosk output csv data into a single file."""
    with open(f"static_data/{output_file}", 'w', newline='', encoding='utf-8') as output_csv:
        writer = csv.writer(output_csv)
        header_flag = True
        for file in contents:
            if file.endswith('.csv'):
                with open(f"static_data/{file}", 'r', encoding='utf-8') as csv_data:
                    csv_reader = csv.reader(csv_data)
                    if header_flag:
                        header = next(csv_reader)
                        writer.writerow(header)
                        header_flag = False
                    else:
                        next(csv_reader)

                    for row in csv_reader:
                        writer.writerow(row)
                os.remove(f"static_data/{file}")
                logging.info(f"Deleted local file: {f"static_data/{file}"}")

    logging.info(f"CSV files combined successfully: {
                 f"static_data/{output_file}"}")


setup_logging()
