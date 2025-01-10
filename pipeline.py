"""Bulk inserts the kiosk output data into the database."""

import logging
import argparse
from dotenv import load_dotenv

from logger_config import setup_logging
from extract import connect_to_s3, list_objects, download_objects, check_objects, combine_csv
from transform import (get_db_connection, get_exhibition_mapping_dict,
                       get_request_mapping_dict, get_rating_mapping_dict,
                       format_request_data_for_insertion, format_rating_data_for_insertion)
from load import load_data, upload_request_data, upload_rating_data


def parse_args():
    """Command line arguments to modify the pipeline."""
    parser = argparse.ArgumentParser(description="Museum ETL Pipeline")
    parser.add_argument("-b", "--bucket",
                        type=str, default="sigma-resources-museum",
                        help="Set the AWS S3 bucket name for where the data is stored. (default=sigma-resources-museum)"
                        )
    parser.add_argument("-n", "--num_rows",
                        type=int, default=None,
                        help="Number of rows (int) to be uploaded to the database (default=all)"
                        )
    parser.add_argument("-l", "--log_output",
                        type=str, choices=["file", "console"],
                        default="console",
                        help="Specify where to log output: 'file' or 'console' (default='console')"
                        )
    return parser.parse_args()


if __name__ == '__main__':

    # Configuration
    args = parse_args()
    setup_logging(args.log_output)
    load_dotenv()
    KIOSK_DATA = 'lmnh_hist_data_full.csv'

    # Connect to s3 bucket, check relevant files and download
    s3 = connect_to_s3()
    contents = list_objects(s3, args.bucket)
    new_contents = check_objects(contents)
    download_objects(s3, args.bucket, new_contents)

    # Combine .csv kiosk data and separate into requests + ratings
    combine_csv(new_contents, KIOSK_DATA)
    [requests, ratings] = load_data(KIOSK_DATA, args.num_rows)

    # Establish a connection to the database
    db_conn = get_db_connection()
    logging.info("Established connection with RDS.")

    # Access the seeded data primary key IDs to simplify upload
    exhibit_mapping = get_exhibition_mapping_dict(db_conn)
    request_mapping = get_request_mapping_dict(db_conn)
    rating_mapping = get_rating_mapping_dict(db_conn)
    logging.info("Value: Key mappings successfully pulled from database.")

    # Process the data for insertion
    requests = format_request_data_for_insertion(
        requests, exhibit_mapping, request_mapping)
    ratings = format_rating_data_for_insertion(
        ratings, exhibit_mapping, rating_mapping)
    logging.info("Kiosk data transformed for insertion into the database.")

    # Upload the formatted data
    upload_request_data(requests, db_conn)
    upload_rating_data(ratings, db_conn)
    logging.info("Upload to database complete.")
    db_conn.close()
