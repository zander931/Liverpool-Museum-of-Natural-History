"""Bulk inserts the kiosk output data into the database."""

import logging
import os
from os import environ as ENV
from dotenv import load_dotenv
import csv

from logger_config import setup_logging
from extract import connect_to_s3, list_objects, download_objects, check_objects, combine_csv
from transform import get_db_connection, get_exhibition_mapping_dict, get_request_mapping_dict, get_rating_mapping_dict, format_request_data_for_insertion, format_rating_data_for_insertion
from load import upload_request_data, upload_rating_data


def load_csv(kiosk_data: str) -> tuple[list[dict], list[dict]]:
    """Loads the kiosk data into two separate lists based on 'type'."""
    requests = []
    ratings = []
    with open(f"static_data/{kiosk_data}", 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('type'):
                requests.append({
                    "site": int(row["site"]),
                    "type": int(float(row["type"])),
                    "at": row["at"]
                })
            else:
                ratings.append({
                    "site": int(row["site"]),
                    "val": int(row["val"]),
                    "at": row["at"]
                })
        return requests, ratings


if __name__ == '__main__':

    # Configuration
    setup_logging()
    load_dotenv()
    kiosk_data = 'lmnh_hist_data_full.csv'

    # Connect to s3 bucket, check relevant files and download
    s3 = connect_to_s3()
    contents = list_objects(s3, ENV['MUSEUM_BUCKET'])
    new_contents = check_objects(contents)
    download_objects(s3, ENV['MUSEUM_BUCKET'], new_contents)

    # Combine .csv kiosk data and separate into requests + ratings
    combine_csv(new_contents, kiosk_data)
    [requests, ratings] = load_csv(kiosk_data)

    # Establish a connection to the database
    db_conn = get_db_connection()
    logging.info("Established connection with database.")

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

    # Deleting .csv file
    os.remove(f"static_data/{kiosk_data}")
    logging.warning("Deleted kiosk file to prevent duplicated entries.")
