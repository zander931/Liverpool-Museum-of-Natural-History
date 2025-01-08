"""Loads the kiosk output data into the database."""

from psycopg2 import connect
from psycopg2.extensions import connection, cursor
from psycopg2.extras import RealDictCursor, RealDictRow
from os import environ as ENV
from dotenv import load_dotenv
import csv
from extract import connect_to_s3, list_objects, download_objects, check_objects, combine_csv


def load_csv(kiosk_data: str) -> tuple[list[dict], list[dict]]:
    """Loads the kiosk data into two separate lists based on 'type'."""
    requests = []
    ratings = []
    with open(kiosk_data, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('type'):
                requests.append(row)
            else:
                ratings.append(row)
        return requests, ratings


def create_staging_table():
    """Creates a temporary staging table within the database."""
    request_temp_query = """
    CREATE TEMP TABLE staging_request_interaction (
        at TIMESTAMPTZ,
        site SMALLINT,
        type SMALLINT
    );
    """
    rating_temp_query = """
    CREATE TEMP TABLE staging_rating_interaction (
        at TIMESTAMPTZ,
        site SMALLINT,
        val SMALLINT
    );
    """
    with conn.cursor() as cur:
        cur.execute(request_temp_query)
        cur.execute(rating_temp_query)

# exhibition_id : public_id
# 1:EXH_01
# 2:EXH_05
# 3:EXH_04
# 4:EXH_00
# 5:EXH_02
# 6:EXH_03


if __name__ == '__main__':

    load_dotenv()
    kiosk_data = 'lmnh_hist_data_full.csv'

    s3 = connect_to_s3()
    contents = list_objects(s3, ENV['MUSEUM_BUCKET'])
    new_contents = check_objects(contents)
    download_objects(s3, ENV['MUSEUM_BUCKET'], new_contents)
    combine_csv(new_contents, kiosk_data)

    [requests, ratings] = load_csv(kiosk_data)

    conn = connect(dbname='museum', user='zander', host='localhost')
    print("Connection established successfully!")
