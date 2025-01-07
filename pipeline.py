"""Loads the kiosk output data into the database."""

from os import environ as ENV
from dotenv import load_dotenv
from extract import connect_to_s3, list_objects, download_objects, check_objects, combine_csv


if __name__ == '__main__':
    load_dotenv()

    s3 = connect_to_s3()
    contents = list_objects(s3, ENV['MUSEUM_BUCKET'])
    new_contents = check_objects(contents)
    download_objects(s3, ENV['MUSEUM_BUCKET'], new_contents)
    combine_csv(new_contents, 'lmnh_hist_data_full.csv')
