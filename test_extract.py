"""Test file for extract.py"""

import pytest
from unittest.mock import MagicMock
from extract import check_objects, combine_csv, download_objects


def test_check_objects():
    assert check_objects(['chipotle_stores.csv', 'explorer.json', 'joana_baby_beaver.jpg', 'lmnh_exhibition', 'lmnh_exhibition_bugs.json', 'lmnh_exhibition_cave.json', 'lmnh_exhibition_dino.json', 'lmnh_exhibition_dino.txt', 'lmnh_exhibition_explorer.json', 'lmnh_exhibition_explorer.txt', 'lmnh_exhibition_pollution.json', 'lmnh_exhibition_pollution.txt', 'lmnh_exhibition_whales.json',
                         'lmnh_hist_data_0.csv', 'lmnh_hist_data_1.csv', 'lmnh_hist_data_2.csv', 'lmnh_hist_data_3.csv', 'lms_exhibition_asimov.json', 'lms_exhibition_planets.json', 'lms_exhibition_poison.json', 'log.json', 'log_2.json', 'log_3.json', 'log_4.json', 'log_5.json', 'uber-raw-data-jun14.csv', 'uber-raw-data-may14.csv', 'uber-raw-data-sep14.csv', 'us-states.json']) == ['lmnh_exhibition_bugs.json', 'lmnh_exhibition_cave.json', 'lmnh_exhibition_dino.json', 'lmnh_exhibition_explorer.json', 'lmnh_exhibition_pollution.json', 'lmnh_exhibition_whales.json', 'lmnh_hist_data_0.csv', 'lmnh_hist_data_1.csv', 'lmnh_hist_data_2.csv', 'lmnh_hist_data_3.csv']
    assert check_objects(
        ['lmnh_hist_data_1.json', 'lmnh_exhibition_bugs.csv']) == []
    assert check_objects([]) == []


def test_download_objects():
    mock_s3_client = MagicMock()
    bucket_name = 'test_bucket'
    objects = ['file1.csv', 'file2.csv', 'file3.csv']

    download_objects(mock_s3_client, bucket_name, objects)

    assert mock_s3_client.download_file.call_count == len(objects)
