"""Consuming kiosk event data."""

from os import environ as ENV
import json
import logging
import argparse
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException

from logger_config import setup_logging
from transform import (get_db_connection, get_exhibition_mapping_dict,
                       get_request_mapping_dict, get_rating_mapping_dict,
                       format_request_data_for_insertion, format_rating_data_for_insertion)
from load import upload_request_data, upload_rating_data


def parse_args():
    """Command line arguments to modify the pipeline."""
    parser = argparse.ArgumentParser(
        description="Museum kiosk stream consumer.")
    parser.add_argument("-l", "--log_output",
                        type=str, choices=["file", "console"],
                        default="console",
                        help="Specify where to log output: 'file' or 'console' (default='console')"
                        )
    return parser.parse_args()


def consume_messages(cons: Consumer, messages_consumed=0) -> tuple[list[dict], list[dict]]:
    """Processes Kafka messages."""

    expected_vals = [-1, 0, 1, 2, 3, 4]  # list[int]
    expected_types = [1, 0]  # list[str]
    expected_sites = ['0', '1', '2', '3', '4', '5']  # list[str]
    req = []
    rat = []

    try:
        while messages_consumed < MAX_MESSAGES:
            msg = cons.poll(1.0)
            if msg:
                if msg.error():
                    logging.debug(
                        "There is an error at offset '{0}': {1}".format(msg.offset(), msg.value()))
                    raise KafkaException(msg.error())
                else:
                    if messages_consumed % log_frequency == 0:
                        try:
                            msg_data = json.loads(msg.value().decode())
                            at_iso = msg_data.get('at')
                            site = msg_data.get('site')
                            val = msg_data.get('val')
                            type_val = msg_data.get('type', None)

                            if (at_iso is None) or (site is None) or (val is None):
                                logging.error(
                                    "Missing data at offset '{0}': {1}".format(msg.offset(), msg.value()))
                                continue
                            else:
                                messages_consumed += 1
                                at = datetime.fromisoformat(at_iso)
                                if not isinstance(val, int):
                                    if val.isdigit():
                                        val = int(val)
                                    else:
                                        logging.error(
                                            "Missing or invalid data at offset '{0}': {1}".format(msg.offset(), msg.value()))
                            if site not in expected_sites:
                                logging.error(
                                    "Invalid value at offset '{0}': {1}".format(msg.offset(), msg.value()))
                            elif val not in expected_vals:
                                logging.error(
                                    "Invalid value at offset '{0}': {1}".format(msg.offset(), msg.value()))
                            elif val == -1:
                                if type_val not in expected_types:
                                    logging.error(
                                        "Mechanical error at offset '{0}': {1}".format(msg.offset(), msg.value()))
                                else:
                                    logging.info("Message {0} REQUEST: at={1}, site={2}, type={3}".format(
                                        messages_consumed, at, site, type_val))
                                    req.append(
                                        {"at": at, "site": int(site), "type": int(type_val)})
                            else:
                                logging.info("Message {0} RATING: at={1}, site={2}, val={3}".format(
                                    messages_consumed, at, site, val))
                                rat.append(
                                    {"at": at, "site": int(site), "val": val})

                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding JSON at offset {
                                          msg.offset()}: {e}")
                            continue
                        except AttributeError as e:
                            logging.debug(f"Unexpected error: {e}")
                            continue

        logging.info(
            "Finished consuming {0} messages.".format(messages_consumed))

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user. {0} messages consumed.".format(
            messages_consumed))

    finally:
        cons.close()

    return req, rat


if __name__ == '__main__':

    MAX_MESSAGES = 17000
    messages_consumed = 0
    log_frequency = 1

    load_dotenv()
    args = parse_args()
    setup_logging(args.log_output, filename='kafka_stream.txt', level=10)

    kafka_config = {
        'bootstrap.servers': ENV['BOOTSTRAP_SERVERS'],
        'security.protocol': ENV['SECURITY_PROTOCOL'],
        'sasl.mechanisms': ENV['SASL_MECHANISM'],
        'sasl.username': ENV['USERNAME'],
        'sasl.password': ENV['PASSWORD'],
        'group.id': 'zander_upload_attempt7',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_config)
    logging.debug("Subscribing to topic: {0}".format(ENV["TOPIC"]))
    consumer.subscribe([ENV["TOPIC"]])

    # Load in the kafka stream and extract valid data
    [requests, ratings] = consume_messages(consumer)
    logging.debug("Request count: {0}".format(len(requests)))
    logging.debug("Rating count: {0}".format(len(ratings)))

    # Establish a connection to the database
    db_conn = get_db_connection()
    logging.debug("Established connection with the database.")

    # Access the seeded data primary key IDs to simplify upload
    exhibit_mapping = get_exhibition_mapping_dict(db_conn)
    request_mapping = get_request_mapping_dict(db_conn)
    rating_mapping = get_rating_mapping_dict(db_conn)
    logging.debug("Key mappings successfully pulled.")

    # Process the data for insertion
    requests = format_request_data_for_insertion(
        requests, exhibit_mapping, request_mapping)
    ratings = format_rating_data_for_insertion(
        ratings, exhibit_mapping, rating_mapping)
    logging.debug("Stream data transformed for insertion.")

    # Upload the formatted data
    upload_request_data(requests, db_conn)
    upload_rating_data(ratings, db_conn)
    logging.debug("Upload to database complete")
    db_conn.close()
