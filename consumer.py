"""Consuming kiosk event data."""

from os import environ as ENV
import json
import logging
import argparse
from datetime import datetime, time

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException

from logger_config import setup_logging
from transform import (get_db_connection, get_exhibition_mapping_dict,
                       get_request_mapping_dict, get_rating_mapping_dict,
                       format_request_data_for_insertion, format_rating_data_for_insertion)
from load import upload_request_data, upload_rating_data


MAX_MESSAGES = 10000
LOG_FREQUENCY = 1
EXPECTED_VALS = [-1, 0, 1, 2, 3, 4]
EXPECTED_TYPES = [1, 0]
EXPECTED_SITES = ['0', '1', '2', '3', '4', '5']


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


def process_message(msg, msg_count: int):
    """Process the data sent in the message."""

    msg_data = json.loads(msg.value().decode())
    at_iso = msg_data.get('at')
    site = msg_data.get('site')
    val = msg_data.get('val')
    type_val = msg_data.get('type', None)

    if (at_iso is None) or (site is None) or (val is None):
        logging.error("Missing data at offset '%d': %s",
                      msg.offset(), msg.value())
        return False

    at = datetime.fromisoformat(at_iso)
    start_time = time(8, 45, 0)
    end_time = time(18, 15, 0)
    if (at.time() < start_time) or (at.time() > end_time):
        logging.error("Outside open times at offset '%d': %s",
                      msg.offset(), msg.value())
        return False

    if not isinstance(val, int):
        if val.isdigit():
            val = int(val)
        else:
            logging.error("Missing or invalid data at offset '%d': %s",
                          msg.offset(), msg.value())
            return False

    if (site not in EXPECTED_SITES) or (val not in EXPECTED_VALS):
        logging.error("Invalid value at offset '%d': %s",
                      msg.offset(), msg.value())
        return False

    if val == -1:
        if type_val not in EXPECTED_TYPES:
            logging.error("Mechanical error at offset '%d': %s",
                          msg.offset(), msg.value())
            return False

        logging.info("Message %d REQUEST: at=%s, site=%s, type=%s",
                     msg_count, at, site, type_val)
        return {"at": at, "site": int(site), "type": int(type_val)}

    logging.info("Message %d RATING: at=%s, site=%s, val=%d",
                 msg_count, at, site, val)
    return {"at": at, "site": int(site), "val": val}


def consume_messages(cons: Consumer, messages_consumed=0) -> tuple[list[dict], list[dict]]:
    """Consumes the Kafka message stream."""

    req = []
    rat = []

    try:
        while messages_consumed < MAX_MESSAGES:
            msg = cons.poll(1.0)
            if msg:
                if msg.error():
                    logging.debug(
                        "There is an error at offset '%d': %s",
                        msg.offset(), msg.value())
                    raise KafkaException(msg.error())
                if messages_consumed % LOG_FREQUENCY == 0:
                    try:
                        if not msg.value():
                            logging.error("Message value is empty at offset '%d'",
                                          msg.offset())
                            continue

                        messages_consumed += 1
                        out = process_message(msg, messages_consumed)

                        if out:
                            if out.get("val") is None:
                                req.append(out)
                            else:
                                rat.append(out)

                    except json.JSONDecodeError as e:
                        logging.error("Error decoding JSON at offset '%d': %s",
                                      msg.offset(), e)
                        continue
                    except AttributeError as e:
                        logging.debug("Unexpected error at offset '%d': %s",
                                      msg.offset(), e)
                        continue

        logging.info(
            "Finished consuming %d messages.", messages_consumed)

    except KeyboardInterrupt:
        logging.info(
            "Consumer interrupted by user. %d messages consumed.", messages_consumed)

    finally:
        cons.close()

    return req, rat


if __name__ == '__main__':

    # Configuration details for the Consumer
    load_dotenv()
    args = parse_args()
    setup_logging(args.log_output, filename='kafka_stream.txt', level=10)

    kafka_config = {
        'bootstrap.servers': ENV['BOOTSTRAP_SERVERS'],
        'security.protocol': ENV['SECURITY_PROTOCOL'],
        'sasl.mechanisms': ENV['SASL_MECHANISM'],
        'sasl.username': ENV['USERNAME'],
        'sasl.password': ENV['PASSWORD'],
        'group.id': 'zander_upload_ec2',
        'auto.offset.reset': 'earliest'
    }

    # Create a consumer instance and subscribe to the topic
    consumer = Consumer(kafka_config)
    logging.debug("Subscribing to topic: %s", ENV["TOPIC"])
    consumer.subscribe([ENV["TOPIC"]])

    # Load in the kafka stream and extract valid data
    [requests, ratings] = consume_messages(consumer)
    logging.debug("Request count: %d", len(requests))
    logging.debug("Rating count: %d", len(ratings))

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
