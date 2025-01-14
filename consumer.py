"""Consuming kiosk event data."""

from os import environ as ENV
import json
import logging
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException

from logger_config import setup_logging


def consume_messages(cons: Consumer, messages_consumed=0) -> None:
    """Processes Kafka messages."""

    expected_vals = [-1, 0, 1, 2, 3, 4]  # list[int]
    expected_types = ['-1', '0']  # list[str]
    expected_sites = ['0', '1', '2', '3', '4', '5']  # list[str]

    try:
        while messages_consumed < MAX_MESSAGES:
            msg = cons.poll(1.0)
            if messages_consumed == 0:
                print(msg.offset())
            if msg:
                if msg.error():
                    logging.debug(
                        "There is an error at offset '{0}'".format(msg.offset()))
                    raise KafkaException(msg.error())
                else:
                    messages_consumed += 1
                    if messages_consumed % log_frequency == 0:
                        try:
                            msg_data = json.loads(msg.value().decode())
                            at_iso = msg_data.get('at')
                            site = msg_data.get('site')
                            val = msg_data.get('val')
                            type_val = msg_data.get('type', None)

                            if (at_iso is None) or (site is None) or (val is None):
                                logging.error(
                                    "Missing data at offset '{0}'".format(msg.offset()))
                                continue
                            else:
                                at = datetime.fromisoformat(at_iso)
                                if not isinstance(val, int):
                                    if val.isdigit():
                                        val = int(val)
                                    else:
                                        logging.error(
                                            "Missing or invalid data at offset '{0}'".format(msg.offset()))
                            if site not in expected_sites:
                                logging.error(
                                    "Invalid value at offset '{0}'".format(msg.offset()))
                            elif val not in expected_vals:
                                logging.error(
                                    "Invalid value at offset '{0}'".format(msg.offset()))
                            elif val == -1:
                                if type_val not in expected_types:
                                    logging.error(
                                        "Mechanical error at offset '{0}'".format(msg.offset()))
                                else:
                                    logging.info("Message {0} REQUEST: at={1}, site={2}, type={3}".format(
                                        messages_consumed, at, site, type_val))
                            else:
                                logging.info("Message {0} RATING: at={1}, site={2}, val={3}".format(
                                    messages_consumed, at, site, val))

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


if __name__ == '__main__':

    MAX_MESSAGES = 100000
    messages_consumed = 0
    log_frequency = 20

    load_dotenv()
    setup_logging('console', level=10)

    kafka_config = {
        'bootstrap.servers': ENV['BOOTSTRAP_SERVERS'],
        'security.protocol': ENV['SECURITY_PROTOCOL'],
        'sasl.mechanisms': ENV['SASL_MECHANISM'],
        'sasl.username': ENV['USERNAME'],
        'sasl.password': ENV['PASSWORD'],
        'group.id': 'zander-cg',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_config)
    logging.debug("Subscribing to topic: {0}".format(ENV["TOPIC"]))
    consumer.subscribe([ENV["TOPIC"]])
    consume_messages(consumer)
