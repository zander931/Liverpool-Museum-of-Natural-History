"""Consuming kiosk event data."""

from os import environ as ENV
import json
import logging

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException

from logger_config import setup_logging


def consume_messages(cons: Consumer, messages_consumed=0) -> None:
    """Processes Kafka messages."""
    try:
        while messages_consumed < MAX_MESSAGES:
            msg = cons.poll(1.0)
            if msg:
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    messages_consumed += 1
                    if messages_consumed % log_frequency == 0:
                        at = json.loads(msg.value().decode().get('at'))
                        site = json.loads(msg.value().decode().get('site'))
                        val = json.loads(msg.value().decode().get('val'))
                        if int(val) == -1:
                            type = json.loads(msg.value().decode().get('type'))
                            if type is None:
                                logging.error(
                                    "Mechanical error at offset '{0}'".format(msg.offset()))
                            else:
                                logging.info("Message {messages_consumed} REQUEST: at {1}, site {2}, type: {3}".format(
                                    messages_consumed, at, site, type))
                        else:
                            logging.info("Message {0} RATING: at {1}, site {2}, val: {3}".format(
                                messages_consumed, at, site, val))
                            # print(json.loads(msg.value().decode(), msg.offset(), msg.error()))

                        if not msg.value():
                            logging.warning(
                                "\nEmpty message received at offset {0}".format(msg.offset()))

        logging.info(
            "Finished consuming {0} messages.".format(messages_consumed))

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user. {0} messages consumed.".format(
            messages_consumed))

    finally:
        cons.close()


if __name__ == '__main__':

    MAX_MESSAGES = 10000
    messages_consumed = 0
    log_frequency = 20

    load_dotenv()
    setup_logging('console')

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
    consumer.subscribe([ENV["TOPIC"]])
    consume_messages(consumer)
