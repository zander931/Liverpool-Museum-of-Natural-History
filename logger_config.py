"""Manages the configuration of logging operations across the ETL pipeline."""

import logging


def setup_logging(level=20):
    """Setup the basicConfig."""
    logging.basicConfig(
        filename="museum_etl.log",
        encoding="utf-8",
        filemode="a",
        level=level,
        format="{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M"
    )
    logging.info("ETL pipeline started running.")
