"""Functions that interact with the museum database"""

from os import environ as ENV
from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor, execute_values


def get_db_connection() -> connection:
    """Returns a live connection to the database."""
    return connect(database=ENV['DB_NAME'],
                   cursor_factory=RealDictCursor)


def get_exhibition_mapping_dict(conn: connection) -> dict:
    """Returns a dict mapping public IDs to in-database IDs"""
    with conn.cursor() as cur:
        cur.execute("SELECT public_id, exhibition_id FROM exhibition;")
        rows = cur.fetchall()
    return {int(row["public_id"][-1]): row["exhibition_id"] for row in rows}


def get_request_mapping_dict(conn: connection) -> dict:
    """Returns a dict mapping request values to in-database IDs"""
    with conn.cursor() as cur:
        cur.execute("SELECT request_value, request_id FROM request;")
        rows = cur.fetchall()
    return {row["request_value"]: row["request_id"] for row in rows}


def get_rating_mapping_dict(conn: connection) -> dict:
    """Returns a dict mapping rating values to in-database IDs"""
    with conn.cursor() as cur:
        cur.execute("SELECT rating_value, rating_id FROM rating;")
        rows = cur.fetchall()
    return {row["rating_value"]: row["rating_id"] for row in rows}


def format_request_data_for_insertion(requests: list[dict], exhibit_mapping: dict, request_mapping: dict) -> list[tuple]:
    """Returns a list of request tuples, replacing string values with foreign key mappings."""
    formatted = []
    for request in requests:
        formatted.append((
            exhibit_mapping[request["site"]],
            request_mapping[request["type"]],
            request["at"]
        ))
    return formatted


def format_rating_data_for_insertion(ratings: list[dict], exhibit_mapping: dict, rating_mapping: dict) -> list[tuple]:
    """Returns a list of rating tuples, replacing string values with foreign key mappings."""
    formatted = []
    for rating in ratings:
        formatted.append((
            exhibit_mapping[rating["site"]],
            rating_mapping[rating["val"]],
            rating["at"]
        ))
    return formatted


def upload_request_data(requests: list[tuple], conn: connection):
    """Bulk inserts the transformed request data to the database."""

    query = """
        INSERT INTO request_interaction
            (exhibition_id, request_id, event_at)
        VALUES %s;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, requests)
    conn.commit()


def upload_rating_data(ratings: list[tuple], conn: connection):
    """Bulk inserts the transformed rating data to the database."""

    query = """
        INSERT INTO rating_interaction
            (exhibition_id, rating_id, event_at)
        VALUES %s;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, ratings)
    conn.commit()
