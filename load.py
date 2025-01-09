"Loads the transactional data after transformation."

from psycopg2.extensions import connection
from psycopg2.extras import execute_values


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
