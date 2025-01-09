"Loads the transactional data after transformation."

import csv
from itertools import islice
from psycopg2.extensions import connection
from psycopg2.extras import execute_values


def load_data(data: str, num_rows: int) -> tuple[list[dict], list[dict]]:
    """Loads the data into two separate lists based on 'type'."""
    req = []
    rat = []
    with open(f"static_data/{data}", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = islice(reader, num_rows)
        for row in rows:
            if row.get('type'):
                req.append({
                    "site": int(row["site"]),
                    "type": int(float(row["type"])),
                    "at": row["at"]
                })
            else:
                rat.append({
                    "site": int(row["site"]),
                    "val": int(row["val"]),
                    "at": row["at"]
                })
        return req, rat


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
