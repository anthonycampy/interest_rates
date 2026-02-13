import psycopg2
from psycopg2 import sql


def get_connection(database_url):
    """Create a new database connection."""
    return psycopg2.connect(database_url)


def fetch_view(conn, view_name):
    """
    Query a view and return rows as list of (date, value) tuples.
    Results are ordered by date ascending.
    """
    query = sql.SQL("SELECT date, value FROM {} ORDER BY date").format(
        sql.Identifier(view_name)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
