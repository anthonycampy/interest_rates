import psycopg2
from psycopg2.extras import execute_values


def get_connection(database_url):
    """Create a new database connection."""
    return psycopg2.connect(database_url)


def get_latest_date(conn, series_id):
    """
    Get the most recent observation date for a series.
    Returns a date string "YYYY-MM-DD" or None if no data exists.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(date) FROM raw_dgs30_fred WHERE series_id = %s",
            (series_id,),
        )
        result = cur.fetchone()[0]
        return result.isoformat() if result else None


def upsert_observations(conn, series_id, observations):
    """
    Upsert FRED observations into the database.

    Uses INSERT ... ON CONFLICT (series_id, date) DO UPDATE
    to handle duplicates idempotently.
    """
    if not observations:
        return 0

    rows = []
    for obs in observations:
        value = None if obs["value"] == "." else float(obs["value"])
        rows.append((
            series_id,
            obs["date"],
            value,
            obs.get("realtime_start"),
            obs.get("realtime_end"),
        ))

    sql = """
        INSERT INTO raw_dgs30_fred
            (series_id, date, value, realtime_start, realtime_end, updated_at)
        VALUES %s
        ON CONFLICT (series_id, date)
        DO UPDATE SET
            value = EXCLUDED.value,
            realtime_start = EXCLUDED.realtime_start,
            realtime_end = EXCLUDED.realtime_end,
            updated_at = now()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template="(%s, %s, %s, %s, %s, now())")
    conn.commit()
    return len(rows)
