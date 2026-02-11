import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


VALID_TABLES = {
    "DGS30": "raw_dgs30_fred",
    "DGS10": "raw_dgs10_fred",
    "DGS5": "raw_dgs5_fred",
    "DGS2": "raw_dgs2_fred",
    "DGS1": "raw_dgs1_fred",
    "T10Y2Y": "raw_t10y2y_fred",
    "DFF": "raw_dff_fred",
}


def _table_name(series_id):
    """Get the table name for a series, validated against allowlist."""
    table = VALID_TABLES.get(series_id.upper())
    if not table:
        raise ValueError(f"Unknown series: {series_id}. Valid: {list(VALID_TABLES.keys())}")
    return table


def get_connection(database_url):
    """Create a new database connection."""
    return psycopg2.connect(database_url)


def get_latest_date(conn, series_id):
    """
    Get the most recent observation date for a series.
    Returns a date string "YYYY-MM-DD" or None if no data exists.
    """
    table = _table_name(series_id)
    query = sql.SQL("SELECT MAX(date) FROM {} WHERE series_id = %s").format(
        sql.Identifier(table)
    )
    with conn.cursor() as cur:
        cur.execute(query, (series_id,))
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

    table = _table_name(series_id)

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

    query = sql.SQL("""
        INSERT INTO {}
            (series_id, date, value, realtime_start, realtime_end, updated_at)
        VALUES %s
        ON CONFLICT (series_id, date)
        DO UPDATE SET
            value = EXCLUDED.value,
            realtime_start = EXCLUDED.realtime_start,
            realtime_end = EXCLUDED.realtime_end,
            updated_at = now()
    """).format(sql.Identifier(table))

    with conn.cursor() as cur:
        execute_values(cur, query, rows, template="(%s, %s, %s, %s, %s, now())")
    conn.commit()
    return len(rows)
