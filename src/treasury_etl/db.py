import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

TABLE_NAME = "raw_treasury_yield_curve"

MATURITY_COLUMNS = [
    "bc_1month",
    "bc_1_5month",
    "bc_2month",
    "bc_3month",
    "bc_4month",
    "bc_6month",
    "bc_1year",
    "bc_2year",
    "bc_3year",
    "bc_5year",
    "bc_7year",
    "bc_10year",
    "bc_20year",
    "bc_30year",
]

ALL_COLUMNS = ["date"] + MATURITY_COLUMNS


def get_connection(database_url):
    """Create a new database connection."""
    return psycopg2.connect(database_url)


def get_latest_date(conn):
    """Get the most recent date in the table. Returns date or None."""
    query = sql.SQL("SELECT MAX(date) FROM {}").format(
        sql.Identifier(TABLE_NAME)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def get_latest_row(conn):
    """Get the most recent row from the table as a dict. Returns dict or None."""
    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in ALL_COLUMNS)
    query = sql.SQL("SELECT {columns} FROM {table} ORDER BY date DESC LIMIT 1").format(
        columns=col_ids,
        table=sql.Identifier(TABLE_NAME),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(ALL_COLUMNS, row))


def upsert_yield_curve(conn, rows):
    """
    Upsert Treasury yield curve rows.
    Uses INSERT ... ON CONFLICT (date) DO UPDATE for idempotency.
    Returns number of rows upserted.
    """
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(tuple(row[col] for col in ALL_COLUMNS))

    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in ALL_COLUMNS)

    set_clause = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
        for c in MATURITY_COLUMNS
    )

    query = sql.SQL("""
        INSERT INTO {table} ({columns}, updated_at)
        VALUES %s
        ON CONFLICT (date)
        DO UPDATE SET
            {set_clause},
            updated_at = now()
    """).format(
        table=sql.Identifier(TABLE_NAME),
        columns=col_ids,
        set_clause=set_clause,
    )

    ph_list = ", ".join(["%s"] * len(ALL_COLUMNS))
    template = f"({ph_list}, now())"

    with conn.cursor() as cur:
        execute_values(cur, query, values, template=template)
    conn.commit()
    return len(values)
