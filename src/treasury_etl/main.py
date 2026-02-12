from datetime import date, datetime, timezone

from .config import get_config
from .treasury_client import fetch_yield_curve
from .db import get_connection, get_latest_date, upsert_yield_curve


def main():
    config = get_config()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Treasury yield curve ETL")

    conn = get_connection(config["database_url"])

    try:
        latest = get_latest_date(conn)
        current_year = date.today().year

        if latest:
            print(f"  Incremental mode: latest date in DB is {latest.isoformat()}")
        else:
            print(f"  No existing data. Fetching current year ({current_year}).")

        rows = fetch_yield_curve(current_year)
        print(f"  Fetched {len(rows)} rows from treasury.gov for {current_year}")

        if latest:
            rows = [r for r in rows if r["date"] > latest]
            print(f"  {len(rows)} new rows (after {latest.isoformat()})")

        count = upsert_yield_curve(conn, rows)
        print(f"  Upserted {count} rows")
    finally:
        conn.close()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Treasury yield curve ETL complete")


if __name__ == "__main__":
    main()
