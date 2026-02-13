from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .config import get_config
from .treasury_client import fetch_yield_curve
from .db import get_connection, get_latest_date, get_latest_row, upsert_yield_curve


def main():
    config = get_config()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Treasury yield curve ETL")

    conn = get_connection(config["database_url"])

    try:
        latest = get_latest_date(conn)
        current_year = datetime.now(ZoneInfo("America/New_York")).date().year

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

        # If no new data for today (e.g. holiday), forward-fill from the latest row
        today = datetime.now(ZoneInfo("America/New_York")).date()
        latest_after = get_latest_date(conn)
        if latest_after and latest_after < today:
            prev_row = get_latest_row(conn)
            if prev_row:
                filled_row = dict(prev_row)
                filled_row["date"] = today
                fill_count = upsert_yield_curve(conn, [filled_row])
                print(f"  No data for {today.isoformat()} (holiday?), forward-filled from {latest_after.isoformat()} ({fill_count} row)")
    finally:
        conn.close()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Treasury yield curve ETL complete")


if __name__ == "__main__":
    main()
