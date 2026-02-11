import datetime

from .config import get_config
from .fred_client import fetch_observations
from .db import get_connection, get_latest_date, upsert_observations


def main():
    config = get_config()
    series_id = config["series_id"]

    print(f"[{datetime.now(datetime.timezone.utc).isoformat()}] Starting FRED ETL for {series_id}")

    conn = get_connection(config["database_url"])

    try:
        latest = get_latest_date(conn, series_id)

        if latest:
            observation_start = latest
            print(f"  Incremental mode: fetching from {observation_start}")
        else:
            observation_start = None
            print("  Backfill mode: fetching all historical data")

        observations = fetch_observations(
            config["fred_api_key"],
            series_id,
            observation_start=observation_start,
        )
        print(f"  Fetched {len(observations)} observations from FRED")

        count = upsert_observations(conn, series_id, observations)
        print(f"  Upserted {count} rows into database")

    finally:
        conn.close()

    print(f"[{datetime.now(datetime.timezone.utc).isoformat()}] FRED ETL complete")


if __name__ == "__main__":
    main()
