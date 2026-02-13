import csv
import os
from datetime import date, timezone, datetime

from .config import get_config, VIEWS
from .db import get_connection, fetch_view


def is_weekday():
    return datetime.now(timezone.utc).weekday() < 5


def export_csv(rows, filepath):
    """Write rows to a CSV file with date,value columns."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "value"])
        for row in rows:
            writer.writerow([row[0].isoformat(), row[1]])


def main():
    config = get_config()
    conn = get_connection(config["database_url"])
    today = date.today()

    try:
        for name, view in VIEWS.items():
            rows = fetch_view(conn, view)
            filepath = os.path.join(config["output_dir"], f"{name}.csv")
            export_csv(rows, filepath)

            max_date = rows[-1][0] if rows else None
            if is_weekday() and max_date != today:
                print(f"WARNING: {name} max date is {max_date}, expected {today}")
            else:
                print(f"OK: {name} — {len(rows)} rows, max date {max_date}")
    finally:
        conn.close()

    print(f"Exported {len(VIEWS)} CSVs to {config['output_dir']}/")


if __name__ == "__main__":
    main()
