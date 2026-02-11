import os
import sys


def get_config():
    """Load configuration from environment variables. Exit if any are missing."""
    required = {
        "FRED_API_KEY": os.environ.get("FRED_API_KEY"),
        "DATABASE_URL": os.environ.get("DATABASE_URL"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(
            f"ERROR: Missing required environment variables: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    return {
        "fred_api_key": required["FRED_API_KEY"],
        "database_url": required["DATABASE_URL"],
        "series_id": os.environ.get("FRED_SERIES_ID", "DGS30"),
    }
