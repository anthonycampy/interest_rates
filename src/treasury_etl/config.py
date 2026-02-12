import os
import sys


def get_config():
    """Load configuration from environment variables."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: Missing required environment variable: DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    return {
        "database_url": database_url,
    }
