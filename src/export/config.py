import os
import sys


VIEWS = {
    "dgs1": "v_dgs1",
    "dgs2": "v_dgs2",
    "dgs5": "v_dgs5",
    "dgs10": "v_dgs10",
    "dgs30": "v_dgs30",
}


def get_config():
    """Load configuration from environment variables."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: Missing required environment variable: DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    return {
        "database_url": database_url,
        "output_dir": os.environ.get("OUTPUT_DIR", "./data"),
    }
