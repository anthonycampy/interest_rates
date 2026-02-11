import requests

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_observations(api_key, series_id, observation_start=None):
    """
    Fetch observations from the FRED API.

    Args:
        api_key: FRED API key
        series_id: e.g. "DGS30"
        observation_start: Optional "YYYY-MM-DD" string. If provided, only
                           fetch observations on or after this date.

    Returns:
        List of dicts: [{"date": "YYYY-MM-DD", "value": "4.25", ...}, ...]
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    if observation_start:
        params["observation_start"] = observation_start

    response = requests.get(FRED_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    return data.get("observations", [])
