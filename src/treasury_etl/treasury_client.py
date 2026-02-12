import xml.etree.ElementTree as ET
from datetime import date

import requests

TREASURY_BASE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/"
    "interest-rates/pages/xml"
)

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
}

MATURITY_FIELDS = [
    "BC_1MONTH",
    "BC_1_5MONTH",
    "BC_2MONTH",
    "BC_3MONTH",
    "BC_4MONTH",
    "BC_6MONTH",
    "BC_1YEAR",
    "BC_2YEAR",
    "BC_3YEAR",
    "BC_5YEAR",
    "BC_7YEAR",
    "BC_10YEAR",
    "BC_20YEAR",
    "BC_30YEAR",
]

FIELD_TO_COLUMN = {f: f.lower() for f in MATURITY_FIELDS}


def fetch_yield_curve(year, month=None):
    """
    Fetch daily Treasury yield curve data for a given year (and optional month).

    Returns list of dicts with 'date' and lowercased maturity column keys.
    """
    params = {
        "data": "daily_treasury_yield_curve",
        "field_tdr_date_value": str(year),
    }
    if month is not None:
        params["field_tdr_date_value_month"] = f"{year}{month:02d}"

    response = requests.get(TREASURY_BASE_URL, params=params, timeout=60)
    response.raise_for_status()

    return _parse_xml(response.content)


def _parse_xml(xml_bytes):
    """Parse Treasury XML feed into list of row dicts."""
    root = ET.fromstring(xml_bytes)
    rows = []

    for entry in root.findall("atom:entry", NS):
        props = entry.find("atom:content/m:properties", NS)
        if props is None:
            continue

        date_text = props.findtext("d:NEW_DATE", namespaces=NS)
        if not date_text:
            continue
        row_date = date.fromisoformat(date_text.split("T")[0])

        row = {"date": row_date}
        for field in MATURITY_FIELDS:
            text = props.findtext(f"d:{field}", namespaces=NS)
            col = FIELD_TO_COLUMN[field]
            row[col] = float(text) if text else None

        rows.append(row)

    return rows
