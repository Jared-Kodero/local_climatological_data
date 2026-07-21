"""
Static configuration for the LCD package: endpoints, the hourly column
schema, short and long names with SI units aligned to the project's existing
station files, and the present-weather code groups used for optional
convective classification.

Unit provenance (NCEI, Local Climatological Data Dataset Documentation):
temperatures in whole degrees Fahrenheit, wind speed in miles per hour, wind
direction on a 360 degree compass from true north (000 = calm), precipitation
in inches with "T" marking a trace (< 0.005 in), station, sea level, and
altimeter pressures in inches of mercury, and visibility in miles. All fields
are converted to SI on ingest.
"""

from __future__ import annotations

from pathlib import Path

# ----------------------------------------------------------------- Endpoints

HOME = Path.home()
TMP = Path("/tmp")  # node-local scratch

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access"
STATIONS_FILE = HOME / "research/scripts/noaa/airports.stations"

# ----------------------------------------------------- Raw -> internal names

# Station identity and hourly fields only; daily, monthly, normals, and
# short-duration columns are discarded on read.
RAW_TO_SHORT: dict[str, str] = {
    "STATION": "station_id",
    "DATE": "time",
    "LATITUDE": "lat",
    "LONGITUDE": "lon",
    "ELEVATION": "elev",
    "NAME": "name",
    "REPORT_TYPE": "report_type",
    "HourlyDryBulbTemperature": "t",
    "HourlyDewPointTemperature": "dpt",
    "HourlyRelativeHumidity": "rh",
    "HourlyWindDirection": "wd",
    "HourlyWindSpeed": "ws",
    "HourlyWindGustSpeed": "wsg",
    "HourlySeaLevelPressure": "sp",
    "HourlyStationPressure": "stp",
    "HourlyVisibility": "vis",
    "HourlyPrecipitation": "prec",
    "HourlyPresentWeatherType": "weather_type",
}

# Surface hourly and synoptic report types (METAR, SPECI, SYNOP). Daily and
# monthly summary rows (SOD, SOM) are excluded by this filter.
HOURLY_REPORT_TYPES: frozenset[str] = frozenset({"FM-12", "FM-15", "FM-16"})

# Per-station coordinate columns (single-valued per station).
COORD_COLUMNS: tuple[str, ...] = ("lat", "lon", "elev", "city", "state")

# Numeric measurement columns carried as float32.
MEASURE_COLUMNS: tuple[str, ...] = (
    "t",
    "dpt",
    "rh",
    "wd",
    "ws",
    "wsg",
    "sp",
    "stp",
    "vis",
    "pr",
)

# Present-weather groups retained verbatim for user-defined classification.
WEATHER_COLUMNS: tuple[str, ...] = ("au", "aw", "mw")

# Final cleaned schema, in output order. 'prec_type' is appended when
# classification is applied.
BASE_COLUMNS: tuple[str, ...] = (
    ("time", "station_id") + COORD_COLUMNS + MEASURE_COLUMNS + WEATHER_COLUMNS
)
OUTPUT_COLUMNS: tuple[str, ...] = BASE_COLUMNS + ("prec_type",)

STRING_COLUMNS: tuple[str, ...] = (
    ("station_id", "city", "state") + WEATHER_COLUMNS + ("prec_type",)
)

# Columns eligible for within-day lagging (CAPE originates from ERA5 and is
# only present after an external merge).
LAGGABLE_COLUMNS: tuple[str, ...] = ("t", "dpt", "rh", "cape", "sp", "ws", "wd")

# ----------------------------------------------------------- Unit metadata

UNITS: dict[str, str] = {
    "time": "UTC",
    "lat": "degrees_north",
    "lon": "degrees_east",
    "elev": "m",
    "t": "degC",
    "dpt": "degC",
    "rh": "%",
    "wd": "degrees",
    "ws": "m/s",
    "wsg": "m/s",
    "sp": "hPa",
    "stp": "hPa",
    "vis": "km",
    "p": "mm/hr",
}

LONG_NAMES: dict[str, str] = {
    "time": "Observation Time (UTC)",
    "station_id": "NOAA Station Identifier",
    "lat": "Station Latitude",
    "lon": "Station Longitude",
    "elev": "Station Elevation",
    "city": "Station City",
    "state": "Station State",
    "t": "Air Temperature",
    "dpt": "Dew Point Temperature",
    "rh": "Relative Humidity",
    "wd": "Wind Direction",
    "ws": "Wind Speed",
    "wsg": "Wind Gust Speed",
    "sp": "Sea Level Pressure",
    "stp": "Station Pressure",
    "vis": "Horizontal Visibility",
    "p": "Hourly Precipitation Intensity",
    "au": "Automated Present Weather (AU group)",
    "aw": "Automated Present Weather (AW group)",
    "mw": "Manual Present Weather (MW group)",
    "prec_type": "Precipitation regime (convective | stratiform | none)",
}

# Conversions from LCD reporting units to SI.
TRACE_INCHES = 0.005  # documented trace value
INCH_TO_MM = 25.4
MPH_TO_MS = 0.44704
INHG_TO_HPA = 33.8639  # 1 inHg = 33.8639 hPa
MILE_TO_KM = 1.609344

# --------------------------------------------------- Convective classification

# Present-weather tokens (au | aw | mw groups) treated as convective. See the
# LCD Present Weather Appendix for the full code tables. Adjust here or supply
# custom code sets to lcd.classify functions.
CONVECTIVE_CODES: dict[str, frozenset[str]] = {
    "au": frozenset({"TS", "TS:7", "SH:7", "SH:6", "VCTS:7", "FC:3", "SQ:2"}),
    "aw": frozenset({"SHRA", "SHSN", "HAIL", "TS", "TS HAIL", "TS+HAIL", "+FC"}),
    "mw": frozenset({"SHRA", "SHRASN", "SHSN", "SH", "TS"}),
}


def variable_attrs(name: str) -> dict[str, str]:
    """Return CF-style attributes (units, long_name) for a variable."""
    attrs: dict[str, str] = {}
    if name in UNITS:
        attrs["units"] = UNITS[name]
    if name in LONG_NAMES:
        attrs["long_name"] = LONG_NAMES[name]
    return attrs
