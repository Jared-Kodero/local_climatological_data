"""
Static configuration for the LCD package: endpoints, the hourly and daily
column schemas, short and long names with SI units, the present-weather code
groups used for optional convective classification, and a small per-frequency
specification registry (:data:`FREQS`) that drives acquisition and cleaning.

Available raw columns are those distributed in the NCEI LCDv2 station-year CSV
files; a representative file ships under ``lcd/data/example_download`` together
with the LCD documentation PDF. The mappings below select the hourly and daily
subsets used by this package.

Unit provenance (NCEI, Local Climatological Data v2 Dataset Documentation):
temperatures in whole degrees Fahrenheit, wind speed in miles per hour, wind
direction on a 360 degree compass from true north (000 = calm), precipitation,
snowfall, and snow depth in inches with "T" marking a trace (< 0.005 in),
station, sea level, and altimeter pressures and the 3-hour pressure change in
inches of mercury, and visibility in miles. All measurements are converted to
SI on ingest.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

# ----------------------------------------------------------------- Endpoints

TMP = Path("/tmp")  # node-local scratch

# Packaged data (station inventory and example download).
DATA_DIR = Path(__file__).resolve().parent / "data"
STATIONS_FILE = DATA_DIR / "airports.stations"
EXAMPLE_DIR = DATA_DIR / "example_download"

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access"

# ----------------------------------------------------------- Unit conversions

TRACE_INCHES = 0.005  # documented trace value (inches)
INCH_TO_MM = 25.4
MPH_TO_MS = 0.44704
INHG_TO_HPA = 33.8639  # 1 inHg = 33.8639 hPa
MILE_TO_KM = 1.609344

# ----------------------------------------------------- Shared column groups

# Per-station coordinate columns (single-valued per station).
COORD_COLUMNS: tuple[str, ...] = ("lat", "lon", "elev", "city", "state")

# Present-weather groups (au | aw | mw) parsed from HourlyPresentWeatherType and
# retained verbatim for user-defined classification.
WEATHER_COLUMNS: tuple[str, ...] = ("au", "aw", "mw")

# ================================ Hourly schema ============================

# Station identity and hourly fields only; daily, monthly, normals, and
# short-duration columns are discarded on read.
HOURLY_RAW_TO_SHORT: dict[str, str] = {
    "STATION": "station_id",
    "DATE": "time",
    "LATITUDE": "lat",
    "LONGITUDE": "lon",
    "ELEVATION": "elev",
    "NAME": "name",
    "REPORT_TYPE": "report_type",
    "HourlyDryBulbTemperature": "t",
    "HourlyDewPointTemperature": "dpt",
    "HourlyWetBulbTemperature": "wbt",
    "HourlyRelativeHumidity": "rh",
    "HourlyWindDirection": "wd",
    "HourlyWindSpeed": "ws",
    "HourlyWindGustSpeed": "wsg",
    "HourlySeaLevelPressure": "sp",
    "HourlyStationPressure": "stp",
    "HourlyAltimeterSetting": "alt",
    "HourlyPressureChange": "pchg",
    "HourlyPressureTendency": "ptend",
    "HourlyVisibility": "vis",
    "HourlyPrecipitation": "prec",
    "HourlySkyConditions": "skyc",
    "HourlyPresentWeatherType": "weather_type",
}

# Surface hourly and synoptic report types (SYNOP, METAR, SPECI). Daily (SOD)
# and monthly (SOM) summary rows are excluded by this filter.
HOURLY_REPORT_TYPES: frozenset[str] = frozenset({"FM-12", "FM-15", "FM-16"})

# Numeric hourly measurement columns carried as float32. 'ptend' is a
# categorical pressure-tendency code (0-8) but is stored numerically.
HOURLY_MEASURE_COLUMNS: tuple[str, ...] = (
    "t",
    "dpt",
    "wbt",
    "rh",
    "wd",
    "ws",
    "wsg",
    "sp",
    "stp",
    "alt",
    "pchg",
    "ptend",
    "vis",
    "prec",
)

# String hourly measurements stored as categorical codes in netCDF.
HOURLY_STRING_MEASURES: tuple[str, ...] = WEATHER_COLUMNS + ("skyc",)

# ================================= Daily schema ============================

# Station identity and daily-summary fields (populated only on SOD rows).
DAILY_RAW_TO_SHORT: dict[str, str] = {
    "STATION": "station_id",
    "DATE": "time",
    "LATITUDE": "lat",
    "LONGITUDE": "lon",
    "ELEVATION": "elev",
    "NAME": "name",
    "REPORT_TYPE": "report_type",
    "DailyMaximumDryBulbTemperature": "tmax",
    "DailyMinimumDryBulbTemperature": "tmin",
    "DailyAverageDryBulbTemperature": "tavg",
    "DailyAverageDewPointTemperature": "dpt",
    "DailyAverageWetBulbTemperature": "wbt",
    "DailyAverageRelativeHumidity": "rh",
    "DailyAverageStationPressure": "stp",
    "DailyAverageSeaLevelPressure": "sp",
    "DailyAverageWindSpeed": "ws",
    "DailyPeakWindSpeed": "wsg",
    "DailyPeakWindDirection": "wd",
    "DailySustainedWindSpeed": "ws_sust",
    "DailySustainedWindDirection": "wd_sust",
    "DailyPrecipitation": "prec",
    "DailySnowfall": "snow",
    "DailySnowDepth": "snwd",
    "DailyHeatingDegreeDays": "hdd",
    "DailyCoolingDegreeDays": "cdd",
    "DailyWeather": "wt",
}

# Daily summary report type (Summary Of Day).
DAILY_REPORT_TYPES: frozenset[str] = frozenset({"SOD"})

# Numeric daily measurement columns carried as float32. Precipitation, snowfall,
# and snow depth accept the documented "T" trace marker.
DAILY_MEASURE_COLUMNS: tuple[str, ...] = (
    "tmax",
    "tmin",
    "tavg",
    "dpt",
    "wbt",
    "rh",
    "stp",
    "sp",
    "ws",
    "wsg",
    "wd",
    "ws_sust",
    "wd_sust",
    "prec",
    "snow",
    "snwd",
    "hdd",
    "cdd",
)
DAILY_TRACE_COLUMNS: tuple[str, ...] = ("prec", "snow", "snwd")

# String daily measurements stored as categorical codes in netCDF.
DAILY_STRING_MEASURES: tuple[str, ...] = ("wt",)

# ------------------------------------------------- Object (string) columns

# Object-dtype columns coerced before reshaping and stored as netCDF
# categorical codes. A superset over both frequencies; membership is checked
# against the frame at run time.
STRING_COLUMNS: tuple[str, ...] = (
    ("city", "state", "station_id")
    + WEATHER_COLUMNS
    + ("skyc", "wt", "prec_type")
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
    # hourly
    "t": "degC",
    "dpt": "degC",
    "wbt": "degC",
    "rh": "%",
    "wd": "degrees",
    "ws": "m/s",
    "wsg": "m/s",
    "sp": "hPa",
    "stp": "hPa",
    "alt": "hPa",
    "pchg": "hPa",
    "vis": "km",
    "prec": "mm/hr",
    # daily
    "tmax": "degC",
    "tmin": "degC",
    "tavg": "degC",
    "ws_sust": "m/s",
    "wd_sust": "degrees",
    "snow": "mm",
    "snwd": "mm",
    "hdd": "degF-day",
    "cdd": "degF-day",
}

LONG_NAMES: dict[str, str] = {
    "time": "Observation Time",
    "station_id": "NOAA Station Identifier",
    "lat": "Station Latitude",
    "lon": "Station Longitude",
    "elev": "Station Elevation",
    "city": "Station City",
    "state": "Station State",
    # hourly
    "t": "Air Temperature",
    "dpt": "Dew Point Temperature",
    "wbt": "Wet Bulb Temperature",
    "rh": "Relative Humidity",
    "wd": "Wind Direction",
    "ws": "Wind Speed",
    "wsg": "Wind Gust Speed",
    "sp": "Sea Level Pressure",
    "stp": "Station Pressure",
    "alt": "Altimeter Setting",
    "pchg": "3-Hour Net Pressure Change",
    "ptend": "Pressure Tendency Code (0-8)",
    "vis": "Horizontal Visibility",
    "prec": "Precipitation",
    "skyc": "Sky Condition / Cloud Layers",
    "au": "Automated Present Weather (AU group)",
    "aw": "Automated Present Weather (AW group)",
    "mw": "Manual Present Weather (MW group)",
    "prec_type": "Precipitation regime (convective | stratiform | none)",
    # daily
    "tmax": "Daily Maximum Air Temperature",
    "tmin": "Daily Minimum Air Temperature",
    "tavg": "Daily Average Air Temperature",
    "ws_sust": "Daily Maximum Sustained Wind Speed",
    "wd_sust": "Daily Maximum Sustained Wind Direction",
    "snow": "Daily Snowfall",
    "snwd": "Daily Snow Depth",
    "hdd": "Heating Degree Days (base 65 degF)",
    "cdd": "Cooling Degree Days (base 65 degF)",
    "wt": "Daily Weather Type (GHCN-Daily WT codes)",
}

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


# ============================ Frequency registry ==========================


@dataclass(frozen=True)
class FreqSpec:
    """Per-frequency acquisition and cleaning specification.

    Attributes
    ----------
    name : {"hourly", "daily"}
        Temporal resolution identifier.
    report_types : frozenset[str]
        LCD report types retained for this frequency.
    raw_to_short : Mapping[str, str]
        Raw NCEI column names mapped to internal short names.
    measure_columns : tuple[str, ...]
        Numeric measurement columns carried as float32.
    string_measures : tuple[str, ...]
        Object-dtype measurement columns stored as netCDF categorical codes.
    """

    name: str
    report_types: frozenset[str]
    raw_to_short: Mapping[str, str]
    measure_columns: tuple[str, ...]
    string_measures: tuple[str, ...]

    @property
    def base_columns(self) -> tuple[str, ...]:
        """Cleaned output columns, in order, before optional classification."""
        return (
            ("time", "station_id")
            + COORD_COLUMNS
            + self.measure_columns
            + self.string_measures
        )


FREQS: dict[str, FreqSpec] = {
    "hourly": FreqSpec(
        name="hourly",
        report_types=HOURLY_REPORT_TYPES,
        raw_to_short=HOURLY_RAW_TO_SHORT,
        measure_columns=HOURLY_MEASURE_COLUMNS,
        string_measures=HOURLY_STRING_MEASURES,
    ),
    "daily": FreqSpec(
        name="daily",
        report_types=DAILY_REPORT_TYPES,
        raw_to_short=DAILY_RAW_TO_SHORT,
        measure_columns=DAILY_MEASURE_COLUMNS,
        string_measures=DAILY_STRING_MEASURES,
    ),
}


def get_freq_spec(freq: str) -> FreqSpec:
    """Return the :class:`FreqSpec` for ``freq`` ('hourly' or 'daily')."""
    try:
        return FREQS[freq]
    except KeyError:
        raise ValueError(
            f"Unknown freq {freq!r}; expected one of {sorted(FREQS)}."
        ) from None


# --------------------------------- Backwards-compatible hourly aliases

RAW_TO_SHORT: dict[str, str] = HOURLY_RAW_TO_SHORT
MEASURE_COLUMNS: tuple[str, ...] = HOURLY_MEASURE_COLUMNS
BASE_COLUMNS: tuple[str, ...] = FREQS["hourly"].base_columns
OUTPUT_COLUMNS: tuple[str, ...] = BASE_COLUMNS + ("prec_type",)
