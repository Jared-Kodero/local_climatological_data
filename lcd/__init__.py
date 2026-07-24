"""
lcd: download, clean, and load NOAA Local Climatological Data (LCDv2).

All user-facing routines return :class:`xarray.Dataset` objects with dimensions
(station, time).

Quick start
-----------
Retrieve hourly records for a region (timestamps are UTC)::

    import lcd

    ds = lcd.get_lcd_from_noaa(
        lon_min=-73, lon_max=-66, lat_min=40, lat_max=50,
        min_year=1980, max_year=2024,
        freq="hourly",               # or "daily"
        months=[6, 7, 8],            # optional calendar-month filter
        classify_convective=True,
    )

Retrieve daily summaries instead (timestamps are Local Standard dates)::

    ds = lcd.get_lcd_from_noaa(
        lon_min=-73, lon_max=-66, lat_min=40, lat_max=50,
        min_year=1980, max_year=2024,
        freq="daily",
    )

Write and reload a stored file::

    lcd.save_dataset(ds, "lcd.nc")
    ds = lcd.open_dataset("lcd.nc")

Derive event durations or lagged predictors::

    durations = lcd.get_durations(ds, output="durations.nc")
    lagged = lcd.get_lag(ds, lag=1, output="lag1.nc")

Classify precipitation regimes from the retained au/aw/mw groups::

    from lcd.classify import add_precip_type
    df = add_precip_type(df)
"""

from . import classify, schema
from .cities import add_city_names
from .core import (
    get_durations,
    get_lag,
    get_lcd_from_noaa,
    open_dataset,
    save_dataset,
)
from .download import (
    DataNotFoundError,
    EmptyDataFrameError,
    Region,
    build,
    clean_directory,
    download_stations,
    lst_to_utc,
    open_xarray,
    read_and_clean,
    read_netcdf,
    select_stations,
    to_local_time,
    to_netcdf,
    to_xarray,
)
from .schema import FREQS, EXAMPLE_DIR, get_freq_spec

__version__ = "0.3.0"

__all__ = [
    "get_lcd_from_noaa",
    "open_dataset",
    "save_dataset",
    "get_durations",
    "get_lag",
    "add_city_names",
    "classify",
    "schema",
    "Region",
    "build",
    "select_stations",
    "download_stations",
    "clean_directory",
    "read_and_clean",
    "lst_to_utc",
    "to_local_time",
    "to_xarray",
    "to_netcdf",
    "read_netcdf",
    "open_xarray",
    "FREQS",
    "EXAMPLE_DIR",
    "get_freq_spec",
    "DataNotFoundError",
    "EmptyDataFrameError",
    "__version__",
]
