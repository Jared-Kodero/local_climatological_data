"""
lcd: download, clean, and load NOAA Local Climatological Data.

Quick start
-----------
Download a region and write a compressed netCDF (timestamps are UTC)::

    import lcd

    path = lcd.get_lcd_from_noaa(
        lon_min=-73, lon_max=-66, lat_min=40, lat_max=50,
        min_year=1980, max_year=2024,
        months=[6, 7, 8],            # optional calendar-month filter
        classify_convective=True,
        as_netcdf=True,              # returns the .nc path; False returns a DataFrame
    )

Load a stored file::

    df = lcd.open_dataset(path, engine="pandas")     # long DataFrame
    ds = lcd.open_dataset(path, engine="netcdf")     # xarray Dataset (station, time)

Derive and save event durations or lagged predictors::

    lcd.get_durations(df, output="durations.nc")
    lcd.get_lag(df, lag=1, output="lag1.nc")

Classify precipitation regimes yourself from the retained au/aw/mw groups::

    from lcd.classify import add_precip_type
    df = add_precip_type(df)
"""

from . import classify
from .cities import add_city_names
from .core import get_durations, get_lag, get_lcd_from_noaa, open_dataset
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

__all__ = [
    "get_lcd_from_noaa",
    "open_dataset",
    "get_durations",
    "get_lag",
    "add_city_names",
    "classify",
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
    "DataNotFoundError",
    "EmptyDataFrameError",
]
