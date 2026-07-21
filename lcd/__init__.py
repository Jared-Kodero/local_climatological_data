"""
lcd: download, clean, and load NOAA Local Climatological Data.

Quick start
-----------
Download a region and write compressed netCDF, then work with the result::

    from lcd import LocalClimatologicalData

    data = LocalClimatologicalData.from_noaa(
        lat_min=25, lat_max=50, lon_min=-125, lon_max=-65,
        start_year=1980, end_year=2024,
        output="~/data/noaa/station.usa.nc",
    )

Load a previously written file (timestamps are UTC)::

    data = LocalClimatologicalData.open_data("~/data/noaa/station.usa.nc")

Subset, then derive event durations or lagged predictors::

    jja = data.sel(convective=True, season="JJA", bbox=(40, 50, -73, -66))
    dur = jja.get_durations()
    lagged = jja.lag(1)

Classify precipitation regimes yourself from the retained au/aw/mw groups::

    from lcd.classify import add_precip_type
    data = add_precip_type(data)
"""

from . import classify
from .core import LocalClimatologicalData
from .download import (
    DataNotFoundError,
    EmptyDataFrameError,
    Region,
    build,
    clean_directory,
    download_stations,
    lst_to_utc,
    read_and_clean,
    read_netcdf,
    select_stations,
    to_netcdf,
    to_xarray,
)

__all__ = [
    "LocalClimatologicalData",
    "Region",
    "build",
    "select_stations",
    "download_stations",
    "clean_directory",
    "read_and_clean",
    "lst_to_utc",
    "to_xarray",
    "to_netcdf",
    "read_netcdf",
    "classify",
    "DataNotFoundError",
    "EmptyDataFrameError",
]
