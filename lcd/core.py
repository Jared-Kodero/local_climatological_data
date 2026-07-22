"""
User-facing entry points for NOAA Local Climatological Data.

get_lcd_from_noaa   download, clean, and return records as netCDF or DataFrame
open_dataset        load a stored file as a pandas DataFrame or xarray Dataset
get_durations       per-station precipitation-event durations (savable)
get_lag             within-day lagged predictors (savable)
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional, Sequence, Union

import numpy as np
import pandas as pd
import xarray as xr

from .cities import add_city_names
from .download import (
    DataNotFoundError,
    Region,
    build,
    open_xarray,
    read_netcdf,
    to_netcdf,
)
from .schema import BASE_URL, LAGGABLE_COLUMNS, STATIONS_FILE

__all__ = [
    "get_lcd_from_noaa",
    "open_dataset",
    "get_durations",
    "get_lag",
]


# ------------------------------------------------------------- Retrieval


def get_lcd_from_noaa(
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    min_year: int,
    max_year: int,
    months: Optional[Sequence[int]] = None,
    classify_convective: bool = True,
    engine: Literal["pandas", "netcdf"] = "netcdf",
    *,
    country: Optional[str] = "US",
    min_year_range: int = 1,
    stations_file: Union[str, Path] = STATIONS_FILE,
    base_url: str = BASE_URL,
    workers: int = 8,
    scheduler: Literal["processes", "threads"] = "processes",
    add_cities: bool = False,
    keep_raw: bool = False,
) -> xr.Dataset | pd.DataFrame:
    """Retrieve NOAA Local Climatological Data for a geographic region.

    Download station-year LCD files from NOAA, clean and subset the hourly
    observations, and return the result as either a pandas DataFrame or an
    xarray Dataset. Records are restricted by geographic bounds, calendar
    years, country, and optionally calendar month.

    Parameters
    ----------
    lon_min, lon_max : float
        Western and eastern longitude bounds, respectively, in decimal
        degrees east. Longitudes west of the prime meridian are negative.
    lat_min, lat_max : float
        Southern and northern latitude bounds, respectively, in decimal
        degrees north.
    min_year, max_year : int
        First and last calendar years to include. Both bounds are inclusive.
    months : sequence of int, optional
        Calendar months to retain, expressed as integers from 1 through 12.
        Filtering is applied during data cleaning to reduce memory use. If
        None, retain all months.
    classify_convective : bool, default True
        Whether to classify precipitation observations and add the resulting
        ``prec_type`` variable or column.
    engine : {"pandas", "netcdf"}, default "netcdf"
        Output representation. ``"pandas"`` returns a
        :class:`pandas.DataFrame`; ``"netcdf"`` converts the cleaned records
        to an :class:`xarray.Dataset`.
    outfile : str or pathlib.Path, optional
        Output path. For the pandas engine, the data are written as CSV using
        a ``.csv`` suffix. For the netCDF engine, this path is passed to the
        netCDF conversion routine. Parent directories are created
        automatically. If None, no CSV file is written.
    country : str, optional, default "US"
        Country code used to restrict the station inventory. Set to None to
        disable country filtering.
    min_year_range : int, default 1
        Minimum number of years for which a station must satisfy the requested
        temporal coverage criteria.
    stations_file : str or pathlib.Path, default STATIONS_FILE
        Path to the station inventory used to identify stations within the
        requested region.
    base_url : str, default BASE_URL
        Base URL from which NOAA LCD station-year files are downloaded.
    workers : int, default 8
        Maximum number of concurrent workers used for downloading and cleaning.
        Values less than one are treated as one worker.
    scheduler : {"processes", "threads"}, default "processes"
        Dask scheduler used for parallel data-cleaning tasks.
    add_cities : bool, default False
        Whether to add ``city`` and ``state`` fields based on the station
        coordinates.
    keep_raw : bool, default False
        Whether to retain downloaded source files after processing.

    Returns
    -------
    pandas.DataFrame
        Cleaned LCD observations when ``engine="pandas"``.
    xarray.Dataset
        Cleaned LCD observations converted to an xarray Dataset when
        ``engine="netcdf"``.

    Notes
    -----
    The function may perform network requests and create files or directories
    on disk. Temporal filtering is applied after station selection and during
    cleaning of the downloaded observations.
    """
    region = Region(
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        min_year,
        max_year,
        country=country,
        min_year_range=min_year_range,
    )
    df = build(
        region,
        stations_file=stations_file,
        base_url=base_url,
        workers=workers,
        classify=classify_convective,
        months=months,
        keep_raw=keep_raw,
        scheduler=scheduler,
    )
    if add_cities:
        df = add_city_names(df)

    return to_netcdf(df, engine)


# ---------------------------------------------------------------- Loading


def open_dataset(
    path: Union[str, Path],
    engine: Literal["pandas", "netcdf"] = "pandas",
) -> Union[pd.DataFrame, xr.Dataset]:
    """Load a stored LCD netCDF file.

    Parameters
    ----------
    path : str or Path
        Path to a netCDF file written by :func:`get_lcd_from_noaa`.
    engine : {"pandas", "netcdf"}, default "pandas"
        "pandas" returns a long DataFrame (padding removed); "netcdf" returns
        the decoded (station, time) xarray Dataset.
    """
    path = Path(path)
    if not path.exists():
        raise DataNotFoundError(f"File not found: {path}")
    if engine == "pandas":
        return read_netcdf(path)
    if engine == "netcdf":
        return open_xarray(path)
    raise ValueError(f"Unknown engine: {engine!r}")


# ------------------------------------------------------ Event durations


def get_durations(
    data: Union[pd.DataFrame, str, Path],
    gap_tolerance_hours: float = 1.0,
    sampling_interval_hours: float = 1.0,
    output: Optional[Union[str, Path]] = None,
) -> Union[pd.DataFrame, Path]:
    """Per-station precipitation-event durations, in minutes.

    An event is a contiguous run of prec > 0; a gap exceeding
    ``gap_tolerance_hours`` terminates it. Duration is
    n_obs * sampling_interval_hours * 60, each observation representing one
    accumulation interval (Eagleson 1972; Restrepo-Posada and Eagleson 1982).
    When ``output`` is given the result is written to netCDF and its path
    returned; otherwise a DataFrame with a 'duration' column is returned.
    """
    df = _as_frame(data)
    parts = [
        _calc_durations(g, gap_tolerance_hours, sampling_interval_hours)
        for _, g in df.groupby("station_id")
    ]
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    if output is not None:
        return to_netcdf(out, output)
    return out


# ------------------------------------------------------------ Lagging


def get_lag(
    data: Union[pd.DataFrame, str, Path],
    lag: int = 1,
    output: Optional[Union[str, Path]] = None,
) -> Union[pd.DataFrame, Path]:
    """Within-day lag of thermodynamic and kinematic variables.

    Lagging is applied per (station_id, calendar day) so values never cross
    day boundaries; mid-day gaps are forward-filled within day. When
    ``output`` is given the result is written to netCDF and its path returned.
    """
    df = _as_frame(data)
    parts = [_apply_lag(g, lag=lag) for _, g in df.groupby("station_id")]
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    if output is not None:
        return to_netcdf(out, output)
    return out


# ============================== Internal helpers ===========================


def _as_frame(data: Union[pd.DataFrame, str, Path]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    return read_netcdf(data)


def _apply_lag(df: pd.DataFrame, lag: int) -> pd.DataFrame:
    cols = [c for c in LAGGABLE_COLUMNS if c in df.columns]
    if not cols:
        return df.copy()
    df = df.sort_values("time").copy()
    df["__day__"] = df["time"].dt.date
    lagged = df.groupby("__day__")[cols].shift(lag)
    lagged = lagged.groupby(df["__day__"]).ffill()
    df[cols] = lagged
    return df.drop(columns=["__day__"])


def _calc_durations(
    df: pd.DataFrame,
    gap_tolerance_hours: float,
    sampling_interval_hours: float,
) -> pd.DataFrame:
    df = df.sort_values("time").reset_index(drop=True)
    dt_hours = df["time"].diff().dt.total_seconds() / 3600.0
    dt_hours = dt_hours.fillna(sampling_interval_hours)

    is_wet = df["prec"] > 0
    prev_dry = ~is_wet.shift(fill_value=False)
    long_gap = dt_hours > gap_tolerance_hours
    new_event = is_wet & (prev_dry | long_gap)

    df["event_id"] = new_event.cumsum().where(is_wet, 0)
    counts = df.groupby("event_id").size()
    duration_min = (counts * sampling_interval_hours * 60.0).rename("duration")

    df = df.merge(duration_min, left_on="event_id", right_index=True, how="left")
    df.loc[df["event_id"] == 0, "duration"] = 0.0
    df.loc[df["prec"].isna(), "duration"] = np.nan
    return df.drop(columns=["event_id"]).dropna(subset=["prec"])
