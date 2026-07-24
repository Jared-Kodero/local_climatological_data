"""
User-facing entry points for NOAA Local Climatological Data.

get_lcd_from_noaa   download and clean records, returning an xarray Dataset
open_dataset        load a stored netCDF as an xarray Dataset
get_durations       per-station precipitation-event durations
get_lag             within-day lagged predictors
save_dataset        write a Dataset to compressed netCDF

All routines return :class:`xarray.Dataset` objects. Records are indexed by
(station, time); the station coordinate holds the NOAA identifier as a string.
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
    to_xarray,
)
from .schema import BASE_URL, LAGGABLE_COLUMNS, STATIONS_FILE, get_freq_spec

__all__ = [
    "get_lcd_from_noaa",
    "open_dataset",
    "save_dataset",
    "get_durations",
    "get_lag",
]

Freq = Literal["hourly", "daily"]


# ------------------------------------------------------------- Retrieval


def get_lcd_from_noaa(
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    min_year: int,
    max_year: int,
    freq: Freq = "hourly",
    months: Optional[Sequence[int]] = None,
    classify_convective: bool = True,
    *,
    country: Optional[str] = "US",
    min_year_range: int = 1,
    stations_file: Union[str, Path] = STATIONS_FILE,
    base_url: str = BASE_URL,
    workers: int = 8,
    scheduler: Literal["processes", "threads"] = "processes",
    add_cities: bool = False,
    keep_raw: bool = False,
    output: Optional[Union[str, Path]] = None,
) -> xr.Dataset:
    """Retrieve NOAA Local Climatological Data for a geographic region.

    Download station-year LCD files from NOAA, retain only the records matching
    the requested temporal frequency, clean and convert them to SI units, and
    return an :class:`xarray.Dataset` with dimensions (station, time).

    Parameters
    ----------
    lon_min, lon_max : float
        Western and eastern longitude bounds, respectively, in decimal degrees
        east. Longitudes west of the prime meridian are negative.
    lat_min, lat_max : float
        Southern and northern latitude bounds, respectively, in decimal degrees
        north.
    min_year, max_year : int
        First and last calendar years to include. Both bounds are inclusive.
    freq : {"hourly", "daily"}, default "hourly"
        Temporal frequency to retain. ``"hourly"`` keeps surface hourly and
        synoptic reports (FM-12, FM-15, FM-16) and returns the hourly variable
        set; ``"daily"`` keeps Summary of Day (SOD) rows and returns the daily
        summary variable set. Records belonging to the other frequency, and all
        monthly and normals rows, are discarded.
    months : sequence of int, optional
        Calendar months to retain, expressed as integers from 1 through 12.
        Filtering is applied during cleaning to reduce memory use. If None,
        retain all months.
    classify_convective : bool, default True
        Whether to classify precipitation observations and add the resulting
        ``prec_type`` variable. Applies to ``freq="hourly"`` only; ignored for
        daily summaries, which carry no sub-daily present-weather groups.
    country : str, optional, default "US"
        Country code used to restrict the station inventory. Set to None to
        disable country filtering.
    min_year_range : int, default 1
        Minimum number of years for which a station must satisfy the requested
        temporal coverage criteria.
    stations_file : str or pathlib.Path, default STATIONS_FILE
        Path to the station inventory used to identify stations in the region.
    base_url : str, default BASE_URL
        Base URL from which NOAA LCD station-year files are downloaded.
    workers : int, default 8
        Maximum number of concurrent workers used for downloading and cleaning.
        Values less than one are treated as one worker.
    scheduler : {"processes", "threads"}, default "processes"
        Dask scheduler used for parallel download and cleaning tasks.
    add_cities : bool, default False
        Whether to overwrite ``city`` and ``state`` using the nearest Natural
        Earth populated place rather than the LCD station name.
    keep_raw : bool, default False
        Whether to retain downloaded source files after processing.
    output : str or pathlib.Path, optional
        If given, the Dataset is additionally written to this path as
        compressed netCDF. Parent directories are created automatically.

    Returns
    -------
    xarray.Dataset
        Cleaned LCD records with dimensions (station, time). Latitude,
        longitude, elevation, city, and state are coordinates along the station
        dimension.

    Notes
    -----
    Hourly timestamps are converted from Local Standard Time to UTC. Daily
    summaries are keyed by their Local Standard calendar date and are not
    shifted. The function performs network requests and creates temporary files
    on disk.
    """
    spec = get_freq_spec(freq)
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
        freq=spec.name,
        keep_raw=keep_raw,
        scheduler=scheduler,
    )
    if add_cities:
        df = add_city_names(df)

    ds = to_xarray(df)
    ds.attrs["frequency"] = spec.name
    if output is not None:
        to_netcdf(ds, output)
    return ds


# ---------------------------------------------------------------- Loading


def open_dataset(path: Union[str, Path]) -> xr.Dataset:
    """Load a stored LCD netCDF file as an xarray Dataset.

    Parameters
    ----------
    path : str or pathlib.Path
        Path to a netCDF file written by :func:`get_lcd_from_noaa` or
        :func:`save_dataset`.

    Returns
    -------
    xarray.Dataset
        Decoded (station, time) Dataset. Categorical fields stored as integer
        codes are decoded back to strings on read.
    """
    path = Path(path)
    if not path.exists():
        raise DataNotFoundError(f"File not found: {path}")
    return open_xarray(path)


def save_dataset(ds: xr.Dataset, path: Union[str, Path]) -> Path:
    """Write a Dataset to compressed netCDF and return the path."""
    return to_netcdf(ds, path)


# ------------------------------------------------------ Event durations


def get_durations(
    data: Union[xr.Dataset, str, Path],
    gap_tolerance_hours: float = 1.0,
    sampling_interval_hours: float = 1.0,
    output: Optional[Union[str, Path]] = None,
) -> xr.Dataset:
    """Per-station precipitation-event durations, in minutes.

    An event is a contiguous run of ``prec > 0``; a gap exceeding
    ``gap_tolerance_hours`` terminates it. Duration is
    ``n_obs * sampling_interval_hours * 60``, each observation representing one
    accumulation interval (Eagleson 1972; Restrepo-Posada and Eagleson 1982).

    Parameters
    ----------
    data : xarray.Dataset, str, or pathlib.Path
        An hourly LCD Dataset or a path to a stored netCDF file.
    gap_tolerance_hours : float, default 1.0
        Maximum gap, in hours, tolerated within a single event.
    sampling_interval_hours : float, default 1.0
        Accumulation interval represented by each observation, in hours.
    output : str or pathlib.Path, optional
        If given, the result is additionally written to this path.

    Returns
    -------
    xarray.Dataset
        Input variables plus a ``duration`` variable, in minutes.
    """
    df = _as_frame(data)
    parts = [
        _calc_durations(g, gap_tolerance_hours, sampling_interval_hours)
        for _, g in df.groupby("station_id")
    ]
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    ds = to_xarray(out)
    ds["duration"].attrs.update(long_name="Precipitation Event Duration", units="min")
    if output is not None:
        to_netcdf(ds, output)
    return ds


# ------------------------------------------------------------ Lagging


def get_lag(
    data: Union[xr.Dataset, str, Path],
    lag: int = 1,
    output: Optional[Union[str, Path]] = None,
) -> xr.Dataset:
    """Within-day lag of thermodynamic and kinematic variables.

    Lagging is applied per (station_id, calendar day) so values never cross day
    boundaries; mid-day gaps are forward-filled within the day.

    Parameters
    ----------
    data : xarray.Dataset, str, or pathlib.Path
        An LCD Dataset or a path to a stored netCDF file.
    lag : int, default 1
        Number of observations to lag by.
    output : str or pathlib.Path, optional
        If given, the result is additionally written to this path.

    Returns
    -------
    xarray.Dataset
        Dataset with the laggable variables replaced by their lagged values.
    """
    df = _as_frame(data)
    parts = [_apply_lag(g, lag=lag) for _, g in df.groupby("station_id")]
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    ds = to_xarray(out)
    ds.attrs["lag"] = lag
    if output is not None:
        to_netcdf(ds, output)
    return ds


# ============================== Internal helpers ===========================


def _as_frame(data: Union[xr.Dataset, str, Path, pd.DataFrame]) -> pd.DataFrame:
    """Normalise Dataset, path, or frame input to a long DataFrame."""
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, xr.Dataset):
        df = data.to_dataframe().reset_index()
        if "station" in df.columns:
            df = df.rename(columns={"station": "station_id"})
        if "prec" in df.columns:
            df = df.dropna(subset=["prec"])
        return df.reset_index(drop=True)
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
