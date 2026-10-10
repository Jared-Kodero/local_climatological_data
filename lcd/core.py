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
    _validate_months,
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
    progress: bool = True,
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
        Filtering is applied during cleaning to reduce memory use. For
        ``freq="hourly"`` the month is that of the UTC timestamp (a record from
        the evening of the last day of a month in Local Standard Time can fall
        in the next UTC month); for ``freq="daily"`` it is that of the Local
        Standard date. If None, retain all months.
    classify_convective : bool, default True
        Whether to classify precipitation observations and add the resulting
        ``prec_type`` variable. Applies to ``freq="hourly"`` only; ignored for
        daily summaries, which carry no sub-daily present-weather groups.
    country : str, optional, default "US"
        Country code used to restrict the station inventory. Set to None to
        disable country filtering.
    min_year_range : int, default 1
        Minimum length, in years, of a station's inventory period of record
        (last year minus first year). The period of record must in addition
        overlap ``min_year`` to ``max_year``.
    stations_file : str or pathlib.Path, default STATIONS_FILE
        Path to the station inventory used to identify stations in the region.
    base_url : str, default BASE_URL
        Base URL from which NOAA LCD station-year files are downloaded.
    workers : int, default 8
        Number of concurrent workers used for cleaning. Values less than one
        are treated as one worker, which cleans serially without a pool.
        Downloads are network-bound and always use at least 16 threads.
    scheduler : {"processes", "threads"}, default "processes"
        Dask scheduler used for the parallel cleaning tasks.
    add_cities : bool, default False
        Whether to overwrite ``city`` and ``state`` using the nearest Natural
        Earth populated place rather than the LCD station name.
    keep_raw : bool, default False
        Whether to retain downloaded source files after processing.
    output : str or pathlib.Path, optional
        If given, the Dataset is additionally written to this path as
        compressed netCDF. Parent directories are created automatically.
    progress : bool, default True
        Whether to print download and cleaning progress to stderr.

    Returns
    -------
    xarray.Dataset
        Cleaned LCD records with dimensions (station, time). Latitude,
        longitude, elevation, city, and state are coordinates along the station
        dimension.

    Raises
    ------
    ValueError
        If ``freq``, ``months`` or the region/year bounds are invalid.
    EmptyDataFrameError
        If no station matches the region, no file could be downloaded, or no
        record remains after cleaning.

    Notes
    -----
    Hourly timestamps are converted from Local Standard Time to UTC. Daily
    summaries are keyed by their Local Standard calendar date and are not
    shifted. Hourly records that report no precipitation are discarded. Station
    files are requested as ``<USAF><WBAN>.csv`` with both identifiers
    zero-padded; downloads that fail after retries are logged and make the
    result incomplete. The function performs network requests and creates
    temporary files on disk.
    """
    spec = get_freq_spec(freq)
    months = _validate_months(months)
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
        progress=progress,
    )
    if add_cities:
        df = add_city_names(df)

    ds = to_xarray(df, freq=spec.name)
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
    out = _durations_frame(df, gap_tolerance_hours, sampling_interval_hours)
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
    out = _lagged_frame(df, lag)
    out = out.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    ds = to_xarray(out)
    ds.attrs["lag"] = lag
    if output is not None:
        to_netcdf(ds, output)
    return ds


# ============================== Internal helpers ===========================


def _as_frame(data: Union[xr.Dataset, str, Path, pd.DataFrame]) -> pd.DataFrame:
    """Normalise Dataset, path, or frame input to a long DataFrame.

    For Datasets the (station, time) grid is flattened to one row per cell that
    reports precipitation (all cells if there is no ``prec`` variable); padding
    cells are never materialised.
    """
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, xr.Dataset):
        return _dataset_to_frame(data)
    return read_netcdf(data)


def _dataset_to_frame(ds: xr.Dataset) -> pd.DataFrame:
    grid = ("station", "time")
    plain = (
        "station" in ds.dims
        and "time" in ds.dims
        and all(v.dims in (grid, ("station",), ("time",)) for v in ds.variables.values())
    )
    if not plain:  # unusual layout: let xarray broadcast
        df = ds.to_dataframe().reset_index()
        if "station" in df.columns:
            df = df.rename(columns={"station": "station_id"})
        if "prec" in df.columns:
            df = df.dropna(subset=["prec"])
        return df.reset_index(drop=True)

    if "prec" in ds.data_vars:
        mask = ds["prec"].transpose(*grid).notnull().values
    else:
        mask = np.ones((ds.sizes["station"], ds.sizes["time"]), dtype=bool)
    si, ti = np.nonzero(mask)

    cols: dict[str, np.ndarray] = {
        "station_id": ds["station"].values[si],
        "time": ds["time"].values[ti],
    }
    for name, coord in ds.coords.items():
        if name in ("station", "time"):
            continue
        if coord.dims == ("station",):
            cols[str(name)] = coord.values[si]
        elif coord.dims == ("time",):
            cols[str(name)] = coord.values[ti]
    for name, var in ds.data_vars.items():
        if var.dims == grid:
            cols[str(name)] = var.values[si, ti]
        elif var.dims == ("station",):
            cols[str(name)] = var.values[si]
        else:
            cols[str(name)] = var.values[ti]
    return pd.DataFrame(cols)


def _lagged_frame(df: pd.DataFrame, lag: int) -> pd.DataFrame:
    """Lag the laggable columns within each (station, calendar day)."""
    cols = [c for c in LAGGABLE_COLUMNS if c in df.columns]
    if not cols:
        return df.copy()
    df = df.sort_values(["station_id", "time"], kind="stable").reset_index(drop=True)
    keys = [df["station_id"], df["time"].dt.normalize()]
    lagged = df.groupby(keys, sort=False)[cols].shift(lag)
    lagged = lagged.groupby(keys, sort=False).ffill()
    df[cols] = lagged
    return df


def _durations_frame(
    df: pd.DataFrame,
    gap_tolerance_hours: float,
    sampling_interval_hours: float,
) -> pd.DataFrame:
    """Add ``duration`` (minutes) to every station's precipitation events.

    Vectorised over all stations at once: rows are ordered by (station, time),
    and an event starts at a wet row whose predecessor in the same station is
    dry, or is more than ``gap_tolerance_hours`` earlier.
    """
    df = df.sort_values(["station_id", "time"], kind="stable").reset_index(drop=True)
    n = len(df)
    if n == 0:
        df["duration"] = np.array([], dtype=float)
        return df

    station = df["station_id"].to_numpy()
    first = np.ones(n, dtype=bool)
    first[1:] = station[1:] != station[:-1]

    dt_hours = df["time"].diff().dt.total_seconds().to_numpy() / 3600.0
    dt_hours = np.where(first, sampling_interval_hours, dt_hours)

    wet = df["prec"].to_numpy() > 0  # NaN compares False
    prev_wet = np.zeros(n, dtype=bool)
    prev_wet[1:] = wet[:-1] & ~first[1:]
    new_event = wet & (~prev_wet | (dt_hours > gap_tolerance_hours))

    event_id = np.where(wet, np.cumsum(new_event), 0)
    counts = np.bincount(event_id)
    duration = np.where(wet, counts[event_id] * sampling_interval_hours * 60.0, 0.0)
    duration[df["prec"].isna().to_numpy()] = np.nan

    df["duration"] = duration
    return df.dropna(subset=["prec"])
