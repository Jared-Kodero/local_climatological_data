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
    as_netcdf: bool = True,
    *,
    as_dataframe: bool = False,
    output: Optional[Union[str, Path]] = None,
    country: Optional[str] = "US",
    min_year_range: int = 1,
    stations_file: Union[str, Path] = STATIONS_FILE,
    base_url: str = BASE_URL,
    workers: int = 8,
    add_cities: bool = False,
    keep_raw: bool = False,
) -> Union[Path, pd.DataFrame]:
    """Download, clean, and return LCD records for a region and year range.

    Parameters
    ----------
    lon_min, lon_max, lat_min, lat_max : float
        Bounding box in degrees east and degrees north.
    min_year, max_year : int
        Inclusive calendar-year range.
    months : sequence of int, optional
        Retain only these calendar months (UTC). Applied during cleaning to
        reduce memory.
    classify_convective : bool, default True
        Add a 'prec_type' column via :mod:`lcd.classify`.
    as_netcdf : bool, default True
        If True, write a compressed (station, time) netCDF and return its path.
        If False, return the cleaned DataFrame.
    output : str or Path, optional
        Destination netCDF path when ``as_netcdf`` is True. Defaults to a name
        built from the bounding box and years in the working directory.
    add_cities : bool, default False
        Assign 'city' and 'state' from Natural Earth via cartopy.

    Returns
    -------
    pathlib.Path or pandas.DataFrame
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
        output=None,
        stations_file=stations_file,
        base_url=base_url,
        workers=workers,
        classify=classify_convective,
        months=months,
        keep_raw=keep_raw,
    )
    if add_cities:
        df = add_city_names(df)

    if not as_netcdf:
        return df

    if output is None:
        output = Path(
            f"lcd_{lat_min}_{lat_max}_{lon_min}_{lon_max}_{min_year}_{max_year}.nc"
        )
    return to_netcdf(df, output)


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
