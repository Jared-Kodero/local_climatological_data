"""
Container class for cleaned NOAA Local Climatological Data.

``LocalClimatologicalData`` is a pandas.DataFrame subclass that adds NOAA
download and I/O, a composable temporal and spatial selection interface,
precipitation-event durations, and within-day lagged predictors.
"""

from __future__ import annotations

import difflib
import warnings
from pathlib import Path
from typing import Literal, Optional, Union

import numpy as np
import pandas as pd
import xarray as xr

from . import download as _dl
from .download import DataNotFoundError, Region
from .schema import LAGGABLE_COLUMNS, LONG_NAMES, OUTPUT_COLUMNS, UNITS

_DT_ATTRS: frozenset[str] = frozenset(
    {"year", "month", "day", "date", "dayofyear",
     "hour", "minute", "week", "weekday", "quarter"}
)

_SEASONS: dict[str, tuple[int, ...]] = {
    "DJF": (12, 1, 2), "MAM": (3, 4, 5), "JJA": (6, 7, 8), "SON": (9, 10, 11),
}


class LocalClimatologicalData(pd.DataFrame):
    """DataFrame subclass for NOAA LCD station records."""

    _metadata = ["data_attrs"]
    attrs_df = pd.DataFrame(
        {"units": pd.Series(UNITS), "long_name": pd.Series(LONG_NAMES)}
    )

    @property
    def _constructor(self) -> type[LocalClimatologicalData]:
        return LocalClimatologicalData

    # --------------------------------------------------------- Construction

    @classmethod
    def from_noaa(
        cls,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_year: int,
        end_year: int,
        *,
        country: Optional[str] = "US",
        min_year_range: int = 1,
        output: Optional[Union[str, Path]] = None,
        stations_file: Union[str, Path] = _dl.STATIONS_FILE,
        base_url: str = _dl.BASE_URL,
        raw_dir: Optional[Union[str, Path]] = None,
        workers: int = 8,
        classify: bool = True,
        keep_raw: bool = False,
    ) -> LocalClimatologicalData:
        """Download, clean, and load LCD records for a bounding box and years.

        Writes a compressed (station, time) netCDF when ``output`` is given.
        Timestamps in the result are UTC.
        """
        region = Region(
            lat_min, lat_max, lon_min, lon_max, start_year, end_year,
            country=country, min_year_range=min_year_range,
        )
        df = _dl.build(
            region, output=output, stations_file=stations_file, base_url=base_url,
            raw_dir=raw_dir, workers=workers, classify=classify, keep_raw=keep_raw,
        )
        obj = cls(df)
        obj.data_attrs = cls.attrs_df
        return obj

    @classmethod
    def open_data(
        cls,
        file: Union[str, Path],
        engine: Literal["netcdf", "pickle", "csv"] = "netcdf",
    ) -> LocalClimatologicalData:
        """Load pre-processed data from netCDF, pickle, or CSV."""
        file = Path(file)
        if not file.exists():
            raise DataNotFoundError(f"File not found: {file}")

        if engine == "netcdf":
            data = _dl.read_netcdf(file)
        elif engine == "pickle":
            data = pd.read_pickle(file)
        elif engine == "csv":
            data = pd.read_csv(file, parse_dates=["time"])
        else:
            raise ValueError(f"Unknown engine: {engine!r}")

        expected = [c for c in OUTPUT_COLUMNS if c != "prec_type"]
        missing = [c for c in expected if c not in data.columns]
        if missing:
            warnings.warn(
                f"Missing expected columns: {missing}",
                category=UserWarning, stacklevel=2,
            )

        obj = cls(data)
        obj.data_attrs = cls.attrs_df
        return obj

    # -------------------------------------------------------------- Output

    def to_xarray(self) -> xr.Dataset:
        """Return the (station, time) CF timeSeries Dataset for this frame."""
        return _dl.to_xarray(pd.DataFrame(self, copy=False))

    def to_netcdf(self, path: Union[str, Path]) -> None:
        """Serialize to compressed (station, time) netCDF with CF attributes."""
        _dl.to_netcdf(pd.DataFrame(self, copy=False), path)

    # ----------------------------------------------------------- Group keys

    def groupby(self, by, *, time_col: str = "time", **kwargs):
        """GroupBy extension accepting datetime-component keys.

        Understands plain columns, datetime attributes ('year', 'month',
        'hour', ...), and accessors ('time.dt.month'). The frame is not
        mutated; derived keys live on a shallow copy.
        """
        if not isinstance(by, list):
            by = [by]

        df = pd.DataFrame(self, copy=False).copy(deep=False)
        keys: list[str] = []
        for b in by:
            if b.startswith(f"{time_col}.dt."):
                attr = b.split(".")[-1]
                df[attr] = getattr(df[time_col].dt, attr)
                keys.append(attr)
            elif b in _DT_ATTRS and b not in df.columns:
                df[b] = getattr(df[time_col].dt, b)
                keys.append(b)
            else:
                keys.append(b)
        return df.groupby(keys, **kwargs)

    # ------------------------------------------------------------ Selection

    def sel(
        self,
        *,
        # precipitation
        convective: Optional[bool] = None,
        prec_types: Optional[Union[str, list[str]]] = None,
        min_p: Optional[float] = None,
        wet_only: bool = False,
        # time (UTC)
        start: Optional[Union[str, pd.Timestamp]] = None,
        end: Optional[Union[str, pd.Timestamp]] = None,
        years: Optional[Union[int, list[int]]] = None,
        months: Optional[Union[int, list[int]]] = None,
        hours: Optional[Union[int, list[int]]] = None,
        season: Optional[str] = None,
        # space
        bbox: Optional[tuple[float, float, float, float]] = None,
        lats: Optional[Union[float, list[float]]] = None,
        lons: Optional[Union[float, list[float]]] = None,
        cities: Optional[Union[str, list[str]]] = None,
        states: Optional[Union[str, list[str]]] = None,
        station_ids: Optional[Union[str, list[str]]] = None,
    ) -> LocalClimatologicalData:
        """Composable subset by precipitation, time (UTC), and space.

        All supplied filters combine with logical AND; spatial, temporal, and
        precipitation filters may be mixed freely.

        Precipitation
            convective : bool
                Shortcut for prec_type == 'convective' (True) or 'stratiform'
                (False). Mutually exclusive with ``prec_types``. Requires a
                'prec_type' column (present when the data were classified).
            prec_types : str or list[str]
                Explicit regime labels to retain.
            min_p : float
                Minimum precipitation intensity (mm/hr), inclusive.
            wet_only : bool
                Retain only records with p > 0.
        Time (UTC)
            start, end : timestamp-like
                Closed [start, end] datetime bounds.
            years, months, hours : int or list[int]
                Calendar-component filters.
            season : {'DJF','MAM','JJA','SON'}
                Meteorological season shortcut.
        Space
            bbox : (lat_min, lat_max, lon_min, lon_max)
                Bounding-box filter.
            lats, lons : float or list[float]
                Exact-coordinate filters; paired when both are given.
            cities, states, station_ids : str or list[str]
                Categorical station filters.
        """
        if convective is not None and prec_types is not None:
            raise ValueError("Provide either 'convective' or 'prec_types'.")
        if (convective is not None or prec_types is not None) \
                and "prec_type" not in self.columns:
            raise ValueError(
                "No 'prec_type' column; rebuild with classify=True or use "
                "lcd.classify.add_precip_type before filtering by regime."
            )

        df = pd.DataFrame(self, copy=False)

        # precipitation
        if convective is True:
            df = df[df["prec_type"] == "convective"]
        elif convective is False:
            df = df[df["prec_type"] == "stratiform"]
        if prec_types is not None:
            df = df[df["prec_type"].isin(_as_list(prec_types))]
        if wet_only:
            df = df[df["p"] > 0]
        if min_p is not None:
            df = df[df["p"] >= min_p]

        # time (UTC)
        if start is not None:
            df = df[df["time"] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df["time"] <= pd.Timestamp(end)]
        if years is not None:
            df = df[df["time"].dt.year.isin(_as_list(years))]
        if season is not None:
            df = df[df["time"].dt.month.isin(_season_months(season))]
        if months is not None:
            df = df[df["time"].dt.month.isin(_as_list(months))]
        if hours is not None:
            df = df[df["time"].dt.hour.isin(_as_list(hours))]

        # space
        if bbox is not None:
            lo_a, hi_a, lo_o, hi_o = bbox
            df = df[df["lat"].between(lo_a, hi_a) & df["lon"].between(lo_o, hi_o)]
        df = _select_coords(df, lats=lats, lons=lons)
        if cities is not None:
            df = df[_str_match_mask(df["city"], cities)]
        if states is not None:
            wanted = {s.strip().upper() for s in _as_list(states)}
            df = df[df["state"].astype(str).str.upper().isin(wanted)]
        if station_ids is not None:
            ids = {str(s) for s in _as_list(station_ids)}
            df = df[df["station_id"].astype(str).isin(ids)]

        df = df.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
        return LocalClimatologicalData(df)

    # -------------------------------------------------------- Introspection

    @property
    def stations(self) -> pd.DataFrame:
        """Unique stations with coordinates and record counts."""
        g = (
            pd.DataFrame(self, copy=False)
            .groupby(["station_id", "city", "state", "lat", "lon"], dropna=False)
            .agg(n_records=("time", "size"),
                 start=("time", "min"), end=("time", "max"))
            .reset_index()
        )
        return g.sort_values("n_records", ascending=False).reset_index(drop=True)

    # ------------------------------------------------------ Event durations

    def get_durations(
        self,
        gap_tolerance_hours: float = 1.0,
        sampling_interval_hours: float = 1.0,
    ) -> LocalClimatologicalData:
        """Per-station precipitation-event durations, in minutes.

        An event is a contiguous run of p > 0; a gap exceeding
        ``gap_tolerance_hours`` terminates it. Duration is
        n_obs * sampling_interval_hours * 60, each observation representing one
        accumulation interval (Eagleson 1972; Restrepo-Posada and Eagleson
        1982).
        """
        parts = [
            _calc_durations(g, gap_tolerance_hours, sampling_interval_hours)
            for _, g in self.groupby("station_id")
        ]
        out = pd.concat(parts, ignore_index=True)
        out = out.sort_values(["lat", "lon", "time"]).reset_index(drop=True)
        return LocalClimatologicalData(out)

    # ------------------------------------------------------------ Lagging

    def lag(self, lag: int = 1) -> LocalClimatologicalData:
        """Within-day lag of thermodynamic and kinematic variables.

        Lagging is applied per (station_id, calendar day) so values never
        cross day boundaries; mid-day gaps are forward-filled within day.
        """
        parts = [_apply_lag(g, lag=lag) for _, g in self.groupby("station_id")]
        out = pd.concat(parts, ignore_index=True)
        out = out.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
        return LocalClimatologicalData(out)


# ============================== Internal helpers ===========================


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _season_months(season: str) -> tuple[int, ...]:
    try:
        return _SEASONS[season.upper()]
    except KeyError:
        raise ValueError(f"season must be one of {sorted(_SEASONS)}") from None


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

    is_wet = df["p"] > 0
    prev_dry = ~is_wet.shift(fill_value=False)
    long_gap = dt_hours > gap_tolerance_hours
    new_event = is_wet & (prev_dry | long_gap)

    df["event_id"] = new_event.cumsum().where(is_wet, 0)
    counts = df.groupby("event_id").size()
    duration_min = (counts * sampling_interval_hours * 60.0).rename("duration")

    df = df.merge(duration_min, left_on="event_id", right_index=True, how="left")
    df.loc[df["event_id"] == 0, "duration"] = 0.0
    df.loc[df["p"].isna(), "duration"] = np.nan
    return df.drop(columns=["event_id"]).dropna(subset=["p"])


def _select_coords(
    df: pd.DataFrame,
    lats: Optional[Union[float, list[float]]],
    lons: Optional[Union[float, list[float]]],
) -> pd.DataFrame:
    """Exact-coordinate selection; paired when both lats and lons are given."""
    if lats is None and lons is None:
        return df
    if lats is not None and not isinstance(lats, list):
        lats = [lats]
    if lons is not None and not isinstance(lons, list):
        lons = [lons]

    if lats is not None and lons is None:
        return df[df["lat"].isin(lats)]
    if lons is not None and lats is None:
        return df[df["lon"].isin(lons)]
    if len(lats) != len(lons):
        raise ValueError("len(lats) must equal len(lons) when both are given.")

    pairs = pd.MultiIndex.from_arrays([df["lat"].values, df["lon"].values])
    targets = pd.MultiIndex.from_arrays([lats, lons])
    return df[pairs.isin(targets)]


def _str_match_mask(series: pd.Series, names: Union[str, list[str]]) -> pd.Series:
    """Case-insensitive name match with fuzzy suggestions for misses."""
    names = [n.strip().upper() for n in _as_list(names)]
    upper = series.astype(str).str.upper()
    available = upper.dropna().unique().tolist()

    valid: list[str] = []
    invalid: list[tuple[str, list[str]]] = []
    for n in names:
        if n in available:
            valid.append(n)
            continue
        close = difflib.get_close_matches(n, available, n=5)
        substr = [a for a in available if n in a]
        suggestions = sorted({*close, *substr})
        if suggestions:
            invalid.append((n, suggestions))

    if invalid:
        report = "\n".join(f"  {n!r} -> {s}" for n, s in invalid)
        warnings.warn("Unmatched names; nearest matches:\n" + report,
                      category=UserWarning, stacklevel=2)
    return upper.isin(valid)
