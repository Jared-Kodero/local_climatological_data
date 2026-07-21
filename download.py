"""
Acquisition and cleaning for NOAA Local Climatological Data.

Provides station selection from the NCEI inventory, concurrent HTTP download
via a retrying requests session, hourly-only cleaning with SI unit conversion,
Local Standard Time to UTC conversion, deduplication of (station, time) pairs,
optional convective classification, and a compressed (station, time) netCDF
serialization following the CF timeSeries convention.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd
import requests
import xarray as xr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import classify as _classify
from .schema import (
    BASE_COLUMNS,
    BASE_URL,
    COORD_COLUMNS,
    HOURLY_REPORT_TYPES,
    INCH_TO_MM,
    INHG_TO_HPA,
    MEASURE_COLUMNS,
    MILE_TO_KM,
    MPH_TO_MS,
    OUTPUT_COLUMNS,
    RAW_TO_SHORT,
    STATIONS_FILE,
    STRING_COLUMNS,
    TMP,
    TRACE_INCHES,
    WEATHER_COLUMNS,
    variable_attrs,
)

logger = logging.getLogger("lcd")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


class EmptyDataFrameError(ValueError):
    """Raised when an operation produces an empty DataFrame unexpectedly."""


class DataNotFoundError(FileNotFoundError):
    """Raised when a requested data file cannot be located."""


# ================================== Region =================================


@dataclass(frozen=True)
class Region:
    """Spatial and temporal request envelope.

    Parameters
    ----------
    lat_min, lat_max, lon_min, lon_max : float
        Bounding box in degrees north and degrees east.
    start_year, end_year : int
        Inclusive calendar-year range.
    country : str, optional
        Two-letter country filter; None disables it.
    min_year_range : int, default 1
        Minimum station operating span (END year minus BEGIN year).
    """

    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    start_year: int
    end_year: int
    country: Optional[str] = "US"
    min_year_range: int = 1

    def __post_init__(self) -> None:
        if self.lat_min > self.lat_max or self.lon_min > self.lon_max:
            raise ValueError("min bounds must not exceed max bounds.")
        if self.start_year > self.end_year:
            raise ValueError("start_year must not exceed end_year.")


# ============================== Station selection ==========================


def select_stations(
    region: Region,
    stations_file: Union[str, Path] = STATIONS_FILE,
) -> pd.DataFrame:
    """Select inventory stations inside a Region.

    Returns a frame with an added ``station_id`` column (USAF + WBAN) and
    integer ``begin``/``end`` operating years.
    """
    stations_file = Path(stations_file)
    if not stations_file.exists():
        raise DataNotFoundError(f"Station inventory not found: {stations_file}")

    inv = pd.read_csv(stations_file, header=0)
    inv[["LAT", "LON"]] = inv[["LAT", "LON"]].astype(float)

    mask = (
        inv["LAT"].between(region.lat_min, region.lat_max)
        & inv["LON"].between(region.lon_min, region.lon_max)
    )
    if region.country is not None:
        mask &= inv["CTRY"] == region.country
    inv = inv[mask].copy()

    inv["begin"] = inv["BEGIN"].astype(str).str[:4].astype(int)
    inv["end"] = inv["END"].astype(str).str[:4].astype(int)
    inv = inv[(inv["end"] - inv["begin"]) >= region.min_year_range]
    inv = inv[(inv["begin"] <= region.end_year) & (inv["end"] >= region.start_year)]

    inv["station_id"] = inv["USAF"].astype(str) + inv["WBAN"].astype(str)
    return inv.drop_duplicates("station_id").reset_index(drop=True)


# ================================= Download ================================


def _make_session(retries: int = 4, backoff: float = 0.5) -> requests.Session:
    """Build a requests session with exponential-backoff retries."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _download_one(
    session: requests.Session,
    base_url: str,
    year: int,
    station_id: str,
    out_dir: Path,
    timeout: float,
) -> str:
    """Fetch one station-year CSV. Returns 'ok', 'missing', or 'error'."""
    url = f"{base_url}/{year}/{station_id}.csv"
    dest = out_dir / str(year) / f"{station_id}.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with session.get(url, stream=True, timeout=timeout) as resp:
            if resp.status_code == 404:
                return "missing"
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    if chunk:
                        fh.write(chunk)
        return "ok"
    except requests.RequestException as exc:
        if dest.exists():
            dest.unlink()
        logger.warning("download failed %s: %s", url, exc)
        return "error"


def download_stations(
    inv: pd.DataFrame,
    start_year: int,
    end_year: int,
    out_dir: Union[str, Path],
    base_url: str = BASE_URL,
    workers: int = 8,
    timeout: float = 60.0,
) -> dict[str, int]:
    """Download every valid (station, year) CSV concurrently.

    Returns a status tally: {'ok': n, 'missing': n, 'error': n}.
    """
    out_dir = Path(out_dir)
    spans = dict(zip(inv["station_id"], zip(inv["begin"], inv["end"])))

    jobs: list[tuple[int, str]] = []
    for year in range(start_year, end_year + 1):
        for sid, (begin, end) in spans.items():
            if begin <= year <= end:
                jobs.append((year, sid))

    logger.info("Downloading %d station-year files", len(jobs))
    session = _make_session()
    tally = {"ok": 0, "missing": 0, "error": 0}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_download_one, session, base_url, y, sid, out_dir, timeout)
            for y, sid in jobs
        ]
        for fut in futures:
            tally[fut.result()] += 1

    logger.info("Download tally: %s", tally)
    return tally


# ================================== Cleaning ===============================


def _to_numeric(series: pd.Series) -> pd.Series:
    """Coerce to float, preserving sign and decimals, dropping quality flags.

    Removes any character that is not a digit, dot, or sign (e.g. the 's'
    suspect flag, '*', or 'M'), retaining negative temperatures rather than
    stripping the leading minus as digit-only cleaning would.
    """
    cleaned = series.astype(str).str.replace(r"[^0-9.\-+]", "", regex=True)
    return pd.to_numeric(cleaned.replace({"": np.nan}), errors="coerce")


def _clean_precip(series: pd.Series) -> pd.Series:
    """Clean the precipitation field, mapping trace to the documented value."""
    s = series.astype(str).str.replace("T", str(TRACE_INCHES), regex=False)
    s = s.str.replace(r"[^0-9.]", "", regex=True)
    s = s.str.split(".").apply(lambda x: ".".join(x[:2]) if isinstance(x, list) else x)
    return pd.to_numeric(s.replace({"": np.nan}), errors="coerce")


def _parse_name(name: object) -> tuple[str, str]:
    """Split an LCD NAME ('CITY ..., ST US') into (city, state)."""
    if not isinstance(name, str) or not name.strip():
        return "", ""
    parts = name.rsplit(",", 1)
    city = parts[0].strip()
    state = ""
    if len(parts) == 2:
        tokens = parts[1].split()
        if tokens:
            state = tokens[0].strip()
    return city, state


def _resample_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Round observation times up to the hour, keeping the best record per hour.

    Among duplicate hours the retained record is the one reporting
    precipitation and having the fewest missing fields.
    """
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], format="mixed", errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")
    df["time"] = df["time"].dt.ceil("h")
    df["_p_ok"] = df["p"].notnull()
    df["_nan"] = df.isnull().sum(axis=1)
    df = df.sort_values(["time", "_p_ok", "_nan"], ascending=[True, True, False])
    df = df.groupby("time").last().reset_index()
    return df.drop(columns=["_p_ok", "_nan"])


def _convert_si(df: pd.DataFrame) -> pd.DataFrame:
    """Apply LCD-to-SI unit conversions in place."""
    df["p"] = df["p"] * INCH_TO_MM
    df["t"] = (df["t"] - 32.0) * (5.0 / 9.0)
    df["dpt"] = (df["dpt"] - 32.0) * (5.0 / 9.0)
    df["ws"] = df["ws"] * MPH_TO_MS
    df["wsg"] = df["wsg"] * MPH_TO_MS
    df["sp"] = df["sp"] * INHG_TO_HPA
    df["stp"] = df["stp"] * INHG_TO_HPA
    df["vis"] = df["vis"] * MILE_TO_KM
    return df


def lst_to_utc(df: pd.DataFrame, time_col: str = "time", lon_col: str = "lon"):
    """Convert Local Standard Time to UTC using the longitude time-zone offset.

    LCD timestamps are Local Standard Time (no daylight saving). The standard
    offset is approximated from longitude,

        offset = round( ((lon + 180) mod 360 - 180) * 24 / 360 )  [hours]

    which is the same mapping used to derive local time from UTC; the inverse
    is applied here, UTC = LST - offset.
    """
    lon = df[lon_col].astype(float)
    offset = (((lon + 180.0) % 360.0 - 180.0) * (24.0 / 360.0)).round()
    df[time_col] = df[time_col] - pd.to_timedelta(offset, unit="h")
    return df


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean one resampled station frame into the base (unclassified) schema."""
    df["p"] = _clean_precip(df["p"])
    for col in ("t", "dpt", "rh", "wd", "ws", "wsg", "sp", "stp", "vis",
                "lat", "lon", "elev"):
        df[col] = _to_numeric(df[col])

    df = _convert_si(df)

    wt = df["weather_type"].fillna("").replace("", "?|?|?")
    groups = wt.str.split("|", expand=True)
    for i, name in enumerate(WEATHER_COLUMNS):
        col = groups[i] if i in groups.columns else ""
        df[name] = pd.Series(col, index=df.index).fillna("").replace("?", "")

    names = df["name"].apply(_parse_name)
    df["city"] = names.str[0]
    df["state"] = names.str[1]

    df = df.dropna(subset=["p"])
    df = lst_to_utc(df)
    return df[list(BASE_COLUMNS)].reset_index(drop=True)


def read_and_clean(
    path: Union[str, Path],
    report_types: frozenset[str] = HOURLY_REPORT_TYPES,
) -> pd.DataFrame:
    """Read one raw station CSV and return a cleaned hourly frame.

    Only schema columns are read; non-hourly summary rows are dropped by
    report type. Returns an empty schema-shaped frame on any error.
    """
    empty = pd.DataFrame(columns=list(BASE_COLUMNS))
    try:
        raw = pd.read_csv(path, dtype=str, usecols=lambda c: c in RAW_TO_SHORT)
        for col in RAW_TO_SHORT:
            if col not in raw.columns:
                raw[col] = np.nan
        df = raw[list(RAW_TO_SHORT)].rename(columns=RAW_TO_SHORT)

        if report_types:
            df = df[df["report_type"].isin(report_types)]
        if df.empty or df["p"].isnull().all():
            return empty

        df = _resample_hourly(df)
        return _clean(df)
    except Exception:
        logger.exception("failed to clean %s", path)
        return empty


def clean_directory(
    raw_dir: Union[str, Path],
    report_types: frozenset[str] = HOURLY_REPORT_TYPES,
    workers: int = 8,
    classify: bool = True,
) -> pd.DataFrame:
    """Clean every CSV under ``raw_dir``, concatenate, deduplicate, classify.

    Deduplication keeps one record per (station_id, time); the record with
    reported precipitation and the fewest missing fields is retained.
    """
    raw_dir = Path(raw_dir)
    files = sorted(raw_dir.rglob("*.csv"))
    if not files:
        raise EmptyDataFrameError(f"No CSV files found under {raw_dir}")

    logger.info("Cleaning %d station files", len(files))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        frames = list(pool.map(lambda f: read_and_clean(f, report_types), files))

    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for col in MEASURE_COLUMNS + ("lat", "lon", "elev"):
        df[col] = df[col].astype(np.float32)
    for col in WEATHER_COLUMNS + ("station_id", "city", "state"):
        df[col] = df[col].astype(str)
    df = df.dropna(subset=["p", "time"])

    # deduplicate (station, time): prefer precipitating, then fewest NaNs
    df["_p_ok"] = df["p"].notnull()
    df["_nan"] = df[list(MEASURE_COLUMNS)].isnull().sum(axis=1)
    df = df.sort_values(["station_id", "time", "_p_ok", "_nan"],
                        ascending=[True, True, True, False])
    df = df.drop_duplicates(["station_id", "time"], keep="last")
    df = df.drop(columns=["_p_ok", "_nan"])

    if classify:
        df = _classify.add_precip_type(df)

    df = df.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    return df


# ================================ netCDF I/O ===============================


def to_xarray(df: pd.DataFrame) -> xr.Dataset:
    """Reshape a cleaned frame into a CF timeSeries Dataset.

    Dimensions are (station, time). Latitude, longitude, elevation, city, and
    state are coordinates along the station dimension. Requires unique
    (station_id, time) pairs.
    """
    df = df.drop_duplicates(["station_id", "time"])
    stations = np.sort(df["station_id"].unique())
    times = np.sort(df["time"].unique())

    meta = (
        df.groupby("station_id")[list(COORD_COLUMNS)].first().reindex(stations)
    )

    ds = xr.Dataset(coords={"station": stations, "time": times})
    ds = ds.assign_coords(
        {c: ("station", meta[c].values) for c in COORD_COLUMNS}
    )

    value_cols = [c for c in df.columns
                  if c not in ("station_id", "time", *COORD_COLUMNS)]
    for col in value_cols:
        pivot = (
            df.pivot(index="station_id", columns="time", values=col)
            .reindex(index=stations, columns=times)
        )
        ds[col] = (("station", "time"), pivot.values)

    for name in ds.variables:
        attrs = variable_attrs(str(name))
        if np.issubdtype(ds[name].dtype, np.datetime64):
            attrs.pop("units", None)  # CF datetime encoding owns 'units'
        ds[name].attrs.update(attrs)
    ds.attrs.update(
        title="NOAA Local Climatological Data (hourly, cleaned, SI units, UTC)",
        source=BASE_URL,
        featureType="timeSeries",
        Conventions="CF-1.8",
    )
    return ds


_PREC_FLAGS: tuple[str, ...] = ("none", "stratiform", "convective")


def to_netcdf(df: pd.DataFrame, path: Union[str, Path]) -> None:
    """Write a cleaned frame to compressed (station, time) netCDF.

    Measurement fields are stored as compressed float32. The 2D present-weather
    fields are stored compactly to avoid variable-length-string overhead:
    ``prec_type`` becomes a CF int8 flag variable, and ``au``/``aw``/``mw``
    become int16 categorical codes whose lookup tables are stored as a JSON
    'categories' attribute. Per-station string coordinates are left as-is.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ds = to_xarray(df)
    encoding: dict[str, dict] = {}

    if "prec_type" in ds.data_vars:
        dims = ds["prec_type"].dims
        raw = ds["prec_type"].values
        lookup = {v: i for i, v in enumerate(_PREC_FLAGS)}
        filled = np.where(pd.isnull(raw), "none", raw)
        coded = np.vectorize(lambda v: lookup.get(str(v), 0))(filled).astype("int8")
        ds["prec_type"] = (dims, coded)
        ds["prec_type"].attrs = {
            "long_name": variable_attrs("prec_type").get("long_name", ""),
            "flag_values": np.arange(len(_PREC_FLAGS), dtype="int8"),
            "flag_meanings": " ".join(_PREC_FLAGS),
        }
        encoding["prec_type"] = {"zlib": True, "complevel": 4}

    for col in WEATHER_COLUMNS:
        if col not in ds.data_vars:
            continue
        dims = ds[col].dims
        raw = ds[col].values
        flat = pd.Series(raw.ravel()).fillna("").astype(str)
        cats, inv = np.unique(flat.values, return_inverse=True)
        ds[col] = (dims, inv.reshape(raw.shape).astype("int16"))
        ds[col].attrs = {
            "long_name": variable_attrs(col).get("long_name", ""),
            "categories": json.dumps(cats.tolist()),
        }
        encoding[col] = {"zlib": True, "complevel": 4}

    for name, var in ds.data_vars.items():
        if np.issubdtype(var.dtype, np.floating):
            encoding[name] = {"zlib": True, "complevel": 4, "dtype": "float32"}

    ds.to_netcdf(path, engine="netcdf4", encoding=encoding)


def read_netcdf(path: Union[str, Path]) -> pd.DataFrame:
    """Read a (station, time) netCDF written by :func:`to_netcdf` into a frame.

    Reverses the compact encoding of ``prec_type`` (int8 flags) and
    ``au``/``aw``/``mw`` (int16 categorical codes) back to strings, then drops
    the empty (station, time) padding cells.
    """
    with xr.open_dataset(path, engine="netcdf4") as ds:
        ds = ds.load()

    if "prec_type" in ds and "flag_meanings" in ds["prec_type"].attrs:
        meanings = np.array(ds["prec_type"].attrs["flag_meanings"].split(),
                            dtype=object)
        ds["prec_type"] = (ds["prec_type"].dims, meanings[ds["prec_type"].values])

    for col in WEATHER_COLUMNS:
        if col in ds and "categories" in ds[col].attrs:
            cats = np.array(json.loads(ds[col].attrs["categories"]), dtype=object)
            ds[col] = (ds[col].dims, cats[ds[col].values])

    df = ds.to_dataframe().reset_index().rename(columns={"station": "station_id"})
    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: v.decode() if isinstance(v, bytes) else v
            )
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["p"])  # drop (station, time) padding cells
    cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    return df[cols].sort_values(["time", "lat", "lon"]).reset_index(drop=True)


# ================================ Orchestrator =============================


def build(
    region: Region,
    output: Optional[Union[str, Path]] = None,
    stations_file: Union[str, Path] = STATIONS_FILE,
    base_url: str = BASE_URL,
    raw_dir: Optional[Union[str, Path]] = None,
    workers: int = 8,
    classify: bool = True,
    keep_raw: bool = False,
) -> pd.DataFrame:
    """Select, download, clean, and optionally serialize LCD records.

    Parameters
    ----------
    region : Region
        Spatial and temporal request envelope.
    output : str or Path, optional
        Destination netCDF path. If None, nothing is written.
    stations_file : str or Path
        NCEI station inventory.
    base_url : str
        Data endpoint (override for testing or mirrors).
    raw_dir : str or Path, optional
        Directory for raw downloads. If None, a node-local temporary directory
        under /tmp is used and removed unless ``keep_raw``.
    workers : int, default 8
        Concurrency for download and cleaning.
    classify : bool, default True
        Add a 'prec_type' column via :mod:`lcd.classify`.
    keep_raw : bool, default False
        Retain the raw CSVs after cleaning.

    Returns
    -------
    pandas.DataFrame
    """
    inv = select_stations(region, stations_file)
    if inv.empty:
        raise EmptyDataFrameError("No stations match the requested region.")
    logger.info("Selected %d stations", len(inv))

    made_tmp = raw_dir is None
    raw_dir = Path(tempfile.mkdtemp(prefix="lcd_", dir=TMP)) if made_tmp else Path(raw_dir)
    try:
        download_stations(
            inv, region.start_year, region.end_year, raw_dir,
            base_url=base_url, workers=workers,
        )
        df = clean_directory(raw_dir, workers=workers, classify=classify)
    finally:
        if made_tmp and not keep_raw:
            shutil.rmtree(raw_dir, ignore_errors=True)

    if output is not None:
        to_netcdf(df, output)
        logger.info("Wrote %d records to %s", len(df), output)
    return df
