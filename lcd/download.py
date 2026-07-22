"""
Acquisition and cleaning for NOAA Local Climatological Data.

Station selection from the NCEI inventory, concurrent HTTP download via a
retrying requests session, hourly-only cleaning with SI unit conversion,
Local Standard Time to UTC conversion, deduplication of (station, time) pairs,
optional convective classification, and a compressed (station, time) netCDF
serialization following the CF timeSeries convention.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd
import requests
import xarray as xr
from dask import compute, delayed
from dask.diagnostics import ProgressBar
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
    """Spatial and temporal request envelope."""

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
    """Select inventory stations inside a Region."""
    stations_file = Path(stations_file)
    if not stations_file.exists():
        raise DataNotFoundError(f"Station inventory not found: {stations_file}")

    inv = pd.read_csv(stations_file, header=0)
    inv[["LAT", "LON"]] = inv[["LAT", "LON"]].astype(float)

    mask = inv["LAT"].between(region.lat_min, region.lat_max) & inv["LON"].between(
        region.lon_min, region.lon_max
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
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=32)
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


def _download_job(
    job: tuple[int, str],
    session: requests.Session,
    base_url: str,
    out_dir: Path,
    timeout: float,
) -> str:
    """imap_unordered worker: unpack (year, station_id) and download."""
    year, sid = job
    return _download_one(session, base_url, year, sid, out_dir, timeout)


def download_stations(
    inv: pd.DataFrame,
    start_year: int,
    end_year: int,
    out_dir: Union[str, Path],
    base_url: str = BASE_URL,
    workers: int = 16,
    scheduler: str = "processes",
    timeout: float = 60.0,
) -> dict[str, int]:
    """Download every valid (station, year) CSV concurrently.

    Uses a thread-backed multiprocessing pool with ``imap_unordered`` so the
    shared requests session is reused and progress advances as each file
    finishes.
    """
    out_dir = Path(out_dir)
    spans = dict(zip(inv["station_id"], zip(inv["begin"], inv["end"])))

    jobs: list[tuple[int, str]] = []
    for year in range(start_year, end_year + 1):
        for sid, (begin, end) in spans.items():
            if begin <= year <= end:
                jobs.append((year, sid))

    session = _make_session()
    worker = partial(
        _download_job,
        session=session,
        base_url=base_url,
        out_dir=out_dir,
        timeout=timeout,
    )

    tasks = [delayed(worker)(job) for job in jobs]

    tally = {"ok": 0, "missing": 0, "error": 0}

    with ProgressBar():
        statuses = compute(
            *tasks,
            scheduler=scheduler,
            num_workers=max(1, workers),
        )

    for status in statuses:
        tally[status] += 1

    return tally


# ================================== Cleaning ===============================


def _to_numeric(series: pd.Series) -> pd.Series:
    """Coerce to float, preserving sign and decimals, dropping quality flags."""
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

    Times are ceiled to the hour (dt.ceil('h')); among duplicate hours the
    retained record is the one reporting precipitation and having the fewest
    missing fields.
    """
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], format="mixed", errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")
    df["time"] = df["time"].dt.ceil("h")
    df["_p_ok"] = df["prec"].notnull()
    df["_nan"] = df.isnull().sum(axis=1)
    df = df.sort_values(["time", "_p_ok", "_nan"], ascending=[True, True, False])
    df = df.groupby("time").last().reset_index()
    return df.drop(columns=["_p_ok", "_nan"])


def _convert_si(df: pd.DataFrame) -> pd.DataFrame:
    """Apply LCD-to-SI unit conversions in place."""
    df["prec"] = df["prec"] * INCH_TO_MM
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

    and the inverse is applied, UTC = LST - offset.
    """
    lon = df[lon_col].astype(float)
    offset = (((lon + 180.0) % 360.0 - 180.0) * (24.0 / 360.0)).round()
    df["local_standard_time"] = df[time_col]
    df[time_col] = df[time_col] - pd.to_timedelta(offset, unit="h")

    return df


def to_local_time(df: pd.DataFrame, time_col: str = "time", lon_col: str = "lon"):
    """Convert UTC back to Local Standard Time using the longitude offset.

    Inverse of :func:`lst_to_utc`. With the same longitude offset,

        offset = round( ((lon + 180) mod 360 - 180) * 24 / 360 )  [hours]

    the local standard time is LST = UTC + offset (no daylight saving). This
    operates on a copy so the passed frame is not modified.
    """
    out = df.copy()
    lon = out[lon_col].astype(float)
    offset = (((lon + 180.0) % 360.0 - 180.0) * (24.0 / 360.0)).round()
    out[time_col] = out[time_col] + pd.to_timedelta(offset, unit="h")
    return out


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean one resampled station frame into the base (unclassified) schema."""
    df["prec"] = _clean_precip(df["prec"])
    for col in (
        "t",
        "dpt",
        "rh",
        "wd",
        "ws",
        "wsg",
        "sp",
        "stp",
        "vis",
        "lat",
        "lon",
        "elev",
    ):
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

    df = df.dropna(subset=["prec"])
    df = lst_to_utc(df)
    return df[list(BASE_COLUMNS)].reset_index(drop=True)


def read_and_clean(
    path: Union[str, Path],
    report_types: frozenset[str] = HOURLY_REPORT_TYPES,
    months: Optional[Sequence[int]] = None,
) -> pd.DataFrame:
    """Read one raw station CSV and return a cleaned hourly frame.

    Only schema columns are read; non-hourly summary rows are dropped by
    report type. When ``months`` is given, only those calendar months (UTC)
    are retained, which reduces memory during retrieval.
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
        if df.empty or df["prec"].isnull().all():
            return empty

        df = _resample_hourly(df)
        df = _clean(df)
        if months is not None and not df.empty:
            df = df[df["time"].dt.month.isin(list(months))]
        return df
    except Exception:
        logger.exception("failed to clean %s", path)
        return empty


def clean_directory(
    raw_dir: Union[str, Path],
    report_types: frozenset[str] = HOURLY_REPORT_TYPES,
    workers: int = 8,
    classify: bool = True,
    months: Optional[Sequence[int]] = None,
    scheduler: str = "processes",
) -> pd.DataFrame:
    """Clean every CSV under ``raw_dir`` in parallel, dedup, and classify.

    Cleaning is CPU-bound, so a process pool is used; it falls back to a
    thread pool if processes are unavailable. Deduplication keeps one record
    per (station_id, time).
    """
    raw_dir = Path(raw_dir)
    files = [str(f) for f in sorted(raw_dir.rglob("*.csv"))]
    if not files:
        raise EmptyDataFrameError(f"No CSV files found under {raw_dir}")

    tasks = [
        delayed(read_and_clean)(
            file,
            report_types=report_types,
            months=months,
        )
        for file in files
    ]

    with ProgressBar():
        frames = list(compute(*tasks, scheduler=scheduler, num_workers=max(1, workers)))

    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for col in MEASURE_COLUMNS + ("lat", "lon", "elev"):
        df[col] = df[col].astype(np.float32)
    for col in WEATHER_COLUMNS + ("station_id", "city", "state"):
        df[col] = df[col].astype(str)
    df = df.dropna(subset=["prec", "time"])

    df["_p_ok"] = df["prec"].notnull()
    df["_nan"] = df[list(MEASURE_COLUMNS)].isnull().sum(axis=1)
    df = df.sort_values(
        ["station_id", "time", "_p_ok", "_nan"], ascending=[True, True, True, False]
    )
    df = df.drop_duplicates(["station_id", "time"], keep="last")
    df = df.drop(columns=["_p_ok", "_nan"])

    if classify:
        df = _classify.add_precip_type(df)

    df = df.sort_values(["time", "lat", "lon"]).reset_index(drop=True)
    return df


# ================================ netCDF I/O ===============================

_PREC_FLAGS: tuple[str, ...] = ("none", "stratiform", "convective")


def _is_float_dtype(dtype) -> bool:
    """np.issubdtype guarded against pandas extension dtypes (e.g. StringDtype)."""
    try:
        return bool(np.issubdtype(dtype, np.floating))
    except TypeError:
        return False


def _is_datetime_dtype(dtype) -> bool:
    try:
        return bool(np.issubdtype(dtype, np.datetime64))
    except TypeError:
        return pd.api.types.is_datetime64_any_dtype(dtype)


def to_xarray(df: pd.DataFrame) -> xr.Dataset:
    """Reshape a cleaned frame into a CF timeSeries Dataset (station, time)."""
    df = df.drop_duplicates(["station_id", "time"]).copy()

    # Coerce pandas string / extension columns to numpy object so xarray and
    # netCDF see plain arrays (newer pandas defaults strings to StringDtype,
    # which numpy.issubdtype cannot interpret).
    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(object)

    stations = np.asarray(df["station_id"].unique(), dtype=object)
    stations.sort()
    times = np.sort(df["time"].unique())

    meta = df.groupby("station_id")[list(COORD_COLUMNS)].first().reindex(stations)

    ds = xr.Dataset(coords={"station": stations, "time": times})

    for c in COORD_COLUMNS:
        values = meta[c].values
        if c in STRING_COLUMNS:
            values = np.asarray(values, dtype=object)
        ds = ds.assign_coords({c: ("station", values)})

    value_cols = [
        c for c in df.columns if c not in ("station_id", "time", *COORD_COLUMNS)
    ]
    for col in value_cols:
        pivot = df.pivot(index="station_id", columns="time", values=col).reindex(
            index=stations, columns=times
        )
        values = pivot.values
        if col in STRING_COLUMNS:
            values = np.asarray(values, dtype=object)
        ds[col] = (("station", "time"), values)

    for name in ds.variables:
        attrs = variable_attrs(str(name))
        if _is_datetime_dtype(ds[name].dtype):
            attrs.pop("units", None)  # CF datetime encoding owns 'units'
        ds[name].attrs.update(attrs)

    ds["station"] = ds["station"].astype(np.int64)
    ds.attrs.update(
        title="NOAA Local Climatological Data (hourly, cleaned, SI units, UTC)",
        source=BASE_URL,
        featureType="timeSeries",
        Conventions="CF-1.8",
    )
    return ds


def to_netcdf(df: pd.DataFrame, engine: str) -> Path:
    """Write a cleaned frame to compressed (station, time) netCDF.

    The 2D present-weather fields are stored compactly to avoid VLEN string
    overhead: ``prec_type`` becomes a CF int8 flag variable, and au/aw/mw
    become int16 categorical codes with a JSON 'categories' attribute.
    """
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
        if _is_float_dtype(var.dtype):
            encoding[name] = {"zlib": True, "complevel": 4, "dtype": "float32"}

    if engine == "pandas":
        return ds.to_dataframe().reset_index()
    return ds


def open_xarray(path: Union[str, Path]) -> xr.Dataset:
    """Open a stored netCDF and decode the compact fields back to strings."""
    with xr.open_dataset(path, engine="netcdf4") as ds:
        ds = ds.load()

    if "prec_type" in ds and "flag_meanings" in ds["prec_type"].attrs:
        meanings = np.array(
            ds["prec_type"].attrs["flag_meanings"].split(), dtype=object
        )
        ds["prec_type"] = (ds["prec_type"].dims, meanings[ds["prec_type"].values])
        ds["prec_type"].attrs["long_name"] = variable_attrs("prec_type").get(
            "long_name", ""
        )

    for col in WEATHER_COLUMNS:
        if col in ds and "categories" in ds[col].attrs:
            cats = np.array(json.loads(ds[col].attrs["categories"]), dtype=object)
            ds[col] = (ds[col].dims, cats[ds[col].values])
            ds[col].attrs["long_name"] = variable_attrs(col).get("long_name", "")
    return ds


def read_netcdf(path: Union[str, Path]) -> pd.DataFrame:
    """Read a (station, time) netCDF into a long frame with padding removed."""
    ds = open_xarray(path)
    df = ds.to_dataframe().reset_index().rename(columns={"station": "station_id"})
    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: v.decode() if isinstance(v, bytes) else v)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["prec"])  # drop (station, time) padding cells
    cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    return df[cols].sort_values(["time", "lat", "lon"]).reset_index(drop=True)


# ================================ Orchestrator =============================


def build(
    region: Region,
    stations_file: Union[str, Path] = STATIONS_FILE,
    base_url: str = BASE_URL,
    raw_dir: Optional[Union[str, Path]] = None,
    workers: int = 8,
    scheduler: str = "processes",
    classify: bool = True,
    months: Optional[Sequence[int]] = None,
    keep_raw: bool = False,
) -> pd.DataFrame:
    """Select, download, clean, and optionally serialize LCD records."""
    inv = select_stations(region, stations_file)
    if inv.empty:
        raise EmptyDataFrameError("No stations match the requested region.")

    made_tmp = raw_dir is None
    raw_dir = (
        Path(tempfile.mkdtemp(prefix="lcd_", dir=TMP)) if made_tmp else Path(raw_dir)
    )
    try:
        download_stations(
            inv,
            region.start_year,
            region.end_year,
            raw_dir,
            base_url=base_url,
            workers=max(workers, 16),
            scheduler=scheduler,
        )
        df = clean_directory(
            raw_dir,
            workers=workers,
            scheduler=scheduler,
            classify=classify,
            months=months,
        )
    finally:
        if made_tmp and not keep_raw:
            shutil.rmtree(raw_dir, ignore_errors=True)

    return df
