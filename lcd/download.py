"""
Acquisition and cleaning for NOAA Local Climatological Data (LCDv2).

Station selection from the NCEI inventory, concurrent HTTP download via
retrying requests sessions, per-frequency cleaning (hourly or daily) with SI
unit conversion, deduplication of (station, time) pairs, optional convective
classification (hourly only), and a compressed (station, time) netCDF
serialization following the CF timeSeries convention.

Hourly records are converted from Local Standard Time to UTC; daily summaries
are keyed by their Local Standard calendar date without a UTC shift.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Sequence, Union

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
    BASE_URL,
    COORD_COLUMNS,
    DAILY_TRACE_COLUMNS,
    INCH_TO_MM,
    INHG_TO_HPA,
    MILE_TO_KM,
    MPH_TO_MS,
    STATIONS_FILE,
    STRING_COLUMNS,
    TRACE_INCHES,
    WEATHER_COLUMNS,
    FreqSpec,
    get_freq_spec,
    variable_attrs,
)

# A library must not configure logging for the application: attach a
# NullHandler only. Warnings still reach stderr through Python's last-resort
# handler, and applications can configure the "lcd" logger as they wish.
logger = logging.getLogger("lcd")
logger.addHandler(logging.NullHandler())


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


def _validate_months(months: Optional[Sequence[int]]) -> Optional[tuple[int, ...]]:
    """Return ``months`` as a sorted tuple of ints in 1..12 (or None)."""
    if months is None:
        return None
    message = "months must be a non-empty sequence of integers from 1 to 12."
    if isinstance(months, (str, bytes)):
        raise ValueError(message)
    try:
        values = list(months)
    except TypeError:
        raise ValueError(message) from None
    if not values:
        raise ValueError(message)
    for m in values:
        if isinstance(m, (bool, str, bytes)):
            raise ValueError(message)
        try:
            ok = int(m) == m and 1 <= int(m) <= 12
        except (TypeError, ValueError):
            ok = False
        if not ok:
            raise ValueError(message)
    return tuple(sorted({int(m) for m in values}))


# ============================== Station selection ==========================


def select_stations(
    region: Region,
    stations_file: Union[str, Path] = STATIONS_FILE,
) -> pd.DataFrame:
    """Select inventory stations inside a Region.

    The NOAA file name of a station is its 6-character USAF identifier followed
    by its 5-digit WBAN identifier. The inventory stores both without leading
    zeros, so they are zero-padded here (USAF to 6, WBAN to 5 characters);
    otherwise stations such as ``701337``/``103`` would be requested under the
    wrong name and silently reported as missing.

    ``region.min_year_range`` is compared with the length of the station's
    inventory period of record (``end year - begin year``); the period must
    also overlap the requested years.
    """
    stations_file = Path(stations_file)
    if not stations_file.exists():
        raise DataNotFoundError(f"Station inventory not found: {stations_file}")

    inv = pd.read_csv(stations_file, header=0, dtype=str)
    for col in ("LAT", "LON"):
        inv[col] = pd.to_numeric(inv[col], errors="coerce")

    mask = inv["LAT"].between(region.lat_min, region.lat_max) & inv["LON"].between(
        region.lon_min, region.lon_max
    )
    if region.country is not None:
        mask &= inv["CTRY"] == region.country
    inv = inv[mask].dropna(subset=["USAF", "WBAN", "BEGIN", "END"]).copy()

    inv["begin"] = pd.to_numeric(inv["BEGIN"].str[:4], errors="coerce")
    inv["end"] = pd.to_numeric(inv["END"].str[:4], errors="coerce")
    inv = inv.dropna(subset=["begin", "end"])
    inv[["begin", "end"]] = inv[["begin", "end"]].astype(int)
    inv = inv[(inv["end"] - inv["begin"]) >= region.min_year_range]
    inv = inv[(inv["begin"] <= region.end_year) & (inv["end"] >= region.start_year)]

    inv["station_id"] = inv["USAF"].str.strip().str.zfill(6) + inv["WBAN"].str.strip().str.zfill(5)
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


class _Progress:
    """Minimal throttled progress line on stderr (no extra dependencies)."""

    def __init__(self, total: int, label: str, enabled: bool = True) -> None:
        self.total = total
        self.label = label
        self.enabled = enabled and total > 0
        self.done = 0
        self._last = 0.0
        self._lock = threading.Lock()

    def update(self, n: int = 1) -> None:
        if not self.enabled:
            return
        with self._lock:
            self.done += n
            now = time.monotonic()
            if self.done < self.total and now - self._last < 0.5:
                return
            self._last = now
            pct = 100.0 * self.done / self.total
            sys.stderr.write(f"\r{self.label}: {self.done}/{self.total} ({pct:.0f}%)")
            sys.stderr.flush()

    def close(self) -> None:
        if self.enabled:
            sys.stderr.write("\n")
            sys.stderr.flush()


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
    except (requests.RequestException, OSError) as exc:
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
    workers: int = 16,
    scheduler: str = "threads",
    timeout: float = 60.0,
    progress: bool = True,
) -> dict[str, int]:
    """Download every valid (station, year) CSV concurrently.

    Downloads are network-bound, so they run on a pool of ``workers`` threads;
    each thread owns one retrying requests session (and so one keep-alive
    connection pool). Starting threads is far cheaper than starting processes,
    which matters because most requested (station, year) pairs are small.
    ``scheduler`` is accepted for backward compatibility and has no effect on
    downloads (it selects the pool used for cleaning in :func:`build`).

    Returns the number of files that were downloaded (``ok``), are not
    published by NOAA (``missing``, HTTP 404), or failed (``error``).
    """
    out_dir = Path(out_dir)
    spans = dict(zip(inv["station_id"], zip(inv["begin"], inv["end"], strict=True), strict=True))

    jobs: list[tuple[int, str]] = []
    for year in range(start_year, end_year + 1):
        for sid, (begin, end) in spans.items():
            if begin <= year <= end:
                jobs.append((year, sid))

    local = threading.local()
    sessions: list[requests.Session] = []
    lock = threading.Lock()
    bar = _Progress(len(jobs), "downloading", progress)

    def run(job: tuple[int, str]) -> str:
        session = getattr(local, "session", None)
        if session is None:
            session = local.session = _make_session()
            with lock:
                sessions.append(session)
        status = _download_one(session, base_url, job[0], job[1], out_dir, timeout)
        bar.update()
        return status

    tally = {"ok": 0, "missing": 0, "error": 0}
    try:
        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            for status in pool.map(run, jobs):
                tally[status] += 1
    finally:
        bar.close()
        for session in sessions:
            session.close()
    return tally


# ================================== Cleaning ===============================


def _factorized(series: pd.Series, func: Callable[[pd.Series], np.ndarray]) -> np.ndarray:
    """Apply ``func`` to the distinct values of ``series`` and broadcast back.

    Weather columns repeat a small vocabulary over many rows, so running the
    (string-heavy) cleaning function once per distinct value is much cheaper
    than once per row and gives identical results. Missing values map to NaN.
    """
    codes, uniques = pd.factorize(series)
    if len(uniques) == 0:
        return np.full(len(series), np.nan)
    values = np.asarray(func(pd.Series(uniques)), dtype=float)
    return np.append(values, np.nan)[codes]  # code -1 (missing) selects the NaN


def _to_numeric_distinct(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(r"[^0-9.\-+]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")  # '' (nothing numeric left) -> NaN


def _clean_precip_distinct(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("T", str(TRACE_INCHES), regex=False)
    s = s.str.replace(r"[^0-9.]", "", regex=True)
    s = s.str.split(".").apply(lambda x: ".".join(x[:2]) if isinstance(x, list) else x)
    return pd.to_numeric(s, errors="coerce")


def _to_numeric(series: pd.Series) -> pd.Series:
    """Coerce to float, preserving sign and decimals, dropping quality flags."""
    values = _factorized(series, _to_numeric_distinct)
    return pd.Series(values, index=series.index, name=series.name)


def _clean_precip(series: pd.Series) -> pd.Series:
    """Clean a precipitation-like field, mapping trace to the documented value."""
    values = _factorized(series, _clean_precip_distinct)
    return pd.Series(values, index=series.index, name=series.name)


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


def _parse_datetime(values: pd.Series) -> pd.Series:
    """Parse LCD timestamps, using the fast ISO 8601 parser where possible."""
    parsed = pd.to_datetime(values, format="ISO8601", errors="coerce")
    bad = parsed.isna() & values.notna()
    if bad.any():  # unusual formats: fall back to per-element inference
        parsed[bad] = pd.to_datetime(values[bad], format="mixed", errors="coerce")
    return parsed


def _resample_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Round observation times up to the hour, keeping the best record per hour.

    Times are ceiled to the hour (dt.ceil('h')). Where several reports fall in
    the same hour, exactly one complete record is retained: the one reporting
    precipitation and, among those, having the fewest missing fields (the
    latest report wins ties). Fields are never mixed between reports.
    """
    df = df.copy()
    df["time"] = _parse_datetime(df["time"])
    df = df.dropna(subset=["time"])
    df["time"] = df["time"].dt.ceil("h")
    if df["time"].duplicated().any():
        df["_p_ok"] = df["prec"].notnull()
        df["_nan"] = df.isnull().sum(axis=1)
        df = df.sort_values(
            ["time", "_p_ok", "_nan"], ascending=[True, True, False], kind="stable"
        )
        df = df.drop_duplicates("time", keep="last").drop(columns=["_p_ok", "_nan"])
    else:
        df = df.sort_values("time", kind="stable")
    return df.reset_index(drop=True)


def _convert_si_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Apply hourly LCD-to-SI unit conversions in place."""
    for col in ("t", "dpt", "wbt"):
        df[col] = (df[col] - 32.0) * (5.0 / 9.0)
    for col in ("ws", "wsg"):
        df[col] = df[col] * MPH_TO_MS
    for col in ("sp", "stp", "alt", "pchg"):
        df[col] = df[col] * INHG_TO_HPA
    df["vis"] = df["vis"] * MILE_TO_KM
    df["prec"] = df["prec"] * INCH_TO_MM
    return df


def _convert_si_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Apply daily-summary LCD-to-SI unit conversions in place.

    Heating and cooling degree days are left in their reported unit
    (degree-days on a 65 degF base) as this is base-unit specific.
    """
    for col in ("tmax", "tmin", "tavg", "dpt", "wbt"):
        df[col] = (df[col] - 32.0) * (5.0 / 9.0)
    for col in ("ws", "wsg", "ws_sust"):
        df[col] = df[col] * MPH_TO_MS
    for col in ("stp", "sp"):
        df[col] = df[col] * INHG_TO_HPA
    for col in ("prec", "snow", "snwd"):
        df[col] = df[col] * INCH_TO_MM
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


def _split_present_weather_one(value: str) -> tuple[str, str, str]:
    """Split one 'au|aw|mw' string; '?' placeholders and padding become ''."""
    if value == "":
        return "", "", ""
    parts = value.split("|")
    out = []
    for i in range(len(WEATHER_COLUMNS)):
        part = parts[i] if i < len(parts) else ""
        out.append("" if part == "?" else part.strip())
    return out[0], out[1], out[2]


def _split_present_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Split HourlyPresentWeatherType ('au|aw|mw') into the three groups."""
    wt = df["weather_type"].fillna("").astype(str)
    codes, uniques = pd.factorize(wt)
    table = np.array([_split_present_weather_one(u) for u in uniques], dtype=object)
    table = table.reshape(len(uniques), len(WEATHER_COLUMNS))
    for i, name in enumerate(WEATHER_COLUMNS):
        df[name] = table[codes, i] if len(uniques) else np.array([], dtype=object)
    return df


def _clean_strings(series: pd.Series) -> np.ndarray:
    """Blank-fill and strip a string column, once per distinct value."""
    s = series.fillna("").astype(str)
    codes, uniques = pd.factorize(s)
    stripped = np.array([u.strip() for u in uniques], dtype=object)
    return stripped[codes] if len(uniques) else np.array([], dtype=object)


def _add_city_state(df: pd.DataFrame) -> pd.DataFrame:
    """Derive 'city' and 'state' from the station NAME, once per distinct name."""
    codes, uniques = pd.factorize(df["name"])
    parsed = [_parse_name(u) for u in uniques]
    cities = np.array([p[0] for p in parsed] + [""], dtype=object)
    states = np.array([p[1] for p in parsed] + [""], dtype=object)
    df["city"] = cities[codes]  # code -1 (missing name) selects the trailing ""
    df["state"] = states[codes]
    return df


def _clean_hourly(df: pd.DataFrame, spec: FreqSpec) -> pd.DataFrame:
    """Clean one resampled station frame into the hourly (unclassified) schema."""
    df["prec"] = _clean_precip(df["prec"])
    numeric = [c for c in spec.measure_columns if c != "prec"] + ["lat", "lon", "elev"]
    for col in numeric:
        df[col] = _to_numeric(df[col])

    df = _convert_si_hourly(df)
    df = _split_present_weather(df)
    df["skyc"] = _clean_strings(df["skyc"])
    df = _add_city_state(df)

    df = df.dropna(subset=["prec"])
    df = lst_to_utc(df)
    return df[list(spec.base_columns)].reset_index(drop=True)


def _clean_daily(df: pd.DataFrame, spec: FreqSpec) -> pd.DataFrame:
    """Clean daily-summary rows into the daily schema (Local Standard date)."""
    df = df.copy()
    for col in spec.string_measures:
        df[col] = _clean_strings(df[col])
    for col in DAILY_TRACE_COLUMNS:
        df[col] = _clean_precip(df[col])
    numeric = [
        c for c in spec.measure_columns if c not in DAILY_TRACE_COLUMNS
    ] + ["lat", "lon", "elev"]
    for col in numeric:
        df[col] = _to_numeric(df[col])

    df = _convert_si_daily(df)
    df = _add_city_state(df)

    df["time"] = _parse_datetime(df["time"]).dt.normalize()
    df = df.dropna(subset=["time"])
    return df[list(spec.base_columns)].reset_index(drop=True)


def read_and_clean(
    path: Union[str, Path],
    freq: str = "hourly",
    months: Optional[Sequence[int]] = None,
) -> pd.DataFrame:
    """Read one raw station CSV and return a cleaned frame for ``freq``.

    Only the schema columns for the requested frequency are read. Rows are
    filtered by report type (hourly report types for 'hourly'; SOD summary rows
    for 'daily'). Hourly rows without a precipitation value are discarded.
    When ``months`` is given, only those calendar months are retained, which
    reduces memory during retrieval; for hourly data the month is that of the
    UTC timestamp, for daily data that of the Local Standard date.
    """
    spec = get_freq_spec(freq)
    months = _validate_months(months)
    empty = pd.DataFrame(columns=list(spec.base_columns))
    try:
        raw = pd.read_csv(path, dtype=str, usecols=lambda c: c in spec.raw_to_short)
        for col in spec.raw_to_short:
            if col not in raw.columns:
                raw[col] = np.nan
        df = raw[list(spec.raw_to_short)].rename(columns=spec.raw_to_short)

        if spec.report_types:
            report = df["report_type"].astype(str).str.strip()
            df = df[report.isin(spec.report_types)]
        if df.empty:
            return empty

        if spec.name == "hourly":
            # Only records reporting precipitation are kept, so the rest can be
            # dropped before the (expensive) cleaning and deduplication steps.
            df = df[df["prec"].notnull()]
            if df.empty:
                return empty
            df = _resample_hourly(df)
            df = _clean_hourly(df, spec)
        else:
            df = _clean_daily(df, spec)
            # Robust selection: retain only rows carrying at least one daily
            # measurement, independent of the exact report-type string.
            num = df[list(spec.measure_columns)].apply(
                pd.to_numeric, errors="coerce"
            )
            df = df[num.notna().any(axis=1)]

        if months is not None and not df.empty:
            df = df[df["time"].dt.month.isin(months)]
        return df
    except Exception:
        logger.exception("failed to clean %s", path)
        return empty


# Below this many files a process pool is slower than cleaning in-process.
_MIN_FILES_FOR_PROCESSES = 24


def _available_cpus() -> int:
    """CPUs this process may use (honours affinity masks and container limits)."""
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:  # not available on macOS/Windows
        return max(1, os.cpu_count() or 1)


def _clean_files(
    files: list[str],
    freq: str,
    months: Optional[tuple[int, ...]],
    workers: int,
    scheduler: str,
    progress: bool,
) -> list[pd.DataFrame]:
    """Run :func:`read_and_clean` over ``files``, in parallel when worthwhile."""
    workers = max(1, int(workers))
    if scheduler == "processes":
        # Cleaning is CPU-bound: more processes than cores only add overhead,
        # and starting a pool (importing pandas/xarray in every worker takes
        # about a second) costs more than cleaning a couple of dozen files.
        workers = min(workers, _available_cpus())
        if len(files) < _MIN_FILES_FOR_PROCESSES:
            workers = 1
    if workers == 1 or len(files) == 1:
        bar = _Progress(len(files), "cleaning", progress)
        frames = []
        try:
            for f in files:
                frames.append(read_and_clean(f, freq, months))
                bar.update()
        finally:
            bar.close()
        return frames

    tasks = [delayed(read_and_clean)(file, freq, months) for file in files]
    with ProgressBar() if progress else nullcontext():
        return list(compute(*tasks, scheduler=scheduler, num_workers=workers))


def clean_directory(
    raw_dir: Union[str, Path],
    freq: str = "hourly",
    workers: int = 8,
    classify: bool = True,
    months: Optional[Sequence[int]] = None,
    scheduler: str = "processes",
    progress: bool = True,
) -> pd.DataFrame:
    """Clean every CSV under ``raw_dir`` in parallel, dedup, and classify.

    Cleaning is CPU-bound, so a process pool is used by default, capped at the
    number of available CPUs and only for 24 or more files (smaller jobs are
    cleaned in-process, which is faster than starting the pool). Scripts that
    reach the process pool must guard their entry point with
    ``if __name__ == "__main__":`` (a requirement of Python's ``spawn`` start
    method). Deduplication
    keeps one record per (station_id, time), preferring the record with the
    fewest missing measurements. Convective classification is applied for the
    hourly frequency only.

    Raises
    ------
    EmptyDataFrameError
        If there are no CSV files, or no record survives cleaning (wrong
        report types, no precipitation, or an empty ``months`` selection).
    """
    spec = get_freq_spec(freq)
    months = _validate_months(months)
    raw_dir = Path(raw_dir)
    files = [str(f) for f in sorted(raw_dir.rglob("*.csv"))]
    if not files:
        raise EmptyDataFrameError(f"No CSV files found under {raw_dir}")

    frames = _clean_files(files, spec.name, months, workers, scheduler, progress)
    frames = [f for f in frames if not f.empty]
    if not frames:
        raise EmptyDataFrameError(
            f"No {spec.name} records remained after cleaning {len(files)} file(s); "
            "check freq, months, and the requested years and region."
        )

    df = pd.concat(frames, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for col in spec.measure_columns + ("lat", "lon", "elev"):
        df[col] = df[col].astype(np.float32)
    for col in spec.string_measures + ("station_id", "city", "state"):
        df[col] = df[col].astype(str)
    df = df.dropna(subset=["time"])
    if spec.name == "hourly":
        df = df.dropna(subset=["prec"])

    if df.duplicated(["station_id", "time"]).any():
        df["_nan"] = df[list(spec.measure_columns)].isnull().sum(axis=1)
        df = df.sort_values(
            ["station_id", "time", "_nan"], ascending=[True, True, False]
        )
        df = df.drop_duplicates(["station_id", "time"], keep="last").drop(columns=["_nan"])

    if classify and spec.name == "hourly":
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


def _is_string_dtype(dtype) -> bool:
    if dtype == object:
        return True
    try:
        return bool(pd.api.types.is_string_dtype(dtype)) and not _is_datetime_dtype(
            dtype
        )
    except TypeError:
        return False


def _infer_freq(df: pd.DataFrame) -> str:
    """Guess the frequency of a cleaned frame from its columns."""
    return "daily" if "tmax" in df.columns else "hourly"


def _scatter_column(
    values: np.ndarray, flat: np.ndarray, shape: tuple[int, int]
) -> np.ndarray:
    """Place per-row ``values`` into a (station, time) grid at flat positions."""
    if values.dtype == object:
        grid = np.full(shape, np.nan, dtype=object)
    else:
        grid = np.full(shape, np.nan, dtype=values.dtype)
    grid.reshape(-1)[flat] = values
    return grid


def to_xarray(df: pd.DataFrame, freq: Optional[str] = None) -> xr.Dataset:
    """Reshape a cleaned frame into a CF timeSeries Dataset (station, time).

    The station identifier is retained as a string coordinate so that leading
    zeros in NOAA station IDs are preserved. Present-weather and other string
    fields are kept as readable strings; compact categorical encoding is
    applied only on write (:func:`to_netcdf`). Cells with no record are NaN.

    ``freq`` ('hourly' or 'daily') selects frequency-specific variable units;
    if omitted it is inferred from the columns.
    """
    freq = freq or _infer_freq(df)
    df = df.dropna(subset=["station_id", "time"]).drop_duplicates(
        ["station_id", "time"]
    )

    station_codes, station_index = pd.factorize(df["station_id"], sort=True)
    time_codes, time_index = pd.factorize(df["time"], sort=True)
    stations = np.asarray(station_index, dtype=object)
    times = np.asarray(time_index.to_numpy())
    shape = (len(stations), len(times))
    flat = station_codes.astype(np.int64) * shape[1] + time_codes

    coord_cols = [c for c in COORD_COLUMNS if c in df.columns]
    ds = xr.Dataset(coords={"station": stations, "time": times})

    if coord_cols:
        meta = (
            df[coord_cols]
            .groupby(station_codes, sort=True)
            .first()
            .reindex(range(shape[0]))
        )
    for c in coord_cols:
        values = meta[c].values
        if c in STRING_COLUMNS:
            values = np.asarray(values, dtype=object)
        ds = ds.assign_coords({c: ("station", values)})

    value_cols = [
        c for c in df.columns if c not in ("station_id", "time", *coord_cols)
    ]
    for col in value_cols:
        series = df[col]
        if col in STRING_COLUMNS:
            grid = _scatter_column(series.astype(object).to_numpy(), flat, shape)
        elif _is_float_dtype(series.dtype):
            grid = _scatter_column(series.to_numpy(), flat, shape)
        else:  # integer, boolean, ...: keep pandas' dtype rules for padded grids
            grid = (
                df.assign(_s=station_codes, _t=time_codes)
                .pivot(index="_s", columns="_t", values=col)
                .reindex(index=range(shape[0]), columns=range(shape[1]))
                .values
            )
        ds[col] = (("station", "time"), grid)

    for name in ds.variables:
        attrs = variable_attrs(str(name), freq)
        if _is_datetime_dtype(ds[name].dtype):
            attrs.pop("units", None)  # CF datetime encoding owns 'units'
        ds[name].attrs.update(attrs)

    ds.attrs.update(
        title="NOAA Local Climatological Data (cleaned, SI units)",
        source=BASE_URL,
        featureType="timeSeries",
        Conventions="CF-1.8",
    )
    return ds


def _category_dtype(n_categories: int):
    """Smallest signed integer type that can hold codes 0 .. n_categories-1."""
    if n_categories <= np.iinfo(np.int16).max + 1:
        return np.int16
    if n_categories <= np.iinfo(np.int32).max + 1:
        return np.int32
    return np.int64


def _encode_for_netcdf(ds: xr.Dataset) -> tuple[xr.Dataset, dict[str, dict]]:
    """Return a copy of ``ds`` with compact codes and its netCDF encoding.

    ``prec_type`` becomes a CF int8 flag variable; every other object-dtype
    field (present-weather groups, sky condition, daily weather) becomes
    integer categorical codes (int16, widened to int32/int64 when a field has
    more than 32768 distinct values) with a JSON 'categories' attribute; float
    fields are stored as compressed float32.
    """
    ds = ds.copy()
    encoding: dict[str, dict] = {}

    if "prec_type" in ds.data_vars:
        dims = ds["prec_type"].dims
        raw = ds["prec_type"].values
        lookup = {v: i for i, v in enumerate(_PREC_FLAGS)}
        coded = (
            pd.Series(raw.ravel(), dtype=object)
            .map(lookup)
            .fillna(0)
            .to_numpy()
            .astype("int8")
            .reshape(raw.shape)
        )
        ds["prec_type"] = (dims, coded)
        ds["prec_type"].attrs = {
            "long_name": variable_attrs("prec_type").get("long_name", ""),
            "flag_values": np.arange(len(_PREC_FLAGS), dtype="int8"),
            "flag_meanings": " ".join(_PREC_FLAGS),
        }
        encoding["prec_type"] = {"zlib": True, "complevel": 4}

    for name in list(ds.data_vars):
        if name == "prec_type":
            continue
        var = ds[name]
        if not _is_string_dtype(var.dtype):
            continue
        dims = var.dims
        raw = var.values
        flat = pd.Series(raw.ravel()).fillna("").astype(str)
        codes, cats = pd.factorize(flat, sort=True)
        codes = codes.astype(_category_dtype(len(cats)))
        ds[name] = (dims, codes.reshape(raw.shape))
        ds[name].attrs = {
            "long_name": variable_attrs(name).get("long_name", ""),
            "categories": json.dumps([str(c) for c in cats]),
        }
        encoding[name] = {"zlib": True, "complevel": 4}

    for name, var in ds.data_vars.items():
        if _is_float_dtype(var.dtype):
            encoding[name] = {"zlib": True, "complevel": 4, "dtype": "float32"}

    return ds, encoding


def to_netcdf(ds: xr.Dataset, path: Union[str, Path]) -> Path:
    """Write a Dataset to compressed (station, time) netCDF and return the path.

    Present-weather and other categorical string fields are stored compactly to
    avoid VLEN string overhead (see :func:`_encode_for_netcdf`).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    enc_ds, encoding = _encode_for_netcdf(ds)
    enc_ds.to_netcdf(path, engine="netcdf4", encoding=encoding)
    return path


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

    for name in list(ds.data_vars):
        if "categories" in ds[name].attrs:
            cats = np.array(json.loads(ds[name].attrs["categories"]), dtype=object)
            ds[name] = (ds[name].dims, cats[ds[name].values])
            ds[name].attrs["long_name"] = variable_attrs(name).get("long_name", "")
    return ds


def read_netcdf(path: Union[str, Path]) -> pd.DataFrame:
    """Read a (station, time) netCDF into a long frame with padding removed."""
    ds = open_xarray(path)
    df = ds.to_dataframe().reset_index().rename(columns={"station": "station_id"})
    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: v.decode() if isinstance(v, bytes) else v)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    if "prec" in df.columns:
        df = df.dropna(subset=["prec"])  # drop (station, time) padding cells
    lead = [c for c in ("time", "station_id", *COORD_COLUMNS) if c in df.columns]
    rest = [c for c in df.columns if c not in lead]
    return df[lead + rest].sort_values(["time", "lat", "lon"]).reset_index(drop=True)


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
    freq: str = "hourly",
    keep_raw: bool = False,
    progress: bool = True,
) -> pd.DataFrame:
    """Select, download, and clean LCD records at the requested frequency.

    Downloads use at least 16 threads regardless of ``workers`` (they are
    network-bound); ``workers`` and ``scheduler`` control the cleaning pool.
    """
    months = _validate_months(months)
    inv = select_stations(region, stations_file)
    if inv.empty:
        raise EmptyDataFrameError("No stations match the requested region.")

    made_tmp = raw_dir is None
    raw_dir = Path(tempfile.mkdtemp(prefix="lcd_")) if made_tmp else Path(raw_dir)
    try:
        tally = download_stations(
            inv,
            region.start_year,
            region.end_year,
            raw_dir,
            base_url=base_url,
            workers=max(workers, 16),
            scheduler=scheduler,
            progress=progress,
        )
        if tally["error"]:
            logger.warning(
                "%d of %d station-year downloads failed (%d succeeded, %d not "
                "published by NOAA); the result is incomplete. Re-run to retry.",
                tally["error"],
                sum(tally.values()),
                tally["ok"],
                tally["missing"],
            )
        df = clean_directory(
            raw_dir,
            freq=freq,
            workers=workers,
            scheduler=scheduler,
            classify=classify,
            months=months,
            progress=progress,
        )
    finally:
        if made_tmp and not keep_raw:
            shutil.rmtree(raw_dir, ignore_errors=True)

    return df
