# local_climatological_data

Download, clean, and load NOAA Local Climatological Data (LCDv2) station
records. Stations are selected from the packaged NCEI inventory by bounding box
and year range, downloaded as per-station annual CSV files, filtered to a single
requested temporal frequency (hourly or daily), converted to SI units,
deduplicated on station-time, optionally classified into convective and
stratiform regimes, and returned as an `xarray.Dataset`.

All user-facing functions return `xarray.Dataset` objects. DataFrame return
options have been removed; use `Dataset.to_dataframe()` if a frame is required.

## Installation

```bash
pip install -e .
pip install -e ".[cities]"   # optional: cartopy-based city assignment
pip install -e ".[test]"     # optional: pytest, to run the test suite
```

Core requirements: numpy, pandas, xarray, netCDF4, dask, requests (Python 3.10
or later). The station inventory ships in `lcd/data/airports.stations`. The test
suite passes with the minimum declared versions (Python 3.11, pandas 2.0,
xarray 2023.1, dask 2023.1) and with current releases (Python 3.13, pandas
2.2 and 3.0).

## Example download

A representative NCEI station-year file and the dataset documentation ship with
the package under `lcd/data/example_download`:

```
lcd/data/example_download/
    01001099999.csv         # NCEI LCDv2 station-year file (JAN MAYEN, 2025)
    LCD_documentation.pdf   # NCEI LCD Dataset Documentation
```

The CSV shows the full set of columns NCEI distributes. Hourly fields are
prefixed `Hourly*`, daily-summary fields `Daily*`, and monthly fields
`Monthly*`; this package reads the hourly or daily subset listed under
[Schema](#schema). The file is reachable at `lcd.EXAMPLE_DIR` and can be cleaned
directly without any network access:

```python
import lcd

df = lcd.read_and_clean(lcd.EXAMPLE_DIR / "01001099999.csv", freq="hourly")
ds = lcd.to_xarray(df)
```

## User-facing functions

### get_lcd_from_noaa

```python
import lcd

ds = lcd.get_lcd_from_noaa(
    lon_min=-73, lon_max=-66, lat_min=40, lat_max=50,
    min_year=1980, max_year=2024,
    freq="hourly",              # "hourly" or "daily"
    months=[6, 7, 8],           # optional calendar-month filter
    classify_convective=True,   # adds a 'prec_type' variable (hourly only)
    output="lcd.nc",            # optional: also write compressed netCDF
    progress=True,              # download/cleaning progress on stderr
)
```

Returns an `xarray.Dataset` with dimensions `(station, time)`. `months` must be
a non-empty sequence of integers from 1 to 12; invalid values raise
`ValueError` before anything is downloaded. `lcd.EmptyDataFrameError` is raised
if no station matches the region, no file could be downloaded, or no record
remains after cleaning.

### Station selection and file names

Stations are chosen from the packaged inventory by bounding box, `country`
(default `"US"`), and overlap of their period of record with
`min_year`-`max_year`. `min_year_range` is the minimum length of that period
of record (last year minus first year). NOAA names each file
`<year>/<USAF><WBAN>.csv`, with USAF padded to 6 and WBAN to 5 characters; the
inventory stores both without leading zeros, so the identifiers are
zero-padded before requesting (for example USAF 701337 with WBAN 103 is
requested as `70133700103`). Version 0.3.0 did not pad, which made roughly one
in eight US inventory stations (925 of 7,170) fail with a silent 404.

The packaged inventory is a snapshot (newest END date 2023-09-08). A station
whose END date lies within 90 days of the snapshot was still reporting when it
was built, so its period of record is treated as open-ended through the current
calendar year. Years in which such a station has no published file return HTTP
404 and are counted as not published. Stations that ended earlier keep their
inventory END year. Without this rule, no request for 2024 or later could match
any station.

### Parallelism

Downloads are network-bound and run on a pool of at least 16 threads, one
keep-alive session per thread. Cleaning is CPU-bound and runs on a Dask process
pool (`scheduler="processes"`, the default) limited to the available CPUs and
used only for 24 or more files; smaller requests are cleaned in-process because
starting the pool takes longer than the work. `workers=1` always cleans
serially. Scripts that reach the process pool must guard their entry point with
`if __name__ == "__main__":`, as required by Python's `spawn` start method;
use `scheduler="threads"` to avoid this. Failed downloads (after four retries,
including bodies cut off mid-transfer) are logged as warnings and, if any occur, a final warning reports that the
result is incomplete.

### Selecting the temporal frequency

`freq` selects which records are retained; rows belonging to the other
frequency, and all monthly and normals rows, are discarded.

| freq | Report types retained | Time convention | Variables |
| --- | --- | --- | --- |
| `"hourly"` | FM-12, FM-15, FM-16 | UTC, ceiled to the hour | hourly set |
| `"daily"` | SOD | Local Standard calendar date | daily-summary set |

```python
ds_hourly = lcd.get_lcd_from_noaa(..., freq="hourly")
ds_daily  = lcd.get_lcd_from_noaa(..., freq="daily")
```

Daily summaries carry no sub-hourly present-weather groups, so
`classify_convective` is ignored when `freq="daily"`.

Hourly records that report no precipitation value are discarded, so the hourly
dataset contains only observations with a precipitation amount (including
`0.00`). The `months` filter uses the UTC timestamp for hourly data and the
Local Standard date for daily data. Hourly UTC timestamps near a year boundary
can fall in the adjacent calendar year.

### open_dataset and save_dataset

```python
ds = lcd.open_dataset("lcd.nc")        # xarray Dataset (station, time)
lcd.save_dataset(ds, "copy.nc")        # compressed netCDF, returns the path
```

### get_durations and get_lag

Both accept a Dataset or a stored file path, return a Dataset, and can be
written directly by passing `output`.

```python
durations = lcd.get_durations(ds)                     # 'duration' variable, minutes
lcd.get_durations(ds, output="durations.nc")
lcd.get_lag(ds, lag=1, output="lag1.nc")              # within-day lag
```

## Classification

Records retain the raw present-weather groups `au`, `aw`, and `mw`, so users may
define their own schemes. The default convective classification is applied
during retrieval (`classify_convective=True`) and can also be applied
separately to any frame carrying those columns.

```python
from lcd.classify import add_precip_type, convective_mask, precip_type

df = add_precip_type(df)      # adds 'prec_type'
mask = convective_mask(df)    # boolean flag
labels = precip_type(df)      # convective | stratiform | none
```

## Cities

`add_city_names(df)` assigns `city` and `state` from the nearest Natural Earth
populated place using cartopy, keyed on `lat` and `lon`. cartopy downloads the
shapefile on first use; if cartopy or the shapefile is unavailable the frame is
returned unchanged. Enable it during retrieval with
`get_lcd_from_noaa(..., add_cities=True)`.

## Schema

### Shared

| Short | Long name | Units |
| --- | --- | --- |
| time | Observation Time | UTC (hourly), LST date (daily) |
| station_id | NOAA Station Identifier | |
| lat | Station Latitude | degrees_north |
| lon | Station Longitude | degrees_east |
| elev | Station Elevation | m |
| city | Station City | |
| state | Station State | |

### Hourly (`freq="hourly"`)

| Short | Long name | Units | Source column |
| --- | --- | --- | --- |
| t | Air Temperature | degC | HourlyDryBulbTemperature |
| dpt | Dew Point Temperature | degC | HourlyDewPointTemperature |
| wbt | Wet Bulb Temperature | degC | HourlyWetBulbTemperature |
| rh | Relative Humidity | % | HourlyRelativeHumidity |
| wd | Wind Direction | degrees | HourlyWindDirection |
| ws | Wind Speed | m/s | HourlyWindSpeed |
| wsg | Wind Gust Speed | m/s | HourlyWindGustSpeed |
| sp | Sea Level Pressure | hPa | HourlySeaLevelPressure |
| stp | Station Pressure | hPa | HourlyStationPressure |
| alt | Altimeter Setting | hPa | HourlyAltimeterSetting |
| pchg | 3-Hour Net Pressure Change | hPa | HourlyPressureChange |
| ptend | Pressure Tendency Code (0-8) | | HourlyPressureTendency |
| vis | Horizontal Visibility | km | HourlyVisibility |
| prec | Precipitation | mm/hr | HourlyPrecipitation |
| skyc | Sky Condition / Cloud Layers | | HourlySkyConditions |
| au | Automated Present Weather (AU group) | | HourlyPresentWeatherType |
| aw | Automated Present Weather (AW group) | | HourlyPresentWeatherType |
| mw | Manual Present Weather (MW group) | | HourlyPresentWeatherType |
| prec_type | Precipitation regime | convective, stratiform, none | derived |

`wbt`, `alt`, `pchg`, `ptend`, and `skyc` are standard atmospheric-analysis
fields added in version 0.3.0. Wet-bulb temperature supports moist
thermodynamic diagnostics; the altimeter setting and the 3-hour pressure change
and tendency code support synoptic pressure-trend analysis; sky condition
carries cloud coverage in oktas and layer base heights.

`skyc` follows the documented `ccc:ll-xxx` layer format, where `ccc` is the
coverage contraction (CLR, FEW, SCT, BKN, OVC, VV), `ll` is the layer amount in
oktas (00-08, with 09 an obscuration and 10 a partial obscuration), and `xxx` is
the cloud base height in hundreds of feet. Up to three layers are reported.

`ptend` is the WMO pressure-tendency characteristic: codes 0-3 indicate a net
pressure increase over the previous three hours, 4 no change, and 5-8 a net
decrease.

Sign of `pchg` (data caveat). The NCEI documentation states that the 3-hour net
change is prefixed with `+` for an increase and `-` for a decrease. In the
shipped example file (JAN MAYEN, 2025) the reported sign is the opposite: across
1,501 pairs of reports exactly three hours apart (reported change larger than
0.015 inHg in magnitude) the correlation between the reported change and the
actual change in station pressure is -0.99, while `ptend` agrees with the actual
direction (codes 0-3 average -0.034 inHg, codes 5-8 average +0.033 inHg). The
package converts the reported value and does not alter its sign. This has been
checked for one station only; until the convention is confirmed for the stations
of interest, derive the change from `stp` or `sp`, or use `ptend` for direction.

### Daily (`freq="daily"`)

| Short | Long name | Units | Source column |
| --- | --- | --- | --- |
| tmax | Daily Maximum Air Temperature | degC | DailyMaximumDryBulbTemperature |
| tmin | Daily Minimum Air Temperature | degC | DailyMinimumDryBulbTemperature |
| tavg | Daily Average Air Temperature | degC | DailyAverageDryBulbTemperature |
| dpt | Dew Point Temperature | degC | DailyAverageDewPointTemperature |
| wbt | Wet Bulb Temperature | degC | DailyAverageWetBulbTemperature |
| rh | Relative Humidity | % | DailyAverageRelativeHumidity |
| stp | Station Pressure | hPa | DailyAverageStationPressure |
| sp | Sea Level Pressure | hPa | DailyAverageSeaLevelPressure |
| ws | Wind Speed | m/s | DailyAverageWindSpeed |
| wsg | Wind Gust Speed | m/s | DailyPeakWindSpeed |
| wd | Wind Direction | degrees | DailyPeakWindDirection |
| ws_sust | Daily Maximum Sustained Wind Speed | m/s | DailySustainedWindSpeed |
| wd_sust | Daily Maximum Sustained Wind Direction | degrees | DailySustainedWindDirection |
| prec | Precipitation | mm | DailyPrecipitation |
| snow | Daily Snowfall | mm | DailySnowfall |
| snwd | Daily Snow Depth | mm | DailySnowDepth |
| hdd | Heating Degree Days (base 65 degF) | degF-day | DailyHeatingDegreeDays |
| cdd | Cooling Degree Days (base 65 degF) | degF-day | DailyCoolingDegreeDays |
| wt | Daily Weather Type (GHCN-Daily WT codes) | | DailyWeather |

Degree days are left on their reported 65 degF base, since the value is
base-specific and not meaningfully expressed in SI.

## Unit conversions

Conversions follow the NCEI LCD Dataset Documentation
(`lcd/data/example_download/LCD_documentation.pdf`): temperature from degrees
Fahrenheit, wind speed from miles per hour, precipitation, snowfall, and snow
depth from inches (trace = 0.005 in), station, sea level, and altimeter
pressures and the 3-hour pressure change from inches of mercury, and visibility
from miles.

For the hourly frequency, observation times are ceiled to the hour, and among
duplicate hours the record reporting precipitation with the fewest missing
fields is kept.

## Time zone handling

LCD timestamps are Local Standard Time and do not use daylight saving. For
hourly records the standard offset is derived from longitude,

```
offset = round( ((lon + 180) mod 360 - 180) * 24 / 360 )   [hours]
UTC = LST - offset
```

and applied per station. Daily summaries describe a Local Standard calendar day
and are therefore normalised to that date without a UTC shift.

The longitude rule reproduces the timestamps of the shipped example (Jan Mayen,
FM-12 synoptic reports): after conversion 65% of its reports fall on a synoptic
hour (00, 03, 06, ... UTC), against 17% if the legal offset of UTC+1 were used.
It is not the legal standard offset for every US station, however: it gives
-6 h for Atlanta Hartsfield (-84.44 degrees; legal EST -5 h), -6 h for Detroit
Metro (-83.33 degrees; EST -5 h), -4 h for Eastport, Maine (-67.01 degrees; EST
-5 h) and -10 h for Anchorage (-150.03 degrees; AKST -9 h). Whether NCEI's own
Local Standard Time for US stations follows the legal offset or this rule has
not been verified here. To check a station, compare the `Sunrise` and `Sunset`
columns of a downloaded file (Local Standard Time) with astronomical solar noon
for its coordinates; the two candidate offsets differ by a full hour.

## netCDF structure

Files are written as a CF `timeSeries` dataset with dimensions
`(station, time)`. The station coordinate holds the NOAA identifier as a string,
preserving leading zeros. Latitude, longitude, elevation, city, and state are
coordinates along the station dimension; measurement variables are stored as
float32 with zlib compression. String fields are stored compactly: `prec_type`
as a CF int8 flag variable and the remaining categorical fields (au, aw, mw,
skyc, wt) as integer codes with a JSON category table, all decoded back to
strings on read. Codes are int16 for up to 32,768 distinct values and widen to
int32 beyond that; version 0.3.0 always used int16, which silently corrupted
fields with more distinct values (sky condition strings can exceed this in
large requests). Station identifiers and observation times are unique along their
dimensions.

## Development

```bash
pip install -e ".[test]"
pytest                                   # offline; downloads use a local mock server
python benchmarks/bench.py --data /tmp/lcd_bench --stations 16 --workers 4
```

The tests need no network access: station files are synthesised in the NCEI
layout and served from a local HTTP server. `benchmarks/bench.py` times each
stage (cleaning, reshaping, netCDF I/O, durations, lag, downloads) on synthetic
station-years and reuses the generated files between runs so that two versions
can be compared on identical input. See `CHANGELOG.md` for measured speed-ups.

## References

- NOAA NCEI, Local Climatological Data (LCD): https://www.ncei.noaa.gov/products/land-based-station/local-climatological-data
- NOAA NCEI, Local Climatological Data Dataset Documentation: https://www.ncei.noaa.gov/pub/data/cdo/documentation/LCD_documentation.pdf
- NOAA NCEI, Local Climatological Data Version 2 (LCDv2) Dataset Documentation: https://www.ncei.noaa.gov/oa/local-climatological-data/v2/doc/lcdv2_DOCUMENTATION.pdf
- Kantor, D., Casey, N. W., Menne, M. J., Buddenberg, A. (2023). Local Climatological Data (LCD), Version 2. NOAA NCEI. https://doi.org/10.25921/jp3d-3v19
- Eagleson, P. S. (1972). Dynamics of flood frequency. Water Resources Research, 8(4), 878-898. https://doi.org/10.1029/WR008i004p00878
- Restrepo-Posada, P. J., Eagleson, P. S. (1982). Identification of independent rainstorms. Journal of Hydrology, 55(1-4), 303-319. https://doi.org/10.1016/0022-1694(82)90136-6
- Natural Earth populated places: https://www.naturalearthdata.com
