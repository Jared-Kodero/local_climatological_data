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
```

Core requirements: numpy, pandas, xarray, netCDF4, dask, requests. The station
inventory ships in `lcd/data/airports.stations`.

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
)
```

Returns an `xarray.Dataset` with dimensions `(station, time)`.

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

## netCDF structure

Files are written as a CF `timeSeries` dataset with dimensions
`(station, time)`. The station coordinate holds the NOAA identifier as a string,
preserving leading zeros. Latitude, longitude, elevation, city, and state are
coordinates along the station dimension; measurement variables are stored as
float32 with zlib compression. String fields are stored compactly: `prec_type`
as a CF int8 flag variable and the remaining categorical fields (au, aw, mw,
skyc, wt) as int16 codes with a JSON category table, all decoded back to strings
on read. Station identifiers and observation times are unique along their
dimensions.

## References

- NOAA NCEI, Local Climatological Data (LCD): https://www.ncei.noaa.gov/products/land-based-station/local-climatological-data
- NOAA NCEI, Local Climatological Data Dataset Documentation: https://www.ncei.noaa.gov/pub/data/cdo/documentation/LCD_documentation.pdf
- NOAA NCEI, Local Climatological Data Version 2 (LCDv2) Dataset Documentation: https://www.ncei.noaa.gov/oa/local-climatological-data/v2/doc/lcdv2_DOCUMENTATION.pdf
- Kantor, D., Casey, N. W., Menne, M. J., Buddenberg, A. (2023). Local Climatological Data (LCD), Version 2. NOAA NCEI. https://doi.org/10.25921/jp3d-3v19
- Eagleson, P. S. (1972). Dynamics of flood frequency. Water Resources Research, 8(4), 878-898. https://doi.org/10.1029/WR008i004p00878
- Restrepo-Posada, P. J., Eagleson, P. S. (1982). Identification of independent rainstorms. Journal of Hydrology, 55(1-4), 303-319. https://doi.org/10.1016/0022-1694(82)90136-6
- Natural Earth populated places: https://www.naturalearthdata.com
