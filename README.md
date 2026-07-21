# local_climatological_data

Download, clean, and load NOAA Local Climatological Data (LCD) hourly station
records. Stations are selected from the packaged NCEI inventory by bounding box
and year range, downloaded as per-station annual CSV files, converted to SI
units, converted from Local Standard Time to UTC, deduplicated on
station-time, optionally classified into convective and stratiform regimes,
and stored as compressed netCDF.

## Installation

```bash
pip install -e .
pip install -e ".[cities]"   # optional: cartopy-based city assignment
```

Core requirements: numpy, pandas, xarray, netCDF4, requests. The station
inventory ships in `lcd/data/airports.stations`.

## User-facing functions

### get_lcd_from_noaa

```python
import lcd

path = lcd.get_lcd_from_noaa(
    lon_min=-73, lon_max=-66, lat_min=40, lat_max=50,
    min_year=1980, max_year=2024,
    months=[6, 7, 8],           # optional calendar-month filter (UTC)
    classify_convective=True,   # adds a 'prec_type' column
    as_netcdf=True,             # returns the .nc path; False returns a DataFrame
)
```

With `as_netcdf=True` the cleaned records are written to a compressed
`(station, time)` netCDF and the file path is returned. With `as_netcdf=False`
the cleaned `pandas.DataFrame` is returned. Retrieval downloads concurrently
and cleans in parallel across processes.

### open_dataset

```python
df = lcd.open_dataset(path, engine="pandas")     # long DataFrame, padding removed
ds = lcd.open_dataset(path, engine="netcdf")     # xarray Dataset (station, time)
```

### get_durations and get_lag

Both accept a DataFrame or a stored file path and can be saved directly by
passing ``output``.

```python
durations = lcd.get_durations(df)                       # 'duration' column, minutes
lcd.get_durations(df, output="durations.nc")            # writes netCDF, returns path
lcd.get_lag(df, lag=1, output="lag1.nc")                # within-day lag, returns path
```

## Classification

Records retain the raw present-weather groups `au`, `aw`, and `mw`, so users
may define their own schemes. The default convective classification is applied
during retrieval (`classify_convective=True`) and can also be applied
separately.

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

| Short | Long name | Units |
| --- | --- | --- |
| time | Observation Time (UTC) | UTC |
| station_id | NOAA Station Identifier | |
| lat | Station Latitude | degrees_north |
| lon | Station Longitude | degrees_east |
| elev | Station Elevation | m |
| city | Station City | |
| state | Station State | |
| t | Air Temperature | degC |
| dpt | Dew Point Temperature | degC |
| rh | Relative Humidity | % |
| wd | Wind Direction | degrees |
| ws | Wind Speed | m/s |
| wsg | Wind Gust Speed | m/s |
| sp | Sea Level Pressure | hPa |
| stp | Station Pressure | hPa |
| vis | Horizontal Visibility | km |
| prec | Hourly Precipitation Intensity | mm/hr |
| au | Automated Present Weather (AU group) | |
| aw | Automated Present Weather (AW group) | |
| mw | Manual Present Weather (MW group) | |
| prec_type | Precipitation regime | convective, stratiform, none |

Unit conversions follow the NCEI LCD Dataset Documentation: temperature from
degrees Fahrenheit, wind speed from miles per hour, precipitation from inches
(trace = 0.005 in), station and sea level pressure from inches of mercury, and
visibility from miles. Only hourly report types (FM-12, FM-15, FM-16) are
retained; daily and monthly summary rows are discarded. Observation times are
ceiled to the hour, and among duplicate hours the record reporting
precipitation with the fewest missing fields is kept.

## Time zone handling

LCD timestamps are Local Standard Time and do not use daylight saving. The
standard offset is derived from longitude,

```
offset = round( ((lon + 180) mod 360 - 180) * 24 / 360 )   [hours]
UTC = LST - offset
```

and applied per station.

## netCDF structure

Files are written as a CF `timeSeries` dataset with dimensions
`(station, time)`. Latitude, longitude, elevation, city, and state are
coordinates along the station dimension; measurement variables are stored as
float32 with zlib compression. The present-weather fields are stored compactly:
`prec_type` as a CF int8 flag variable and au/aw/mw as int16 categorical codes
with a JSON category table, all decoded back to strings on read. Station
identifiers and observation times are unique along their dimensions.

## References

- NOAA NCEI, Local Climatological Data (LCD): https://www.ncei.noaa.gov/products/land-based-station/local-climatological-data
- Kantor, D., Casey, N. W., Menne, M. J., Buddenberg, A. (2023). Local Climatological Data (LCD), Version 2. NOAA NCEI. https://doi.org/10.25921/jp3d-3v19
- Eagleson, P. S. (1972). Dynamics of flood frequency. Water Resources Research, 8(4), 878-898.
- Restrepo-Posada, P. J., Eagleson, P. S. (1982). Identification of independent rainstorms. Journal of Hydrology, 55(1-4), 303-319.
- Natural Earth populated places: https://www.naturalearthdata.com
