# local_climatological_data

Download, clean, and load NOAA Local Climatological Data (LCD) hourly station
records. The package selects stations from the NCEI inventory by bounding box
and year range, downloads the per-station annual CSV files, converts every
field to SI units, converts Local Standard Time to UTC, deduplicates
station-time pairs, optionally classifies precipitation into convective and
stratiform regimes, and serializes to compressed netCDF. A pandas.DataFrame
subclass provides temporal and spatial selection, precipitation-event
durations, and within-day lagged predictors.

## Installation

```bash
pip install -e .
```

Requires numpy, pandas, xarray, netCDF4, and requests.

## Usage

Download a region and write a netCDF file (timestamps in the result are UTC):

```python
from lcd import LocalClimatologicalData

data = LocalClimatologicalData.from_noaa(
    lat_min=25, lat_max=50, lon_min=-125, lon_max=-65,
    start_year=1980, end_year=2024,
    output="~/data/noaa/station.usa.nc",
)
```

Load a pre-processed file:

```python
data = LocalClimatologicalData.open_data("~/data/noaa/station.usa.nc")
```

Subset, then derive event durations or lagged predictors:

```python
jja = data.sel(convective=True, season="JJA", bbox=(40, 50, -73, -66))
durations = jja.get_durations()      # 'duration' column, minutes
lagged = jja.lag(1)                  # within-day 1-step lag
```

## Selection

`sel` combines all supplied filters with logical AND.

Precipitation: `convective` (True selects convective, False selects
stratiform), `prec_types`, `min_p` (mm/hr), `wet_only`.
Time (UTC): `start`, `end`, `years`, `months`, `hours`, `season`
(`DJF`, `MAM`, `JJA`, `SON`).
Space: `bbox=(lat_min, lat_max, lon_min, lon_max)`, `lats`, `lons` (paired when
both given), `cities`, `states`, `station_ids`.

`data.stations` returns the unique stations with coordinates and record counts.

## Classification

Records retain the raw present-weather groups `au`, `aw`, and `mw`, so users
may define their own schemes. The default convective classification is applied
during `build`/`from_noaa` (`classify=True`) and can also be applied
separately:

```python
from lcd.classify import add_precip_type, convective_mask, precip_type

data = add_precip_type(data)                 # adds 'prec_type'
mask = convective_mask(data)                 # boolean flag
labels = precip_type(data)                   # convective | stratiform | none
```

A record is convective if any token in its `au`, `aw`, or `mw` group matches
the corresponding convective code set (thunderstorm, shower, hail, funnel, or
squall descriptors). The regime is then convective when p > 0 and the flag is
set, stratiform when p > 0 and not set, and none when p == 0. Custom code sets
may be passed to the classification functions.

## Schema

Cleaned columns, with SI units and long names.

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
| p | Hourly Precipitation Intensity | mm/hr |
| au | Automated Present Weather (AU group) | |
| aw | Automated Present Weather (AW group) | |
| mw | Manual Present Weather (MW group) | |
| prec_type | Precipitation regime | convective, stratiform, none |

Unit conversions follow the NCEI LCD Dataset Documentation: temperature from
degrees Fahrenheit, wind speed from miles per hour, precipitation from inches
(trace = 0.005 in), station and sea level pressure from inches of mercury, and
visibility from miles. Only hourly report types (FM-12, FM-15, FM-16) are
retained; daily and monthly summary rows are discarded. A latent
sign-stripping bug in prior regex cleaning is fixed, so sub-zero temperatures
are preserved.

## Time zone handling

LCD timestamps are Local Standard Time and do not use daylight saving. The
standard offset is derived from longitude,

```
offset = round( ((lon + 180) mod 360 - 180) * 24 / 360 )   [hours]
UTC = LST - offset
```

and applied per station. Because LST is year-round standard time, this
longitude-based offset is exact for the standard-time convention and avoids
daylight-saving ambiguity.

## netCDF structure

Files are written as a CF `timeSeries` dataset with dimensions
`(station, time)`. Latitude, longitude, elevation, city, and state are
coordinates along the station dimension; measurement variables are stored as
float32 with zlib compression. Station identifiers and observation times are
unique along their respective dimensions. This layout keeps latitude,
longitude, and time all present and selectable while avoiding the empty
`n_lat x n_lon x n_time` cube that three independent axes would create for
point stations. The dataset is available in memory with `data.to_xarray()`.

## References

- NOAA NCEI, Local Climatological Data (LCD): https://www.ncei.noaa.gov/products/land-based-station/local-climatological-data
- Kantor, D., Casey, N. W., Menne, M. J., Buddenberg, A. (2023). Local Climatological Data (LCD), Version 2. NOAA NCEI. https://doi.org/10.25921/jp3d-3v19
- Eagleson, P. S. (1972). Dynamics of flood frequency. Water Resources Research, 8(4), 878-898.
- Restrepo-Posada, P. J., Eagleson, P. S. (1982). Identification of independent rainstorms. Journal of Hydrology, 55(1-4), 303-319.
