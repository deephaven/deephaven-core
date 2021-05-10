# Weather Observation Data

This directory contains daily weather observations observations from 19 different locations around the world. The observations start in the 1940s and are continuous to the present day. In total, there are 57330 observations.

The samples come from the NOAA Global Surface Summary of the Day (GSOD) collection.

The `station_data.csv` file describes each observation station; there are data files name after each city where the observations were made. Because stations began recording data at different times, the files have unequal starting points.


## Fields in the `station_data.csv` file

`station_data.csv` has 11 fields:

- **USAF:** US Air Force identifier for the station
- **WBAN:** five-digit WBAN (Weather Bureau Army-Navy) identifier for the station
- **Station Name:** Name of station
- **CTRY:** Two-letter ISO-ALPHA2 country code
- **STATE:** State (or geographic subdivision) where observation was made
- **ICAO:** International Civil Aviation Organization code for the airport where observations were made. Might be blank.
- **LAT:** decimal latitude of the station (negative is west)
- **LON:** decimal longitude of the station (positive is north)
- **ELEV(M):** Elevation of the station in meters
- **BEGIN:** First day station observations are available, as YYYYMMDD
- **END:** Last day station observations are available, as YYYYMMDD

Some stations don't have a `WBAN` number, and this field is "99999" for those stations.

## Fields in each observation file

There are fourteen fields recorded in the observation file. The `USAF` and `WBAN` fields together act as a composite foreign key to the `station_data.csv` record for the station reporting the observation.

- **USAF:** US Air Force identifier for the station
- **WBAN:** five-digit WBAN (Weather Bureau Army-Navy) identifier for the station
- **ObservationDate:** Date of the observation, as YYYMMDD
- **TemperatureF:** mean temperature of the day, in Fahrenheit
- **DewPointF:** mean dew-pont temperature of the day, in Fahrenheit 
- **SeaLevelPressureMB:** average normalized sea-level pressure in millibars
- **StationPressureMB:** average station atmospheric pressure in millibars:
- **VisibilityFeet:** Average visibility, in feet
- **MeanWindSpeedMPH:** average wind speed in miles per hour
- **MaxWindSpeedMPH:** maximum wind speed in miles per hour
- **MaxGustSpeedMPH:** peak guest wind speed in miles per hour
- **MaxTemperatureF:** maximum temperature, in Fahrenheit
- **MinTemperatureF:** minimum temperature, in Fahrenheit
- **PreciptitationInches:** total precipitation, in inches


A Station may fail to report or compute fields for every observation, so some fields are null for some observations. Records in the file are not guaranteed to be ordered by `ObservationDate`.

# Source and License

This data was built from data sets publicly available on [the NOAA (National Oceanic and Atmospheric Administration) website](https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc%3AC00516/html#), and publicly available from [the NOAA data catalog](https://data.noaa.gov/dataset/dataset/global-surface-summary-of-the-day-gsod). It is provided here for demonstrative use without any warranty as to the accuracy, reliability, or completeness of the data.

