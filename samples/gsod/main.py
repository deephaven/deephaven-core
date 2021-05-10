import os
import requests
import subprocess
import logging

#
# The Global Surface Summary of the Day (GSOD) data summarizes surface weather observations
# since the 1930s at over 11000 stations. This script pulls data  from NOAA websites and gets
# info for about 20 stations that have been continuously active since 1945 or so.
#
# The script packages the data up so that each station has its own CSV file, and adds an
# extra station_data.csv to give prescriptive data about the station.
#
# The NOAA data is described in detail here:
#  https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00516
#
# Each station is identified by a combination of a USAF ID number and a WBAN number.
#
# station_ids, below, enumerates the combined USAF-WBAN number to be retrieved and gives
# that station a "friendly" name, which becomes the name of the output .csv file for the
# station. Some stations change names and numbers, but represent the same data; La Guardia
# Airport, for example, switched numbers with a slight change in naming ... but is still
# LGA observations. TThe two USAF-WBAN keys below are both tagged with the 'nyc' friendly
# name, and therefore contribute to the same 'nyc' file so that coverage is cumulative.
#
# A first run of the script will retrieve all the station data to a cache directory.
# Once the cache is populated, individual files are unzipped from the downloaded archives,
# parsed and formatted, and then emitted into the CSV file.
#
# This script has only been tested on Windows. Moving to another OS probably means picking
# good path names for the configuration below; and making sure that 7zip (or another
# archiver) is installed and working for that OS.
#
# Some information about station numbers is here:
#   https://www.ncdc.noaa.gov/datan-access/land-based-station-data/station-metadata
#
# More notes about GSOD data:
#   https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/
#   https://data.noaa.gov/dataset/dataset/global-surface-summary-of-the-day-gsod
#   https://www.kaggle.com/noaa/gsod
#


# config -- where does data come from and go?
# these directories must be created before running
cache_dir = 'f:\\deephaven\\gsod_cache\\'
temp_dir = 'f:\\deephaven\\temp\\'
output_dir = 'f:\\deephaven\\gsod\\'


# url_root = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/'
url_root = 'https://www1.ncdc.noaa.gov/pub/data/gsod/'
station_history_url = 'https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'

# list of station IDs we'll work
# includes the StationID and WBAN concatenated, plus our own "friendly name".
# Note that a friendly name might have more than one StationID-WBAN in order
# to combine continuous reporting over dates when the station was available.
#
# Some choices seem a bit goofy; why Temosachic and not Mexico City or Monterrey?
# Turns out some "large" locations surprisingly don't have contiuous data available,
# so an obscure nearby location is chosen for coverage rather than familiarity.
station_ids = [
    [ '727930-24233', 'seattle' ] ,
    [ '037720-99999', 'heathrow' ],
    [ '999999-14732', 'nyc' ],
    [ '725030-14732', 'nyc' ],
    [ '162430-99999', 'latina' ],           # in italy
    [ '071470-99999', 'velizy' ],           # france, SW of paris
    [ '762200-99999', 'temosachic' ],       # mexico
    [ '947670-99999', 'sydney' ],
    [ '956770-99999', 'adelaide' ],
    [ '476710-99999', 'tokyo' ],
    [ '688160-99999', 'capetown' ],
    [ '417640-99999', 'hyderabad' ],
    [ '260630-99999', 'pulkovo' ],          # russia, St Petersburg
    [ '411500-99999', 'bahrain' ],
    [ '085150-99999', 'santamaria' ],       # airport on ilha de santa maria
    [ '862970-99999', 'encarnacion' ],      # southern paraguay
    [ '871290-99999', 'santiago' ],
    [ '846580-99999', 'padre_aldamiz' ]
    [ '071500-99999', 'le_bourget' ],       # france, NE of Paris
    [ '107380-99999', 'stuttgart', ],       # central Germany
]

# dictionary of friendly names to lists of CSV lines
station_data_dict = dict()


# get the name of a cached archive file (including path)
def compute_cache_file_name(year, station_id):
    return f"{cache_dir}{year}-{station_id}.op.gz"

# get the name of a temp file (including path)
def compute_temp_file_name(year, station_id):
    return f"{temp_dir}{station_id}-{year}.op"

# get a file; if it exists in cache, all done and return 1
# if it doesn't exist, get it and return 2
# if that fails, return 0
def populate_file(year, station_id):
    file_name = compute_cache_file_name(year, station_id)

    if os.path.exists(file_name):
        return 1, None

    try:
        url = f"{url_root}{year}/{station_id}-{year}.op.gz"
        req = requests.get(url, allow_redirects=True)

        # if it's not application/gzip, it must be an error message
        if req.headers.get('content-type') != 'application/gzip':
            return 0, f"{url}: bad content-type"

        open(file_name, 'wb').write(req.content)
        return 2, None
    except FileNotFoundError:
        return 0, f"{url} not found"


# unzips the zip_file into the temp_dir
def unzip_file(zip_file):
    cmd = ['7z', 'e', '-y', f"-o{temp_dir}", zip_file]
    proc = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    if proc.returncode != 0:
        print(f"return code {proc.returncode}\n{stdout}")
    return proc.returncode


# convert the string to a float
# but return empty string '' if values is an all-nines placeholder
def float_or_empty3(str):
    x = float(str)
    ret = x if x != 999.9 else ''
    return ret

def float_or_empty4(str):
    x = float(str)
    ret = x if x != 9999.9 else ''
    return ret


# get CSV lines from the input *.op file
def read_file_to_csv(file_name):
    input_file = open(file_name, "r")
    lines = input_file.readlines()

    # discard the header line
    lines.pop(0)
    csv_lines = []

    for i in range(0, len(lines)):
        stn = lines[i][0:6]
        wban = lines[i][7:12]
        date = lines[i][14:22]
        temp = float_or_empty3(lines[i][25:30])
        dew_point = float_or_empty3(lines[i][36:41])
        sea_level_pressure = float_or_empty4(lines[i][46:52])
        station_pressure = float_or_empty4(lines[i][57:63])
        visibility = float_or_empty3(lines[i][68:73])
        mean_wind_speed = float_or_empty3(lines[i][78:83])
        max_wind_speed = float_or_empty3(lines[i][88:93])
        max_gust_speed = float_or_empty3(lines[i][95:100])
        max_tmp = float_or_empty3(lines[i][103:108])
        min_tmp = float_or_empty3(lines[i][111:116])
        precip = float_or_empty3(lines[i][118:123])

        csv_line = (f"{stn},{wban},{date},{temp},{dew_point},{sea_level_pressure},{station_pressure},"
                    f"{visibility},{mean_wind_speed},{max_wind_speed},{max_gust_speed},{max_tmp},{min_tmp},{precip}\n")

        csv_lines.append(csv_line)

    return csv_lines


# Write a header for the CSV file
def get_csv_header():
    return (
        "USAF,WBAN,ObservationDate,TemperatureF,DewPointF,SeaLevelPressureMB,StationPressureMB,"
        "VisibilityFeet,MeanWindSpeedMPH,MaxWindSpeedMPH,MaxGustSpeedMPH,MaxTemperatureF,"
        "MinTemperatureF,PreciptitationInches\n"
    )


# load up the station history from the URL; returns a list of lines
def get_station_history():
    req = requests.get(station_history_url, allow_redirects=True)

    # if it's not text/csv, it must be an error message
    if req.headers.get('content-type') != 'text/csv':
       return None, f"{url}: bad content-type"

    str = req.content.decode('utf-8')
    return str.split('\n'), None


# write out the station_data.csv file; has a header, plus info about each station
# we'll track. keyed by USAF/WBAN station number.
def write_station_data_file():
    with open(f"{output_dir}station_data.csv", 'w') as station_data_file:

        # get station_data.csv file together
        station_histories, msg = get_station_history()
        if station_histories is None:
            print(f"couldn't get station history, aborting")
            exit(1)

        # copy the header
        station_data_file.write(station_histories[0])
        station_data_file.write('\n')

        # TODO: O(n^2), but not too horrible at runtime just yet ...
        for j in range(1, len(station_histories)):
            station_history = station_histories[j].split(',')
            if len(station_history) != 11:
                continue

            station = station_history[0].strip('\"')
            WBAN = station_history[1].strip('\"')
            match = f"{station}-{WBAN}"

            for k in range(len(station_ids)):
                if match == station_ids[k][0]:
                    station_data_file.write(station_histories[j])
                    station_data_file.write('\n')


###
#TODO: make a main(), factor to functions

# turn on debug logging for request package; useful if requests are going bad
# logging.basicConfig(level=logging.DEBUG)

write_station_data_file()

# make sure we have all the data we want locally
#TODO: could check the station data info to see if year is in range for that station,
# rather than probing for a file that won't exist.
for year in range(1940, 2022):
# for year in range(1990, 1999):
    for station_info in station_ids:
        print(f"data for {year} for station {station_info[1]} ({station_info[0]}) ", end='')
        x, msg = populate_file(year, station_info[0])
        if x == 0:
            print(f"not found\n\t{msg}")
        elif x == 1:
            print(f"already in cache")
        elif x == 2:
            print(f"downloaded")

# with all the data built, unpack it and accumulate each file
for station_info in station_ids:
    station_lines = []
    for year in range(1990, 1999):

        zip_file_name = compute_cache_file_name(year, station_info[0])
        data_file_name = compute_temp_file_name(year, station_info[0])
        if not os.path.exists(zip_file_name):
            continue

        ret = unzip_file(zip_file_name)
        if ret != 0:
            print(f"failed to unpack file, aborting")
            exit(1)

        # read it, convert to CSV and append to our total
        station_lines.extend(read_file_to_csv(data_file_name))

        os.remove(data_file_name)

    print(f"{station_info[0]} has {len(station_lines)} lines")

    if station_info[1] not in station_data_dict:
        station_data_dict[station_info[1]] = []
    station_data_dict.get(station_info[1]).extend(station_lines)

# write all the files out!
for (city, lines) in station_data_dict.items():
    print(f"{city} has {len(lines)} lines")
    with open(f"{output_dir}{city}.csv", 'w') as out_file:
        out_file.write(get_csv_header())
        out_file.writelines(lines)

