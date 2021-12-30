from deephaven.TableTools import *
from deephaven import DynamicTableWriter, Types as dht
from deephaven import ComboAggregateFactory as caf
from deephaven import DBTimeUtils
import time
from threading import Lock
from threading import Thread
import random
import time
import json
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from math import cos, asin, sqrt, pi
import os

os.system("pip install requests")
import requests

@dataclass
class Location:
    city: str = None
    state: str = None
    lat: float = -999
    lon: float = -999

    lastObsTime: datetime = None

    observationStation: str = None
    forecastStation: str = None

    def __hash__(self):
        return hash((self.city, self.state))

API_KEY = 'YOUR_KEY_HERE'
CITY_LOCK = Lock()
trackedCities = set()

# First, let's create a table to manage the relevant cities to monitor
columnNames = ["Timestamp", "State", "City", "Temp", "Humidity"]
columnTypes = [dht.datetime, dht.string, dht.string, dht.float64, dht.float64]
tableWriter = DynamicTableWriter(columnNames, columnTypes)

CurrentData = tableWriter.getTable()

# Bin the data by 30 minute and 1 hour intervals
binnedData = CurrentData.updateView("bin30M=lowerBin(Timestamp, 30 * MINUTE)", "bin1Hr=lowerBin(Timestamp, 1 *HOUR)")

# Compute some statistics
binnedStats30 = binnedData.by(caf.AggCombo( \
    caf.AggMin("Min30Temp=Temp", "Min30Humid=Humidity"), \
    caf.AggMax("Max30Temp=Temp", "Max30Humid=Humidity"), \
    caf.AggAvg("Avg30Temp=Temp", "Avg30Humid=Humidity"), \
    caf.AggFirst("bin1Hr")), "State", "City", "bin30M")

binnedStats60 = binnedData.by(caf.AggCombo( \
    caf.AggMin("Min60Temp=Temp", "Min60Humid=Humidity"), \
    caf.AggMax("Max60Temp=Temp", "Max60Humid=Humidity"), \
    caf.AggAvg("Avg60Temp=Temp", "Avg60Humid=Humidity")), "State", "City", "bin1Hr")

combinedStats = binnedStats30.naturalJoin(binnedStats60, "State,City,bin1Hr");

# Now make a table containing the last values of all the relevant intervals for each city.
LastByCityState = combinedStats.lastBy("State", "City") \
    .dropColumns("bin30M", "bin1Hr") \
    .naturalJoin(CurrentData.lastBy("State", "City"), "State,City") \
    .moveUpColumns("Timestamp", "State", "City", "Temp", "Humidity");


# Borrowed from https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
def distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 12742 * asin(sqrt(a)) #2*R*asin...

# Locate the Lat/Lon of a particular city state
def geoLocate(cityName) -> Location:
    if len(cityName) <= 0:
        raise ValueError("Place name must be present")

    # First go figure out Lat/Lon so we can pass that to NOAA and locate a station
    geoResp = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params={'address': cityName, 'key': API_KEY})
    if geoResp.status_code != 200:
        raise ValueError(cityName + " is not a valid place -> " + geoJson['error_message'])
    geoJson = geoResp.json()

    # Process the response JSON and look for the City and state (typically locality and administrative_area_level_1)
    localCity = localState =''
    resultsEl = geoJson['results'][0]
    if resultsEl is None:
        raise ValueError("Cannot determine location of " + cityName + " no valid results")

    comps = resultsEl['address_components']
    if comps is None:
        raise ValueError("Cannot determine location of " + cityName + " no components")

    for val in comps:
        if 'locality' in val['types']:
            localCity = val['long_name']
        if 'administrative_area_level_1' in val['types']:
            localState = val['long_name']

    if localCity is None or localState is None:
        raise ValueError("Unable to determine city and state for " + cityName)

    geom = resultsEl['geometry']
    if geom is None:
        raise ValueError("Unable to determine lat/lon for " + cityName)

    loc = geom['location'];
    if loc is None:
        raise ValueError("Unable to determine lat/lon for " + cityName)

    print("Located " + localState +", " + localCity + " at [" + str(loc['lat']) + ", " + str(loc['lng']) + "]")
    return Location(city=localCity, state=localState, lat=loc['lat'], lon=loc['lng'])

# We have to poke NOAA's APIs so that we can find 1) the "Point" 2) The OBservation station and 3) the Forecast station
# Once we collect this data we can construct an appropriate API call to fetch current / forecasted weather data
def discoverWeather(loc):
    # First, convert the Lat/lon into a point
    pointJson = requests.get("https://api.weather.gov/points/" + str(loc.lat) + "%2C" + str(loc.lon)).json()
    #    print(pointJson)
    #    if pointJson['status'] == 404:
    #        raise ValueError("NOAA Unable to locate point for " + str(loc))

    # Next, use that point to locate the Grid location, which tells us the station URLs
    loc.forecastStation = pointJson['properties']['forecast']
    observationUrl = pointJson['properties']['observationStations']

    # Finally, locate the station to use for observations and build a URL we can just call
    stationJson = requests.get(observationUrl).json()
    #if stationJson['status'] == 404:
    #    raise ValueError("NOAA Unable to locate observation stations for " + str(loc))

    # Sift through the set of features and locate the closest one as the crow flies and use that as the observation
    # station.
    closestFeature = None
    closestDistance = 99999999999
    for feature in stationJson['features']:
        featurePoint = feature['geometry']['coordinates']
        # Note that NOAA's coordinates are lon,lat -NOT- lat,lon :(
        dist = distance(loc.lat, loc.lon, featurePoint[1], featurePoint[0])
        #print("Station " + feature['properties']['name'] + " at distance " + str(dist))
        if closestFeature is None or dist < closestDistance:
            closestFeature = feature
            closestDistance = dist

    #print("Closest station was " + str(closestFeature) + " at distance " + str(closestDistance))
    loc.observationStation = "https://api.weather.gov/stations/" + closestFeature['properties']['stationIdentifier'] + "/observations/latest"

def updateObservation(lc):
    obsJson = requests.get(lc.observationStation).json()

    # TODO:  Actual error checking
    time = datetime.fromisoformat(obsJson['properties']['timestamp'])

    if lc.lastObsTime is None or lc.lastObsTime < time:
        lc.lastObsTime = time
        temp = obsJson['properties']['temperature']['value']
        humid = obsJson['properties']['relativeHumidity']['value']
        print("Updated " + str(lc) + " at " + str(time))
        tableWriter.logRow(DBTimeUtils.millisToTime((int)(time.timestamp()*1000)), lc.state, lc.city, temp, humid)


# A simple method to add a city to the set of cities to watch
def beginWatch(cityName):
    loc = geoLocate(cityName)

    # Don't do anything further if we're already watching this location
    if loc in trackedCities:
        return

    discoverWeather(loc)
    if loc.observationStation is None:
        raise ValueError("Could not locate observation URL for " + cityName)

    with(CITY_LOCK):
        trackedCities.add(loc)
        updateObservation(loc)

def periodicFetchRealData():
    while True:
        for lc in trackedCities:
            updateObservation(lc)
        time.sleep(60)


def makeUpSomeData():
    while True:
        with(CITY_LOCK):
            for lc in trackedCities:
                tableWriter.logRow(DBTimeUtils.currentTime(), lc.state, lc.city, random.uniform(33,97), random.uniform(0,100))
        time.sleep(1)

bobby = Thread(target=periodicFetchRealData)
bobby.start()
