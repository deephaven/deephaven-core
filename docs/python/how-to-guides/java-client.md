---
title: Extract data with a Java client
---

This guide will show you how to connect to a Deephaven instance with a Java client, and use it to extract data and tables.

While Deephaven Community Core provides a rich set of features directly through its Web IDE, it also excels at delivering the value of that analysis to other external systems.

We can easily imagine Deephaven providing market analysis data to an external trading engine, or analysed sensor data to a powerplant control system.

We will show you how to construct a simple system that: monitors weather data (sourced from [NOAA](https://www.weather.gov/documentation/services-web-api)), performs some analysis, provides the results to a Java application that controls what cities are monitored, and displays results, all with live ticking data.

![Multiple weather data tables displayed in the Deephaven IDE](../assets/tutorials/java-client/overview_dh.png)

## Prerequisites

- Clone the [Deephaven Community repository](https://github.com/deephaven/deephaven-core)
- [Build and run Deephaven](../getting-started/docker-install.md)
- Create a valid [Google Geolocation API key](https://developers.google.com/maps/documentation/geolocation/overview)

## The application

The application is broken up into three major parts:

- The first part creates the Deephaven table where weather data will be recorded, and then creates several derived tables that will analyze the data to produce some simple statistics. Each of these tables will be visited in more detail in the following sections.
- The second part is a handful of simple Python methods that will extract position and weather data from the Google Geolocation API and NOAA's weather data API and record it. It also provides a method for the external java application to add more cities to be monitored.
- The final part is a very simple Java application that connects to the Deephaven worker and pulls the ticking results to be displayed. It also provides some simple functionality to add more cities to be monitored, demonstrating that complex Client/Server applications can be built directly with Deephaven.

## Running the Python weather server

At this point, you should have a running Deephaven IDE. You first need to configure and run the Python Weather Server script. This is broken down into three major segments.

1. Import the libraries needed into the worker.

```python skip-test
import random
import time
import json
import os

from dataclasses import dataclass
from datetime import datetime
from math import cos, asin, sqrt, pi
from threading import Lock
from threading import Thread

from deephaven import *
from deephaven import DynamicTableWriter, dtypes as dht
from deephaven import agg
from deephaven.time import epoch_millis_to_instant

os.system("pip install requests")
import requests
```

> [!NOTE]
> Note that the 'requests' library, which is used to do simple GET calls, is not part of the default Deephaven Python image, so it is imported directly into the running session. See [How to install Python packages](../how-to-guides/install-and-use-python-packages.md) for more details.

2. Create the [DynamicTableWriter](../how-to-guides/table-publisher.md#dynamictablewriter) that will be used to ingest ticking data.

```python skip-test
# First, let's create a table to manage the relevant cities to monitor
dynamic_table_writer_columns = {
    "Timestamp": dht.DateTime,
    "State": dht.string,
    "City": dht.string,
    "Temp": dht.float64,
    "Humidity": dht.float64,
}
table_writer = DynamicTableWriter(dynamic_table_writer_columns)

current_data = table_writer.table
```

This simply creates a table with five columns to which the application will record weather measurements.

| Column    | Type     |
| --------- | -------- |
| Timestamp | DateTime |
| State     | String   |
| City      | String   |
| Temp      | double   |
| Humidity  | double   |

3. Perform some simple data analysis.

```python skip-test
from deephaven import agg

# Bin the data by 30 minute and 1 hour intervals using the "lowerBin" feature
binned_data = current_data.update_view(formulas=["bin30M=lowerBin(Timestamp, 30 * MINUTE)", "bin1Hr=lowerBin(Timestamp, 1 *HOUR)"])

# Compute Min/Max/Average for Temperature and Humidity,
# grouping the data first, by the State, City, and 30 minute time bin of each measurement
# This also pulls forward the 1 hour time bin value column using the "First" aggregation
agg_list30 = [
    agg.min_(cols=["Min30Temp=Temp", "Min30Humid=Humidity"]),
    agg.max_(cols=["Max30Temp=Temp", "Max30Humid=Humidity"]),
    agg.avg(cols=["Avg30Temp=Temp", "Avg30Humid=Humidity"]),
    agg.first(cols=["bin1Hr"])
]

binned_stats30 = binned_data.agg_by(agg_list30, by=["State", "City", "bin30M"])

# Next, Compute Min/Max/Average, grouping, instead, by the State, City, and hourly time bins
agg_list60 = [
    agg.min_(cols=["Min60Temp=Temp", "Min60Humid=Humidity"]),
    agg.max_(cols=["Max60Temp=Temp", "Max60Humid=Humidity"]),
    agg.avg(cols=["Avg60Temp=Temp", "Avg60Humid=Humidity"])
]

binned_stats60 = binned_data.agg_by(agg_list60, by=["State", "City", "bin1Hr"])

# Ideally,  these would be viewed in a single aggregated table
# this will join the two tables together using the State, City, and Hourly time bin
combined_stats = binned_stats30.natural_join(binned_stats60, "State,City,bin1Hr");

# Finally, create a table of the last relevant value of each location, by City and State,
# discarding the time bin columns
# For bonus points, it also joins back on the current weather measurement
# for each location to produce a table that contains
# the min/max/average temperature and humidity for the most recent 30 minutes, and 1 hour,
# as well as the current values.
last_city_by_state = combined_stats.last_by(by=["State", "City"])\
    .drop_columns(cols=["bin30M", "bin1Hr"])\
    .natural_join(table=current_data.last_by(by=["State", on=["City"])], joins=["State,City"])\
    .move_columns_up(cols=["Timestamp", "State", "City", "Temp", "Humidity"])
```

4. Create a data structure for collecting information on each location to use to fetch weather data.

   > [!NOTE]
   > In the code below, be sure to insert your Google Geolocation API key into the `API_KEY` variable.

```python skip-test
@dataclass
class Location:
    city: str = None
    state: str = None
    lat: float = -999
    lon: float = -999

    last_obs_time: datetime = None

    observationStation: str = None
    forecastStation: str = None

    def __hash__(self):
        return hash((self.city, self.state))


API_KEY = "YOUR_API_KEY"
CITY_LOCK = Lock()
trackedCities = set()
```

5. Add the code that fetches weather data.

<details>
    <summary>`FetchWeatherData.py`</summary>

```python skip-test
# Borrowed from https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
def distance(lat1, lon1, lat2, lon2):
    p = pi / 180
    a = (
        0.5
        - cos((lat2 - lat1) * p) / 2
        + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    )
    return 12742 * asin(sqrt(a))  # 2*R*asin...


# Locate the Lat/Lon of a particular city state
def geoLocate(cityName) -> Location:
    if len(cityName) <= 0:
        raise ValueError("Place name must be present")

    # First go figure out Lat/Lon so we can pass that to NOAA and locate a station
    geoResp = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": cityName, "key": API_KEY},
    )
    if geoResp.status_code != 200:
        raise ValueError(
            cityName + " is not a valid place -> " + geoJson["error_message"]
        )
    geoJson = geoResp.json()

    # Process the response JSON and look for the City and State (typically locality and administrative_area_level_1)
    localCity = localState = ""
    resultsEl = geoJson["results"][0]
    if resultsEl is None:
        raise ValueError(
            "Cannot determine location of " + cityName + " no valid results"
        )

    comps = resultsEl["address_components"]
    if comps is None:
        raise ValueError("Cannot determine location of " + cityName + " no components")

    for val in comps:
        if "locality" in val["types"]:
            localCity = val["long_name"]
        if "administrative_area_level_1" in val["types"]:
            localState = val["long_name"]

    if localCity is None or localState is None:
        raise ValueError("Unable to determine city and state for " + cityName)

    geom = resultsEl["geometry"]
    if geom is None:
        raise ValueError("Unable to determine lat/lon for " + cityName)

    loc = geom["location"]
    if loc is None:
        raise ValueError("Unable to determine lat/lon for " + cityName)

    print(
        "Located "
        + localState
        + ", "
        + localCity
        + " at ["
        + str(loc["lat"])
        + ", "
        + str(loc["lng"])
        + "]"
    )
    return Location(city=localCity, state=localState, lat=loc["lat"], lon=loc["lng"])


# We have to poke NOAA's APIs so that we can find
# 1) the "Point" 2) the Observation station and 3) the Forecast station.
# Once we collect this data, we can construct an appropriate API call
# to fetch current / forecasted weather data.
def discoverWeather(loc):
    # First, convert the Lat/lon into a point
    pointJson = requests.get(
        "https://api.weather.gov/points/" + str(loc.lat) + "%2C" + str(loc.lon)
    ).json()

    # Next, use that point to locate the Grid location, which tells us the station URLs
    loc.forecastStation = pointJson["properties"]["forecast"]
    observationUrl = pointJson["properties"]["observationStations"]

    # Finally, locate the station to use for observations and build a URL we can just call
    stationJson = requests.get(observationUrl).json()

    # Sift through the set of features and locate the closest one as the crow flies
    # and use that as the observation station.
    closestFeature = None
    closestDistance = 99999999999
    for feature in stationJson["features"]:
        featurePoint = feature["geometry"]["coordinates"]
        # Note that NOAA's coordinates are lon,lat -NOT- lat,lon :(
        dist = distance(loc.lat, loc.lon, featurePoint[1], featurePoint[0])
        # print("Station " + feature['properties']['name'] + " at distance " + str(dist))
        if closestFeature is None or dist < closestDistance:
            closestFeature = feature
            closestDistance = dist

    loc.observationStation = (
        "https://api.weather.gov/stations/"
        + closestFeature["properties"]["stationIdentifier"]
        + "/observations/latest"
    )


def updateObservation(lc):
    obs_json = requests.get(lc.observationStation).json()

    # TODO:  Actual error checking
    time = datetime.fromisoformat(obs_json["properties"]["timestamp"])

    if lc.last_obs_time is None or lc.last_obs_time < time:
        lc.last_obs_time = time
        temp = obs_json["properties"]["temperature"]["value"]
        humid = obs_json["properties"]["relativeHumidity"]["value"]
        print("Updated " + str(lc) + " at " + str(time))
        table_writer.write_row(
            epoch_millis_to_instant((int)(time.timestamp() * 1000)),
            lc.state,
            lc.city,
            temp,
            humid,
        )


# A simple method to add a city to the set of cities to watch
def beginWatch(cityName):
    loc = geoLocate(cityName)

    # Don't do anything further if we're already watching this location
    if loc in trackedCities:
        return

    discoverWeather(loc)
    if loc.observationStation is None:
        raise ValueError("Could not locate observation URL for " + cityName)

    with CITY_LOCK:
        trackedCities.add(loc)
        updateObservation(loc)


def periodicFetchRealData():
    while True:
        for lc in trackedCities:
            updateObservation(lc)
        time.sleep(60)


monitorThread = Thread(target=periodicFetchRealData)
monitorThread.start()
```

</details>

The Python code above is compressed for brevity, but essentially contains four methods:

- A method to turn a Location name into a position using Google Geolocation.
- A method to discover the NOAA Weather API Endpoints for the position.
- A method to fetch the current weather observation for the position.
- A thread that fetches the current weather for all requested cities every 1 minute.

At this point, you have a completely functional Deephaven application that is ready to start providing data to another client.

## The Java client

The final, and most interesting part of this example, is the Java client. It is an extremely simple Java Swing UI that has the ability to connect to the Deephaven worker and fetch the final `LastByCityState` statistics table. It also demonstrates the ability to communicate with the worker by requesting additional cities to be tracked.

The client code can be found in the repository in the `java-client/weather-server-example` directory. Build this and run the `WeatherDash` class to bring up the UI.

![The Java client UI](../assets/tutorials/java-client/java-app.png)

Enter the address of your Deephaven IDE and click **Connect**. Then type an address in the **Location** text box and click **Add**.

You will now see live weather data for the city you entered.

![The Java client UI, now displaying live weather data for the city the user entered](../assets/tutorials/java-client/java-app-data.png)

## How it works

The Deephaven Java Client provides two major features to users:

- It provides a lightweight API that fetches [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) data streams for users to work with. If you're familiar with Arrow Flight, and this is the format you want, then you can use this directly.
- It also provides the ability to transform the Arrow data stream directly into a local, ticking, Deephaven Table! Many users will want to use the power of Deephaven tables directly.

> [!NOTE]
> The Java Client code makes extensive use of the `Builder` pattern to make object construction more expressive and natural.

The first part is to create a `ManagedChannel`. This is the root connection between your application and the Deephaven worker.

```java
final ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(host);
if (plaintext || "localhost:10000".equals(host)) {
    channelBuilder.usePlaintext();
} else {
    channelBuilder.useTransportSecurity();
}

managedChannel = channelBuilder.build();
```

Once you have a channel to the worker, we need to create a `FlightSession`. This class layers the Deephaven logic on top of the raw channel to the worker.

```java
final BufferAllocator bufferAllocator = new RootAllocator();
final FlightSessionFactory flightSessionFactory =
        DaggerDeephavenFlightRoot.create().factoryBuilder()
                .managedChannel(managedChannel)
                .scheduler(flightScheduler)
                .allocator(bufferAllocator)
                .build();

flightSession = flightSessionFactory.newFlightSession();
```

> [!WARNING]
> The Deephaven Java-API is still an alpha library. In future versions, the method described below to convert the [Arrow Flight](https://arrow.apache.org/) Stream into a Deephaven table will be dramatically simplified.
>
> This example has packaged this code into the class `BarrageSupport` to separate this complexity.

Now, we will create the `BarrageSupport` instance and fetch the `LastByCityState` from the worker.

> [!NOTE]
> This is where the magic happens! In step 3, we defined a query-scope variable named `LastByCityState`. To reference in a remote client, we convert it into a Flight Ticket simply by prefixing `s/`. Any tables that are exposed in the query scope can be accessed as easily as that.

```java
support = new BarrageSupport(managedChannel, flightSession);
statsTable = support.fetchSubscribedTable("s/LastByCityState");
```

At this point, the variable `statsTable` is now a live, ticking, local instance of the table created in the Python script. This application simply displays the contents of that table directly, but a more sophisticated app could use this table data in any way it likes.

## Bidirectional communication

One of the other exciting features of the Java client is the ability to communicate directly with the worker. This communication can be as sophisticated as creating even more specific derived tables using the same natural query language that is so powerful in the Deephaven Web IDE.

This example gives the user the ability to add cities to the set of monitored locations.

```java
try (final ConsoleSession console = flightSession.session().console("python").get()) {
    final Changes c = console.executeCode("beginWatch(\"" + escapeString(place) + "\")");
    if (c.errorMessage().isPresent()) {
        log.error().append("Error adding " + place + ": " + c.errorMessage().get())
    } else {
        log.info().append("Added " + place);
    }
} catch (ExecutionException | InterruptedException | TimeoutException e) {
    e.printStackTrace();
}
```

This simply sends a Python command to the server (`beginWatch(place)`) and checks for an error response. The server returns a `Changes` object, which allows the application to determine what has changed in the scope of the worker (for example, tables being created or variables being changed).

> [!WARNING]
> This example embeds a user string into a command. Be extremely careful to escape quotes and other special characters within the string to prevent script injection attacks.

## Listening to Deephaven tables

The last piece of the puzzle is to process the data from the table. You can, of course, directly read the data from the table. You can also "listen" to the table, which allows the application to respond to ticking changes in the table.

```java
table.addUpdateListener(listener = new InstrumentedShiftAwareListenerAdapter(table, false) {
    @Override
    public void onUpdate(Update upstream) {
        // Process the update as needed,  the Update class contains Index instances that describe
        // 1) What rows have been added
        // 2) What rows have been removed
        // 3) What rows have been modified
        // 4) What rows have been structurally shifed in address space,  but without changes to column data
        // 5) What columns were affected by the changes
    }
});
```

## Table subscription deep dive

As mentioned above, creating a subscribed, local Deephaven table will be easier in upcoming releases. However, below we discuss briefly what the example does.

First, the application needs to "export" a table from the server. This is done by requesting a `TableHandle` from the server, and then retrieving the `Ticket` object from it. The `Ticket` can be thought of simply as a unique identifier for a particular table stream.

```java
final TableHandle handle = session.session().ticket(tableName);
final Export tableExport = handle.export();
final Ticket tableTicket = tableExport.ticket();
```

Next, it must fetch the Schema of the Flight Stream. This tells the Deephaven libraries how to interpret the bytes from the Flight stream and convert them into Rows and Columns of a table. From this schema, this creates a `TableDefinition` and finally a `BarrageTable`, which is the core of the client side Deephaven table implementation.

```java
final Schema schema = session.getSchema(tableExport);
final TableDefinition definition = BarrageSchemaUtil.schemaToTableDefinition(schema);

final BitSet columnsToSubscribe = new BitSet();
columnsToSubscribe.set(0, definition.getColumns().length);

final BarrageTable resultTable = BarrageTable.make(definition, false);
```

Finally, we create a subscription to the server, which effectively tells the server what rows and columns the application requires, and starts the stream of data from the server.

```java
final BarrageClientSubscription resultSub = new BarrageClientSubscription(
    ExportTicketHelper.toReadableString(tableTicket, "exportTable"),
    channel, BarrageClientSubscription.makeRequest(tableTicket, null, columnsToSubscribe),
    new BarrageStreamReader(), resultTable);
```

With that final step, the Deephaven Table is subscribed, and will receive live updates from the server as data changes. Client code can then further listen to that table to handle changes as required.

> [!CAUTION]
> When your app is done using tables that were fetched, be sure to call `resultSub.close()` to release any resources that are being held. Without this, the server believes the table is still active and will continue to consume memory and CPU time.
