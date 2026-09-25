---
title: Extract data with a Java client
---

This guide will show you how to connect to a Deephaven instance with a Java client, and use it to extract data and tables.

While Deephaven Community Core provides a rich set of features directly through its Web IDE, it also excels at delivering the value of that analysis to other external systems.

We can easily imagine Deephaven providing market analysis data to an external trading engine, or analysed sensor data to a powerplant control system.

We will show you how to construct a simple system that: monitors weather data (sourced from [NOAA](https://www.weather.gov/documentation/services-web-api)), performs some analysis, provides the results to a Java application that controls what cities are monitored, and displays results, all with live ticking data.

![Multiple weather data tables displayed in the Deephaven IDE](../assets/tutorials/java-client/overview_dh.png)

## Prerequisites

- Clone the [Deephaven Community repository](https://github.com/deephaven/deephaven-core).
- [Build and run Deephaven](../getting-started/docker-install.md).
- Create a valid [Google Geolocation API key](https://developers.google.com/maps/documentation/geolocation/overview).

> [!NOTE]
> This guide demonstrates connecting a Java client to a Deephaven Python server. Server-side Python code is provided throughout if you want to follow along.

## The application

The application is broken up into three major parts:

- The first part creates the Deephaven table where weather data will be recorded, and then creates several derived tables that will analyze the data to produce some simple statistics. Each of these tables will be visited in more detail in the following sections.
- The second part is a handful of simple Python methods that will extract position and weather data from the Google Geolocation API and NOAA's weather data API and record it. It also provides a method for the external java application to add more cities to be monitored.
- The final part is a very simple Java application that connects to the Deephaven worker and pulls the ticking results to be displayed. It also provides some simple functionality to add more cities to be monitored, demonstrating that complex Client/Server applications can be built directly with Deephaven.

## Running the Python weather server

At this point, you should have a running Deephaven IDE. You first need to configure and run the Python Weather Server script. This is broken down into five steps.

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
from deephaven.time import to_j_instant

os.system("pip install requests")
import requests
```

> [!NOTE]
> Note that the 'requests' library, which is used to do simple GET calls, is not part of the default Deephaven Python image, so it is imported directly into the running session. See [How to install Python packages](/core/docs/how-to-guides/install-and-use-python-packages) for more details.

2. Create the [DynamicTableWriter](../how-to-guides/table-publisher.md#dynamictablewriter) that will be used to ingest ticking data.

> [!TIP]
> For streaming data directly from a Java client, you can also use [input tables](./java-client-input-tables.md), which allow the client to upload data without running code on the server.

```python skip-test
# First, let's create a table to manage the relevant cities to monitor
dynamic_table_writer_columns = {
    "Timestamp": dht.Instant,
    "State": dht.string,
    "City": dht.string,
    "Temp": dht.float64,
    "Humidity": dht.float64,
}
table_writer = DynamicTableWriter(dynamic_table_writer_columns)

current_data = table_writer.table
```

This simply creates a table with five columns to which the application will record weather measurements.

| Column    | Type    |
| --------- | ------- |
| Timestamp | Instant |
| State     | String  |
| City      | String  |
| Temp      | double  |
| Humidity  | double  |

3. Perform some simple data analysis.

```python skip-test
from deephaven import agg

# Bin the data by 30 minute and 1 hour intervals using the "lowerBin" feature
binned_data = current_data.update_view(
    formulas=[
        "bin30M=lowerBin(Timestamp, 30 * MINUTE)",
        "bin1Hr=lowerBin(Timestamp, 1 *HOUR)",
    ]
)

# Compute Min/Max/Average for Temperature and Humidity,
# grouping the data first, by the State, City, and 30 minute time bin of each measurement
# This also pulls forward the 1 hour time bin value column using the "First" aggregation
agg_list30 = [
    agg.min_(cols=["Min30Temp=Temp", "Min30Humid=Humidity"]),
    agg.max_(cols=["Max30Temp=Temp", "Max30Humid=Humidity"]),
    agg.avg(cols=["Avg30Temp=Temp", "Avg30Humid=Humidity"]),
    agg.first(cols=["bin1Hr"]),
]

binned_stats30 = binned_data.agg_by(agg_list30, by=["State", "City", "bin30M"])

# Next, Compute Min/Max/Average, grouping, instead, by the State, City, and hourly time bins
agg_list60 = [
    agg.min_(cols=["Min60Temp=Temp", "Min60Humid=Humidity"]),
    agg.max_(cols=["Max60Temp=Temp", "Max60Humid=Humidity"]),
    agg.avg(cols=["Avg60Temp=Temp", "Avg60Humid=Humidity"]),
]

binned_stats60 = binned_data.agg_by(agg_list60, by=["State", "City", "bin1Hr"])

# Ideally,  these would be viewed in a single aggregated table
# this will join the two tables together using the State, City, and Hourly time bin
combined_stats = binned_stats30.natural_join(binned_stats60, "State,City,bin1Hr")
# Finally, create a table of the last relevant value of each location, by City and State,
# discarding the time bin columns
# For bonus points, it also joins back on the current weather measurement
# for each location to produce a table that contains
# the min/max/average temperature and humidity for the most recent 30 minutes, and 1 hour,
# as well as the current values.
last_city_by_state = (
    combined_stats.last_by(by=["State", "City"])
    .drop_columns(cols=["bin30M", "bin1Hr"])
    .natural_join(
        table=current_data.last_by(by=["State", "City"]), on=["State", "City"]
    )
    .move_columns_up(cols=["Timestamp", "State", "City", "Temp", "Humidity"])
)
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
        raise ValueError(cityName + " is not a valid place -> " + geoResp.text)
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

    if not localCity or not localState:
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
            to_j_instant(time),
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
        with CITY_LOCK:
            cities = list(trackedCities)
        for lc in cities:
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

The final, and most interesting, part of this example is the Java client. It connects to the Deephaven server, subscribes to the `last_city_by_state` statistics table, and displays it. It also communicates with the server to request that additional cities be tracked.

The repository doesn't include a complete client application for this example. The screenshots below illustrate what a simple Java Swing client built from the API pieces in this guide might look like.

![The Java client UI](../assets/tutorials/java-client/java-app.png)

After a city is added, such a UI shows live weather data for it:

![The Java client UI, now displaying live weather data for the city the user entered](../assets/tutorials/java-client/java-app-data.png)

The sections below walk through the pieces of the Deephaven Java client that you use to build such an application. Complete, runnable Barrage client examples are in the repository's `java-client/barrage-examples` directory — for example, `SubscribeTable`.

## How it works

The Deephaven Java client provides two major features to users:

- It provides a lightweight API that fetches [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) data streams for users to work with. If you're familiar with Arrow Flight, and this is the format you want, then you can use this directly.
- It also provides the ability to transform the Arrow data stream directly into a local, ticking, Deephaven table using the Barrage protocol. Many users will want to use the power of Deephaven tables directly.

> [!NOTE]
> The Java client code makes extensive use of the `Builder` pattern to make object construction more expressive and natural.

### Create a Barrage session

A `BarrageSession` is the client's connection to the Deephaven server. It extends `FlightSession`, so it provides both the Arrow Flight API and the ability to subscribe to tables as local, ticking Deephaven tables. Create one from a `BarrageSessionFactoryConfig`:

```java
final BufferAllocator allocator = new RootAllocator();
final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

final BarrageSessionFactoryConfig.Factory factory = BarrageSessionFactoryConfig.builder()
        .clientConfig(ClientConfig.builder()
                .target(DeephavenTarget.of(URI.create("dh+plain://localhost:10000")))
                .build())
        .allocator(allocator)
        .scheduler(scheduler)
        .build()
        .factory();

final BarrageSession session = factory.newBarrageSession();
```

Use the `dh+plain` scheme for a plaintext connection and `dh` for a TLS connection. If the server requires authentication, pass a `SessionConfig` to `newBarrageSession` — for example, `SessionConfig.builder().authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler <key>").build()` for [pre-shared key authentication](./authentication/auth-psk.md).

Closing the session does not close the underlying channel. When the application is done, close the session, call `factory.managedChannel().shutdown()`, shut down the scheduler, and close the `RootAllocator`.

### Prepare the client-side engine

Subscribed tables are real Deephaven tables, so the client needs a Deephaven update graph and an execution context before it subscribes:

```java
final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("DEFAULT").existingOrBuild();

final ExecutionContext executionContext = ExecutionContext.newBuilder()
        .markSystemic()
        .emptyQueryScope()
        .newQueryLibrary()
        .setUpdateGraph(updateGraph)
        .build();

try (final SafeCloseable ignored = executionContext.open()) {
    // subscribe to tables here
}
```

### Subscribe to a table

> [!NOTE]
> This is where the magic happens! In step 3, the server script defined a query-scope variable named `last_city_by_state`. `TicketTable.fromQueryScopeField` references any table in the server's query scope by name.

```java
final BarrageSubscription subscription = session.subscribe(
        TicketTable.fromQueryScopeField("last_city_by_state"),
        BarrageSubscriptionOptions.builder().build());

final Table statsTable = subscription.entireTable().get();
```

`entireTable` returns a `Future` that completes once all rows of the table have arrived. At that point, `statsTable` is a live, ticking, local instance of the table created in the server script. This application simply displays the contents of that table, but a more sophisticated app could use this table data in any way it likes.

`BarrageSubscription` also supports narrower requests:

- `partialTable` takes a position-space `RowSet` viewport and a `BitSet` of columns, and subscribes to only those rows and columns. Pass `null` for `columns` to subscribe to all columns. An overload with a `reverseViewport` argument treats the viewport as offsets from the end of the table.
- `snapshotEntireTable` and `snapshotPartialTable` fetch a static snapshot instead of a ticking table.

To fetch a one-time snapshot without creating a subscription, use the session's `snapshot` method, which returns a `BarrageSnapshot` with `entireTable` and `partialTable` methods.

> [!CAUTION]
> Subscriptions participate in Deephaven's liveness system. The subscription ends, and the server stops sending data, once the subscribed table is no longer live. To control this explicitly, open a `LivenessScope` (for example, with `LivenessScopeStack.open` in a try-with-resources block) before subscribing; closing the scope releases the table, its listeners, and the subscription.

## Bidirectional communication

One of the other exciting features of the Java client is the ability to communicate directly with the server. This communication can be as sophisticated as creating even more specific derived tables using the same natural query language that is so powerful in the Deephaven Web IDE.

This example gives the user the ability to add cities to the set of monitored locations.

```java
try (final ConsoleSession console = session.session().console("python").get()) {
    // Base64-encode the user's text so it can't break out of the Python string literal
    final String encoded = Base64.getEncoder().encodeToString(place.getBytes(StandardCharsets.UTF_8));
    final Changes c = console.executeCode(
            "import base64; beginWatch(base64.b64decode('" + encoded + "').decode('utf-8'))");
    if (c.errorMessage().isPresent()) {
        System.err.println("Error adding " + place + ": " + c.errorMessage().get());
    } else {
        System.out.println("Added " + place);
    }
} catch (ExecutionException | InterruptedException | TimeoutException e) {
    e.printStackTrace();
}
```

This sends a Python command to the server that calls `beginWatch` with the user's text, and checks for an error response. The server returns a `Changes` object, which allows the application to determine what has changed in the server's scope (for example, tables being created or variables being changed).

> [!WARNING]
> Never concatenate raw user input into a script. The example above Base64-encodes the text first; the Base64 alphabet contains no quotes, backslashes, or control characters, so the encoded value can't escape the Python string literal and inject code.

## Listening to Deephaven tables

The last piece of the puzzle is to process the data from the table. You can, of course, directly read the data from the table. You can also "listen" to the table, which allows the application to respond to ticking changes in the table.

```java
statsTable.addUpdateListener(listener = new InstrumentedTableUpdateListenerAdapter(statsTable, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        // Process the update as needed. The TableUpdate describes
        // 1) What rows have been added: upstream.added()
        // 2) What rows have been removed: upstream.removed()
        // 3) What rows have been modified: upstream.modified()
        // 4) What rows have been shifted in row key space, without changes to column data: upstream.shifted()
        // 5) What columns were modified: upstream.modifiedColumnSet()
    }
});
```

Keep a reference to the listener (here, the `listener` field) for as long as it should receive updates. See [table listeners](./table-listeners-groovy.md) for more on writing listeners.

## Subscribe from a Groovy server

A Groovy script running on one Deephaven server can subscribe to a table on another server the same way. The server already has an update graph and execution context, and it can create the `BarrageSession` for you with its own allocator, scheduler, and channel settings:

```groovy skip-test
import io.deephaven.client.impl.ClientConfig
import io.deephaven.client.impl.SessionConfig
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions
import io.deephaven.qst.table.TicketTable
import io.deephaven.server.runner.DeephavenApiServer
import io.deephaven.uri.DeephavenTarget

config = ClientConfig.builder()
        .target(DeephavenTarget.of(URI.create("dh+plain://remote-host:10000")))
        .build()

barrageSession = DeephavenApiServer.getInstance()
        .sessionFactoryCreator()
        .barrageFactory(config)
        .newBarrageSession(SessionConfig.builder()
                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler <key>")
                .build())

statsTable = barrageSession.subscribe(
        TicketTable.fromQueryScopeField("last_city_by_state"),
        BarrageSubscriptionOptions.builder().build())
        .entireTable()
        .get()
```

> [!NOTE]
> `DeephavenApiServer.getInstance` is annotated `@InternalUseOnly` and may change without notice. This example authenticates to the remote server with a pre-shared key; if the remote server uses anonymous authentication, call `newBarrageSession` with no arguments instead.

For the common case of fetching a remote table by name, [Deephaven URIs](./use-uris.md) are simpler: `ResolveTools.resolve("dh+plain://remote-host:10000/scope/last_city_by_state")` creates the session and subscription for you. However, URI resolution supports only anonymous authentication; if the remote server uses PSK authentication, use the `BarrageSession` approach above.

## Related documentation

- [Java client input tables](./java-client-input-tables.md) - Stream data from a Java client using input tables
- [Arrow Flight](./data-import-export/arrow-flight.md)
- [Servers and clients](../conceptual/client-server-model.md)
