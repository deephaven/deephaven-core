---
title: Extract data with a Java client
---

This guide shows you how to connect to a Deephaven server with a Java client, subscribe to a ticking table (a table whose rows update as new data arrives), and run code on the server from the client. The example monitors live weather data from [NOAA](https://www.weather.gov/documentation/services-web-api) and displays analyzed results in a Java application.

## Prerequisites

- Run a Deephaven server with Python as its scripting language, such as the `ghcr.io/deephaven/server` image. See the [Python Docker install guide](/core/docs/getting-started/docker-install/). The server script and the client's `console("python")` call need a Python server, so the Groovy image, `server-slim`, can't run them.
- Create a valid [Google Geocoding API key](https://developers.google.com/maps/documentation/geocoding/overview).
- Set up a Java project that depends on `io.deephaven:deephaven-java-client-barrage-dagger`, which includes the [Barrage](../conceptual/what-is-barrage.md), [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html), and session client libraries used below.
- Run the client JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`, which Arrow needs on Java 16 and later. With Deephaven 43.0 and later on Java 25 and later, also pass `-Dio.netty.noUnsafe=false`. See [JVM configuration](./java-client-input-tables.md#jvm-configuration).
- Optionally, clone the [Deephaven Community repository](https://github.com/deephaven/deephaven-core) to run the complete client examples described in [The Java client](#the-java-client).

## The application

The application has two parts:

- **The Python weather server**: a Deephaven script that creates a table to record weather data and several derived tables that compute simple statistics. The script also defines Python functions that get position data from the Google Geocoding API and weather data from NOAA's weather API, and then write the weather data to the table. One of these functions lets the Java client add more cities to monitor.
- **The Java client**: a simple Java application that connects to the Deephaven server and displays the ticking results. It can also ask the server to monitor more cities.

The following sections walk through each part. The server script is complete and runnable. The complete Java client isn't published, so this guide shows the parts of its code that connect to the server, subscribe to the table, listen for updates, and run code on the server.

## Run the Python weather server

With Deephaven running, open the Deephaven IDE and run the weather server script. The script has five steps.

1. Import the libraries the script needs.

```python skip-test
import time
import os

from dataclasses import dataclass
from datetime import datetime
from math import cos, asin, sqrt, pi
from threading import Lock
from threading import Thread

from deephaven import DynamicTableWriter, dtypes as dht
from deephaven import agg
from deephaven.time import to_j_instant

os.system("pip install requests")
import requests
```

> [!NOTE]
> The script uses the `requests` library for simple GET calls. The default Deephaven Python image doesn't include `requests`, so the script installs it into the running session. See [How to install Python packages](/core/docs/how-to-guides/install-and-use-python-packages) for more details.

2. Create the [`DynamicTableWriter`](/core/docs/how-to-guides/table-publisher/#dynamictablewriter) that ingests ticking data.

```python skip-test
# Create a table to record weather measurements
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

The [`DynamicTableWriter`](/core/docs/how-to-guides/table-publisher/#dynamictablewriter) creates a table with five columns. The script writes each weather measurement to this table.

| Column    | Type    |
| --------- | ------- |
| Timestamp | Instant |
| State     | String  |
| City      | String  |
| Temp      | double  |
| Humidity  | double  |

3. Perform some simple data analysis. The script bins each measurement into 30-minute and 1-hour intervals with [`update_view`](/core/docs/reference/table-operations/select/update-view/), computes statistics for each bin with [`agg_by`](/core/docs/reference/table-operations/group-and-aggregate/aggBy/), combines the two sets of statistics with [`natural_join`](/core/docs/reference/table-operations/join/natural-join/), and keeps the latest row for each city with [`last_by`](/core/docs/reference/table-operations/group-and-aggregate/lastBy/).

```python skip-test
# Bin the data by 30 minute and 1 hour intervals using the "lowerBin" feature
binned_data = current_data.update_view(
    formulas=[
        "Bin30M = lowerBin(Timestamp, 30 * MINUTE)",
        "Bin1Hr = lowerBin(Timestamp, 1 * HOUR)",
    ]
)

# Compute Min/Max/Average for Temperature and Humidity,
# grouping the data first, by the State, City, and 30 minute time bin of each measurement
# This also pulls forward the 1 hour time bin value column using the "First" aggregation
agg_list30 = [
    agg.min_(cols=["Min30Temp=Temp", "Min30Humid=Humidity"]),
    agg.max_(cols=["Max30Temp=Temp", "Max30Humid=Humidity"]),
    agg.avg(cols=["Avg30Temp=Temp", "Avg30Humid=Humidity"]),
    agg.first(cols=["Bin1Hr"]),
]

binned_stats30 = binned_data.agg_by(agg_list30, by=["State", "City", "Bin30M"])

# Next, compute Min/Max/Average, grouping by the State, City, and hourly time bins
agg_list60 = [
    agg.min_(cols=["Min60Temp=Temp", "Min60Humid=Humidity"]),
    agg.max_(cols=["Max60Temp=Temp", "Max60Humid=Humidity"]),
    agg.avg(cols=["Avg60Temp=Temp", "Avg60Humid=Humidity"]),
]

binned_stats60 = binned_data.agg_by(agg_list60, by=["State", "City", "Bin1Hr"])

# Join the two tables on State, City, and the hourly time bin to view them in one table
combined_stats = binned_stats30.natural_join(
    binned_stats60, on=["State", "City", "Bin1Hr"]
)

# Finally, create a table of the last relevant value of each location, by City and State,
# discarding the time bin columns
# For bonus points, it also joins back on the current weather measurement
# for each location to produce a table that contains
# the min/max/average temperature and humidity for the current 30-minute and 1-hour time bins,
# as well as the current values.
last_city_by_state = (
    combined_stats.last_by(by=["State", "City"])
    .drop_columns(cols=["Bin30M", "Bin1Hr"])
    .natural_join(
        table=current_data.last_by(by=["State", "City"]), on=["State", "City"]
    )
    .move_columns_up(cols=["Timestamp", "State", "City", "Temp", "Humidity"])
)
```

4. Create a `Location` data class that holds what the script needs to fetch weather data for each city.

   > [!NOTE]
   > In the code below, be sure to insert your Google Geocoding API key into the `API_KEY` variable.

```python skip-test
@dataclass
class Location:
    city: str = None
    state: str = None
    lat: float = -999
    lon: float = -999

    last_obs_time: datetime = None

    observation_station: str = None
    forecast_station: str = None

    # A location is identified by its city and state
    def __hash__(self):
        return hash((self.city, self.state))

    def __eq__(self, other):
        return (self.city, self.state) == (other.city, other.state)


API_KEY = "YOUR_API_KEY"
CITY_LOCK = Lock()
tracked_cities = set()
```

5. Add the code that fetches weather data.

<details>
    <summary>Weather data fetching code</summary>

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
def geo_locate(city_name) -> Location:
    if len(city_name) <= 0:
        raise ValueError("Place name must be present")

    # First go figure out Lat/Lon so we can pass that to NOAA and locate a station
    geo_resp = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": city_name, "key": API_KEY},
    )
    geo_json = geo_resp.json()
    status = geo_json.get("status", str(geo_resp.status_code))
    if status != "OK":
        reason = geo_json.get("error_message", status)
        raise ValueError(city_name + " is not a valid place -> " + reason)

    # Process the response JSON and look for the City and State (typically locality and administrative_area_level_1)
    local_city = local_state = ""
    results = geo_json.get("results")
    if not results:
        raise ValueError(
            "Cannot determine location of " + city_name + " no valid results"
        )
    results_el = results[0]

    comps = results_el.get("address_components")
    if not comps:
        raise ValueError("Cannot determine location of " + city_name + " no components")

    for val in comps:
        if "locality" in val["types"]:
            local_city = val["long_name"]
        if "administrative_area_level_1" in val["types"]:
            local_state = val["long_name"]

    if not local_city or not local_state:
        raise ValueError("Unable to determine city and state for " + city_name)

    geom = results_el.get("geometry")
    if geom is None:
        raise ValueError("Unable to determine lat/lon for " + city_name)

    loc = geom.get("location")
    if loc is None:
        raise ValueError("Unable to determine lat/lon for " + city_name)

    print(
        "Located "
        + local_state
        + ", "
        + local_city
        + " at ["
        + str(loc["lat"])
        + ", "
        + str(loc["lng"])
        + "]"
    )
    return Location(city=local_city, state=local_state, lat=loc["lat"], lon=loc["lng"])


# We have to poke NOAA's APIs so that we can find
# 1) the "Point" 2) the Observation station and 3) the Forecast station.
# Once we collect this data, we can construct an appropriate API call
# to fetch current / forecasted weather data.
def discover_weather(loc):
    # First, convert the Lat/lon into a point
    point_json = requests.get(
        "https://api.weather.gov/points/" + str(loc.lat) + "%2C" + str(loc.lon)
    ).json()

    # Next, use that point to locate the Grid location, which tells us the station URLs
    loc.forecast_station = point_json["properties"]["forecast"]
    observation_url = point_json["properties"]["observationStations"]

    # Finally, locate the station to use for observations and build a URL we can just call
    station_json = requests.get(observation_url).json()

    # Sift through the set of features and locate the closest one as the crow flies
    # and use that as the observation station.
    closest_feature = None
    closest_distance = 99999999999
    for feature in station_json["features"]:
        feature_point = feature["geometry"]["coordinates"]
        # Note that NOAA's coordinates are lon,lat -NOT- lat,lon :(
        dist = distance(loc.lat, loc.lon, feature_point[1], feature_point[0])
        # print("Station " + feature['properties']['name'] + " at distance " + str(dist))
        if closest_feature is None or dist < closest_distance:
            closest_feature = feature
            closest_distance = dist

    loc.observation_station = (
        "https://api.weather.gov/stations/"
        + closest_feature["properties"]["stationIdentifier"]
        + "/observations/latest"
    )


def update_observation(lc):
    obs_json = requests.get(lc.observation_station).json()

    obs_time = datetime.fromisoformat(obs_json["properties"]["timestamp"])

    if lc.last_obs_time is None or lc.last_obs_time < obs_time:
        lc.last_obs_time = obs_time
        temp = obs_json["properties"]["temperature"]["value"]
        humid = obs_json["properties"]["relativeHumidity"]["value"]
        print("Updated " + str(lc) + " at " + str(obs_time))
        table_writer.write_row(
            to_j_instant(obs_time),
            lc.state,
            lc.city,
            temp,
            humid,
        )


# A simple function to add a city to the set of cities to watch
def begin_watch(city_name):
    loc = geo_locate(city_name)

    # Don't do anything further if we're already watching this location
    if loc in tracked_cities:
        return

    discover_weather(loc)
    if loc.observation_station is None:
        raise ValueError("Could not locate observation URL for " + city_name)

    with CITY_LOCK:
        tracked_cities.add(loc)
        update_observation(loc)


def periodic_fetch_real_data():
    while True:
        # Copy the set under the lock, because begin_watch can add to it at any time
        with CITY_LOCK:
            cities = list(tracked_cities)
        for lc in cities:
            # Skip a city whose fetch fails this time, so one bad response doesn't stop the thread
            try:
                update_observation(lc)
            except Exception as e:
                print("Failed to update " + str(lc) + ": " + str(e))
        time.sleep(60)


monitor_thread = Thread(target=periodic_fetch_real_data)
monitor_thread.start()
```

</details>

The main functions in the collapsed code above are:

- `geo_locate` turns a place name into a position using the Google Geocoding API.
- `discover_weather` finds the NOAA weather API endpoints for the position.
- `update_observation` fetches the current weather observation for the position and writes it to the table.
- `begin_watch` adds a city to the set of monitored cities. The Java client calls this function.
- `periodic_fetch_real_data` runs on a background thread and fetches the current weather for all monitored cities every minute. If the fetch for a city fails, it prints the error and moves on to the next city.

At this point, the weather server is ready to provide data to the Java client. The script monitors no cities until something calls `begin_watch`. The Java client calls `begin_watch`. You can also call it in the IDE, for example with `begin_watch("Springfield, ME")`.

## The Java client

The Java client is a Swing UI that subscribes to the server's `last_city_by_state` table and calls `begin_watch` to add cities.

![The Java client UI](../assets/tutorials/java-client/java-app.png)

In the finished application, the user enters the server's host and port, such as `localhost:10000`, in the **Host** text box and clicks **Connect**. Next, the user types an address in the **Location** text box and clicks **Add**. The UI then displays live weather data for that city.

![The Java client UI, now displaying live weather data for the city the user entered](../assets/tutorials/java-client/java-app-data.png)

For complete, runnable clients, see the examples in the repository's [`java-client/barrage-examples`](https://github.com/deephaven/deephaven-core/tree/main/java-client/barrage-examples) directory, such as [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java).

The Deephaven Java client can get table data from the server in two ways:

- A lightweight API that fetches [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) data streams, for when you want your data in Arrow Flight format. For examples, see the repository's [`java-client/flight-examples`](https://github.com/deephaven/deephaven-core/tree/main/java-client/flight-examples) directory.
- Table subscriptions and snapshots over the [Barrage](../conceptual/what-is-barrage.md) streaming protocol. A subscription delivers a server table to the client as a local, ticking Deephaven table, and a snapshot delivers it as a local static table.

The [Arrow Flight guide](./data-import-export/arrow-flight.md) lists the Arrow types Deephaven supports.

This guide uses table subscriptions to get data, and a console session to run code on the server.

The snippets in the following sections use these variables from the application:

- `host` is the server's host and port, such as `localhost:10000`.
- `plaintext` is `true` when the server doesn't use TLS.
- `psk` is the server's [pre-shared key (PSK)](./authentication/auth-psk.md), which the server prints in its startup log.
- `flightScheduler` is a [`ScheduledExecutorService`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ScheduledExecutorService.html), such as one created with `Executors.newScheduledThreadPool(4)`. It runs the client's background work, including refreshing the session token.
- `place` is the location the user typed.
- `log` is the application's logger, a Deephaven [`Logger`](/core/javadoc/io/deephaven/io/logger/Logger.html). Substitute your application's own logger.

### Connect to the server

The first step is to create a [`ManagedChannel`](https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannel.html), the root connection between your application and the Deephaven server. The Java client creates most objects with builders. Here, the code configures a [`ManagedChannelBuilder`](https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html) and then calls `build`.

```java
final ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(host);
if (plaintext) {
    channelBuilder.usePlaintext();
} else {
    channelBuilder.useTransportSecurity();
}

managedChannel = channelBuilder.build();
```

Once you have a channel to the server, create a [`BarrageSession`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html). `BarrageSession` extends [`FlightSession`](/core/javadoc/io/deephaven/client/impl/FlightSession.html) and adds table subscriptions and snapshots. `FlightSession` adds Arrow Flight data transfer to a Deephaven session.

By default, a Deephaven server requires a PSK. Pass it with `authenticationTypeAndValue`, prefixed by the name of the PSK authentication handler.

```java
final BufferAllocator bufferAllocator = new RootAllocator();
final BarrageSessionFactory barrageSessionFactory =
        DaggerDeephavenBarrageRoot.create().factoryBuilder()
                .managedChannel(managedChannel)
                .scheduler(flightScheduler)
                .allocator(bufferAllocator)
                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler " + psk)
                .build();

barrageSession = barrageSessionFactory.newBarrageSession();
```

Instead of building the channel yourself, you can create the session from a [`BarrageSessionFactoryConfig`](/core/javadoc/io/deephaven/client/impl/BarrageSessionFactoryConfig.html), as the [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java) example does.

### Subscribe to the table

The server script stores the result table in a [query scope](/core/docs/how-to-guides/query-scope/) variable named `last_city_by_state`. A remote client asks for a server table with a Flight ticket, which identifies one table stream. To build the ticket for a query scope variable, prefix the variable name with `s/`, as in `s/last_city_by_state`. A client can fetch any table in the query scope the same way.

The subscription produces a local Deephaven table that the Deephaven engine inside the client process maintains. Before it subscribes, the client creates two objects:

- An [update graph](../conceptual/dag.md), which delivers each update to the local table and its listeners.
- An [execution context](../conceptual/execution-context.md), which gives the subscribing thread access to that update graph.

The code also opens a [liveness scope](../conceptual/liveness-scope-concept.md), which holds the subscription and the subscribed table. Closing the scope releases them, and the server stops spending memory and CPU time on the subscription. Keep the scope open for as long as you read from or listen to the table, and close it when you finish, as the `try`-with-resources block below does.

To subscribe, the application does three things:

1. Calls [`TicketTable.fromQueryScopeField`](/core/javadoc/io/deephaven/qst/table/TicketTable.html) to build the `s/` ticket from the variable name, and asks the server for a [`TableHandle`](/core/javadoc/io/deephaven/client/impl/TableHandle.html) for that ticket.
2. Calls [`subscribe`](/core/javadoc/io/deephaven/client/impl/BarrageSession.html) on the `BarrageSession` to subscribe to that handle.
3. Calls [`entireTable`](/core/javadoc/io/deephaven/client/impl/BarrageSubscription.html), which returns a [`Future`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/Future.html). The `Future` completes with the subscribed [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) once all of its rows arrive.

```java
// The subscribed table is a client-side engine table, so the client needs an update graph and an execution context
final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("DEFAULT").existingOrBuild();
final ExecutionContext executionContext = ExecutionContext.newBuilder()
        .markSystemic()
        .emptyQueryScope()
        .newQueryLibrary()
        .setUpdateGraph(updateGraph)
        .build();

final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();

try (final SafeCloseable ignoredContext = executionContext.open();
        final SafeCloseable ignoredScope = LivenessScopeStack.open();
        final TableHandle handle = barrageSession.session()
                .of(TicketTable.fromQueryScopeField("last_city_by_state"))) {
    final BarrageSubscription subscription = barrageSession.subscribe(handle, options);
    final Table statsTable = subscription.entireTable().get();

    // Read from or listen to statsTable here
}
```

At this point, `statsTable` is a local copy of the server's `last_city_by_state` table, and it updates as the server's data changes. This application displays the table directly. Other applications can filter, join, or aggregate it like any other Deephaven table.

The steps above are adapted from the [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java) example.

### Listen to the table

Once the client has a local ticking table, such as `statsTable` from [Subscribe to the table](#subscribe-to-the-table), it can process the table's data. You can read the data from the table directly. You can also add a listener to the table so that the application responds to each update, as the [table listeners guide](./table-listeners-groovy.md) explains.

Read the table or add the listener inside the `try` block that opened the liveness scope, and keep that block open for as long as the listener should receive updates. The [`SubscribeTable`](https://github.com/deephaven/deephaven-core/blob/main/java-client/barrage-examples/src/main/java/io/deephaven/client/examples/SubscribeTable.java) example does this by waiting on a [`CountDownLatch`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CountDownLatch.html) inside the block until it has received the number of updates it asked for. When the block exits, the scope closes and releases the subscription and the table, and the listener stops receiving updates.

```java
statsTable.addUpdateListener(listener = new InstrumentedTableUpdateListenerAdapter(statsTable, false) {
    @Override
    public void onUpdate(TableUpdate upstream) {
        // Process the update as needed. The TableUpdate describes:
        // 1) What rows have been added
        // 2) What rows have been removed
        // 3) What rows have been modified
        // 4) What rows have been structurally shifted in address space, but without changes to column data
        // 5) What columns were affected by the changes
    }
});
```

The `false` argument tells [`InstrumentedTableUpdateListenerAdapter`](/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html) not to keep its own reference to the listener, so the code keeps one in the `listener` field. Without that field, the JVM can garbage collect the listener, and the listener stops receiving updates.

### Run code on the server

The Java client can also run code on the server through a [`ConsoleSession`](/core/javadoc/io/deephaven/client/impl/ConsoleSession.html). For example, the client can create more derived tables with the same query language you use in the Deephaven IDE.

This example lets the user add cities to the set of monitored cities.

```java
try (final ConsoleSession console = barrageSession.session().console("python").get()) {
    final Changes c = console.executeCode("begin_watch(\"" + escapeString(place) + "\")");
    if (c.errorMessage().isPresent()) {
        log.error().append("Error adding " + place + ": " + c.errorMessage().get()).endl();
    } else {
        log.info().append("Added " + place).endl();
    }
} catch (ExecutionException | InterruptedException | TimeoutException e) {
    e.printStackTrace();
}
```

This code sends the server a Python call to `begin_watch` that passes the escaped location string. It then checks the response for an error. The console type, `"python"`, must match the server's scripting language. The server returns a [`Changes`](/core/javadoc/io/deephaven/client/impl/script/Changes.html) object, which tells the application what changed in the server's scope, such as which tables and variables the command created or updated.

> [!WARNING]
> This example embeds a user string into a command. `escapeString` stands for a helper that you write to escape quotes and other special characters within the string. Without it, a user can inject arbitrary code into the command that the server runs.

To send data from the client without running code on the server, use [input tables](./java-client-input-tables.md).

## Related documentation

- [Send data to Deephaven from a Java client](./java-client-input-tables.md)
- [Arrow Flight](./data-import-export/arrow-flight.md)
- [Servers and clients](../conceptual/client-server-model.md)
