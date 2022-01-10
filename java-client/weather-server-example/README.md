## Java API Example -- Weather Dashboard
This project contains an example of a simple Deephaven server application with a Java client.
The server portion of the example uses Python to connect to and pull weather and climate data from NOAA APIs, from which
it produces a live ticking table.  It performs some simple aggregations on that data
and provides a function which a client can invoke with the Java API
to add new locations to be monitored.

### Setup
To run the example, you need a valid Google Geolocation API key.  Edit the included weather_server.py script
and replace "YOUR_AP_KEY_HERE" with your key.  If you are building Deephaven from source follow the instructions to build
the containers,  then simply start the weather server with `docker-compose up`

To run the client portion of the application you may either run it directly from the main class `WeatherDash` from your 
IDE of choice, or compile the project and run it with the following command

`./gradlew :weather-server-example:run`

### Usage

Start the server and client as described above.  When the client starts, click "Connect", then add locations by typing
their address (as you would with Google Maps).  Once added, the table in the client will update as new data is pulled.
