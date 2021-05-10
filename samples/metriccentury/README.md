# Metric Century #

The `metriccentury.csv` file contains observations of a bicycle ride of 100 kilometers. Every second, the rider's heart rate, speed, cumulative distance, and power output are recorded. Each observation also includes 3D GPS positioning that reveals the absolute position and elevation of the rider.

The set has 16987 observations that span five hours, 36 minutes of elapsed time.

## Fields ##
- **Time:** UTC time of this observation
- **Cadence:** pedal strokes/minute
- **SpeedKPH:** speed in Km/H
- **Watts:** instantaneous power output in watts
- **LatitudeDegrees:** latitude position in decimal (positive is north)
- **LongitudeDegrees:** longitude position in decimal (negative is west)
- **AltitudeMeters:** altitude above sea-level in meters
- **HeartRate:** heart rate in beats/minute

## Data challenges ##

- One clear problem is false precision. Most values are expressed to eight or ten decimal places, when that precision isn't justified. For example, the rider's altitude is certainly not reliably known to the micro-meter.
- GPS selective availability causes vibration at rest, or jumps while moving.
- Sensors may occasional report unlikely or impossible values.
- The rider sometimes stops, so there are gaps in the time-line.

## Simple queries ##

- Highest and lowest altitudes
- Bounding box of travel (eastern-most, northern-most, ... points)
- highest speed
- fastest heart rate
- maximum distance from start
- time in fitness zones (histogram of heart rate)
- time in power zones (histogram of power)
- max sustained effort (highest sustained effort for buckets of interval length)

## More interesting queries ##

- steepest apparent descent or ascent
- sharpest acceleration or deceleration
- total energy expended (assuming a total mass of 100 kg for bike, rider, and payload)
- was the rider ever walking the bike (cadence = 0, but speed != 0)
- how many stop lights or stop signs did the rider encounter?
- longest sustained climb (or descent) gradient
- longest sustained power effort
- coasting or not coasting, vs speed?

## Interesting visualizations ##

- power-to-gradient correlation
- power-to-speed correlation
- plot the route; add a third dimension (like speed or power; even altitude)
- effort (total watts) over time
- more effort at beginning, middle, or end of the ride?

# Source and License

This data was contributed to the public domain by the rider, Mike Blaszczak. It is provided here for demonstrative purposes without any warranty for fitness of purpose or usability.