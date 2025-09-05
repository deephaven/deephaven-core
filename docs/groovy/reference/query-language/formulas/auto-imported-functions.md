---
title: Auto-imported functions
sidebar_label: Auto-imported functions
---

This guide lists the Java packages that are automatically available in a Deephaven session.

These imports, which include classes, methods, and constants, are available to use throughout Deephaven without specifying the classpath. In the following example, the query string calls [`abs`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#abs(int)), which is automatically imported upon server startup.

```groovy order=source,result
source = newTable(intCol("IntegerColumn", 1, 2, -2, -1))
result = source.update("Abs = abs(IntegerColumn)")
```

## Deephaven's automatic query language imports

Here is a complete list of everything that is imported automatically into the query language when a new instance of the Groovy IDE is started:

<details>
<summary>java.lang.*</summary>

- [java.lang.\*](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/package-summary.html)

The `java.lang` package contains classes that are key building blocks of the Java language, which Deephaven's engine is dependent on. This package contains the following Classes:

- Boolean
- Byte
- Character
- Character.Subset
- Character.UnicodeBlock
- Class
- ClassLoader
- ClassValue
- Compiler
- Double
- Enum
- Float
- InheritableThreadLocal
- Integer
- Long
- Math
- Number
- Object
- Package
- Process
- ProcessBuilder
- ProcessBuilder.Redirect
- Runtime
- RuntimePermission
- SecurityManager
- Short
- StackTraceElement
- StrictMath
- String
- StringBuffer
- StringBuilder
- System
- Thread
- ThreadGroup
- ThreadLocal
- Throwable
- Void

</details>

<details>
<summary>io.deephaven.engine.table.Table</summary>

- [io.deephaven.engine.table.Table](/core/javadoc/io/deephaven/engine/table/Table.html)

`Table` is an interface that provides access to the following methods:

- addUpdateListener
- apply
- applyToAllBy
- awaitUpdate
- byteColumnIterator
- characterColumnIterator
- close
- coalesce
- columnIterator
- doubleColumnIterator
- dropColumnFormats
- flatten
- floatColumnIterator
- formatColumns
- formatColumnWhere
- formatRowWhere
- getColumnSource
- getColumnSourceMap
- getColumnSources
- getDefinition
- getDescription
- getRowSet
- getSubTable
- hasColumns
- headBy
- headPct
- integerColumnIterator
- isEmpty
- isFailed
- isFlat
- isRefreshing
- longColumnIterator
- meta
- moveColumns
- moveColumnsDown
- moveColumnsUp
- numColumns
- objectColumnIterator
- partitionBy
- partitionedAggBy
- releaseCachedResources
- removeBlink
- removeUpdateListener
- renameAllColumns
- renameColumns
- rollup
- setTotalsTable
- shortColumnIterator
- sizeForInstrumentation
- slice
- slicePct
- tailBy
- tailPct
- tree
- withKeys
- withUniqueKeys
- wouldMatch

</details>

<details>
<summary>io.deephaven.engine.util.ColorUtilImpl.*</summary>

- [io.deephaven.engine.util.ColorUtilImpl.\*](/core/javadoc/io/deephaven/engine/util/ColorUtilImpl.html)

This class contains the following static methods for setting and formatting colors:

- background
- backgroundForeground
- backgroundForegroundAuto
- backgroundOverride
- bg
- bgfg
- bgfga
- bgo
- fg
- fgo
- foreground
- foregroundOverride
- heatmap
- heatmapFg
- heatmapForeground
- isBackgroundSelectionOverride
- toLong
- valueOf

</details>

<details>
<summary>io.deephaven.function.Basic.*</summary>

- [io.deephaven.function.Basic.\*](/core/javadoc/io/deephaven/function/Basic.html)

This class contains a set of basic static functions that can be applied to primitive types:

- array
- arrayObj
- concat
- count
- countDistinct
- countDistinctObj
- countObj
- distinct
- distinctObj
- enlist
- first
- firstIndexOf
- firstObj
- forwardFill
- ifelse
- ifelseObj
- in
- inObj
- inRange
- isNull
- last
- lastObj
- len
- nth
- nthObj
- nullValueFor
- repeat
- replaceIfNull
- reverse
- reverseObj
- unbox
- vec
- vecObj

</details>

<details>
<summary>io.deephaven.function.Logic.*</summary>

- [io.deephaven.function.Logic.\*](/core/javadoc/io/deephaven/function/Logic.html)

Importing this class allows the use of the following logic expressions in query strings:

- `&&` (and)
- `!` (not)
- `||` (or)

</details>

<details>
<summary>io.deephaven.function.Numeric.*</summary>

- [io.deephaven.function.Numeric.\*](/core/javadoc/io/deephaven/function/Numeric.html)

The `Numeric` Class contains a set of commonly used numeric static functions that can be applied to numeric types:

- abs
- absAvg
- acos
- asin
- atan
- avg
- ceil
- clamp
- containsNonFinite
- cor
- countNeg
- countPos
- countZero
- cov
- cummax
- cummin
- cumprod
- cumsum
- exp
- floor
- indexOfMax
- indexOfMaxObj
- indexOfMin
- indexOfMinObj
- isFinite
- isInf
- isNaN
- log
- lowerBin
- max
- maxObj
- median
- min
- minObj
- percentile
- pow
- product
- replaceIfNaN
- replaceIfNonFinite
- rint
- round
- sequence
- signum
- sin
- sqrt
- std
- ste
- sum
- tan
- tstat
- upperBin
- var
- wavg
- wstd
- wste
- wsum
- wtstat
- wvar

</details>

<details>
<summary>io.deephaven.function.Parse.*</summary>

- [io.deephaven.function.Parse.\*](/core/javadoc/io/deephaven/function/Parse.html)

This class provides static methods for parsing strings to primitive values:

- parseBoolean
- parseByte
- parseDouble
- parseFloat
- parseInt
- parseLong
- parseShort
- parseUnsignedInt
- parseUnsignedLong

</details>

<details>
<summary>io.deephaven.function.Random.*</summary>

- [io.deephaven.function.Random.\*](/core/javadoc/io/deephaven/function/Random.html)

This class provides static methods for generating random values:

- random
- randomBool
- randomDouble
- randomFloat
- randomGaussian
- randomInt
- randomLong

</details>

<details>
<summary>io.deephaven.function.Sort.*</summary>

- [io.deephaven.function.Sort.\*](/core/javadoc/io/deephaven/function/Sort.html)
  This class contains static methods used for sorting primitive types:
- sort
- sortDescending
- sortDescendingObj
- sortObj

</details>

<details>
<summary>io.deephaven.gui.color.Color.*</summary>

- [io.deephaven.gui.color.Color.\*](/core/javadoc/io/deephaven/gui/color/Color.html)

The `Color` class contains static methods for creating colors:

- color
- colorHSL
- colorNames
- colorRGB
- valueOf
- values

The `Color` class also contains the following static constants representing specific colors:

- ALICEBLUE
- ANTIQUEWHITE
- AQUA
- AQUAMARINE
- AZURE
- BEIGE
- BISQUE
- BLACK
- BLANCHEDALMOND
- BLUE
- BLUEVIOLET
- BRIGHT_BLUE
- BRIGHT_BLUEGREEN
- BRIGHT_GREEN
- BRIGHT_GREENYELLOW
- BRIGHT_PURPLE
- BRIGHT_PURPLEBLUE
- BRIGHT_RED
- BRIGHT_REDPURPLE
- BRIGHT_YELLOW
- BRIGHT_YELLOWRED
- BROWN
- BURLYWOOD
- CADETBLUE
- CHARTREUSE
- CHOCOLATE
- CORAL
- CORNFLOWERBLUE
- CORNSILK
- CRIMSON
- CYAN
- DARK_BLUE
- DARK_BLUEGREEN
- DARK_GREEN
- DARK_GREENYELLOW
- DARK_PURPLE
- DARK_PURPLEBLUE
- DARK_RED
- DARK_REDPURPLE
- DARK_YELLOW
- DARK_YELLOWRED
- DARKBLUE
- DARKCYAN
- DARKGOLDENROD
- DARKGRAY
- DARKGRAYISH_BLUE
- DARKGRAYISH_BLUEGREEN
- DARKGRAYISH_GREEN
- DARKGRAYISH_GREENYELLOW
- DARKGRAYISH_PURPLE
- DARKGRAYISH_PURPLEBLUE
- DARKGRAYISH_RED
- DARKGRAYISH_REDPURPLE
- DARKGRAYISH_YELLOW
- DARKGRAYISH_YELLOWRED
- DARKGREEN
- DARKGREY
- DARKKHAKI
- DARKMAGENTA
- DARKOLIVEGREEN
- DARKORANGE
- DARKORCHID
- DARKRED
- DARKSALMON
- DARKSEAGREEN
- DARKSLATEBLUE
- DARKSLATEGRAY
- DARKTURQUOISE
- DARKVIOLET
- DB_GREEN
- DB_ORANGE
- DB_PINK
- DEEP_BLUE
- DEEP_BLUEGREEN
- DEEP_GREEN
- DEEP_GREENYELLOW
- DEEP_PURPLE
- DEEP_PURPLEBLUE
- DEEP_RED
- DEEP_REDPURPLE
- DEEP_YELLOW
- DEEP_YELLOWRED
- DEEPPINK
- DEEPSKYBLUE
- DIMGRAY
- DIMGREY
- DODGERBLUE
- DULL_BLUE
- DULL_BLUEGREEN
- DULL_GREEN
- DULL_GREENYELLOW
- DULL_PURPLE
- DULL_PURPLEBLUE
- DULL_RED
- DULL_REDPURPLE
- DULL_YELLOW
- DULL_YELLOWRED
- FIREBRICK
- FLORALWHITE
- FORESTGREEN
- FUCHSIA
- GAINSBORO
- GHOSTWHITE
- GOLD
- GOLDENROD
- GRAY
- GRAY1
- GRAY2
- GRAY3
- GRAY4
- GRAY5
- GRAY6
- GRAY7
- GRAY8
- GRAYISH_BLUE
- GRAYISH_BLUEGREEN
- GRAYISH_GREEN
- GRAYISH_GREENYELLOW
- GRAYISH_PURPLE
- GRAYISH_PURPLEBLUE
- GRAYISH_RED
- GRAYISH_REDPURPLE
- GRAYISH_YELLOW
- GRAYISH_YELLOWRED
- GREEN
- GREENYELLOW
- GREY
- HONEYDEW
- HOTPINK
- INDIANRED
- INDIGO
- IVORY
- KHAKI
- LAVENDER
- LAVENDERBLUSH
- LAWNGREEN
- LEMONCHIFFON
- LIGHT_BLUE
- LIGHT_BLUEGREEN
- LIGHT_GREEN
- LIGHT_GREENYELLOW
- LIGHT_PURPLE
- LIGHT_PURPLEBLUE
- LIGHT_RED
- LIGHT_REDPURPLE
- LIGHT_YELLOW
- LIGHT_YELLOWRED
- LIGHTBLUE
- LIGHTCORAL
- LIGHTCYAN
- LIGHTGOLDENRODYELLOW
- LIGHTGRAY
- LIGHTGRAYISH_BLUE
- LIGHTGRAYISH_BLUEGREEN
- LIGHTGRAYISH_GREEN
- LIGHTGRAYISH_GREENYELLOW
- LIGHTGRAYISH_PURPLE
- LIGHTGRAYISH_PURPLEBLUE
- LIGHTGRAYISH_RED
- LIGHTGRAYISH_REDPURPLE
- LIGHTGRAYISH_YELLOW
- LIGHTGRAYISH_YELLOWRED
- LIGHTGREEN
- LIGHTGREY
- LIGHTPINK
- LIGHTSALMON
- LIGHTSEAGREEN
- LIGHTSKYBLUE
- LIGHTSLATEGRAY
- LIGHTSTEELBLUE
- LIGHTYELLOW
- LIME
- LIMEGREEN
- LINEN
- MAGENTA
- MAROON
- MEDIUMAQUAMARINE
- MEDIUMBLUE
- MEDIUMORCHID
- MEDIUMPURPLE
- MEDIUMSEAGREEN
- MEDIUMSLATEBLUE
- MEDIUMSPRINGGREEN
- MEDIUMTURQUOISE
- MEDIUMVIOLETRED
- MIDNIGHTBLUE
- MINTCREAM
- MISTYROSE
- MOCCASIN
- NAVAJOWHITE
- NAVY
- NO_FORMATTING
- OLDLACE
- OLIVE
- OLIVEDRAB
- ORANGE
- ORANGERED
- ORCHID
- PALE_BLUE
- PALE_BLUEGREEN
- PALE_GREEN
- PALE_GREENYELLOW
- PALE_PURPLE
- PALE_PURPLEBLUE
- PALE_RED
- PALE_REDPURPLE
- PALE_YELLOW
- PALE_YELLOWRED
- PALE_GOLDENROD
- PALEGREEN
- PALETURQUOISE
- PALEVIOLETRED
- PAPAYAWHIP
- PEACHPUFF
- PERU
- PINK
- PLUM
- POWDERBLUE
- PURPLE
- REBECCAPURPLE
- RED
- ROSYBROWN
- ROYALBLUE
- SADDLEBROWN
- SALMON
- SANDYBROWN
- SEAGREEN
- SEASHELL
- SIENNA
- SILVER
- SKYBLUE
- SLATEBLUE
- SLATEGRAY
- SLATEGREY
- SNOW
- SPRINGGREEN
- STEELBLUE
- STRONG_BLUE
- STRONG_BLUEGREEN
- STRONG_GREEN
- STRONG_GREENYELLOW
- STRONG_PURPLE
- STRONG_PURPLEBLUE
- STRONG_RED
- STRONG_REDPURPLE
- STRONG_YELLOW
- STRONG_YELLOWRED
- TAN
- TEAL
- THISTLE
- TOMATO
- TURQUOISE
- VERYPALE_BLUE
- VERYPALE_BLUEGREEN
- VERYPALE_GREEN
- VERYPALE_GREENYELLOW
- VERYPALE_PURPLE
- VERYPALE_PURPLEBLUE
- VERYPALE_RED
- VERYPALE_REDPURPLE
- VERYPALE_YELLOW
- VERYPALE_YELLOWRED
- VIOLET
- VIVID_BLUE
- VIVID_BLUEGREEN
- VIVID_GREEN
- VIVID_GREENYELLOW
- VIVID_PURPLE
- VIVID_PURPLEBLUE
- VIVID_RED
- VIVID_REDPURPLE
- VIVID_YELLOW
- VIVID_YELLOWRED
- WHEAT
- WHITE
- WHITESMOKE
- YELLOW
- YELLOWGREEN

</details>

<details>
<summary>io.deephaven.time.DateTimeUtils.*</summary>

- [io.deephaven.time.DateTimeUtils.\*](/core/javadoc/io/deephaven/time/DateTimeUtils.html)

The `DateTimeUtils` class contains static methods for working with time:

- atMidnight
- currentClock
- dayOfMonth
- dayOfWeek
- dayOfYear
- diffDays
- diffMicros
- diffMillis
- diffMinutes
- diffNanos
- diffSeconds
- diffYears365
- diffYearsAvg
- epochAutoToEpochNanos
- epochAutoToInstant
- epochAutoToZonedDateTime
- epochMicros
- epochMicrosToInstant
- epochMicrosToZonedDateTime
- epochMillis
- epochMillisToInstant
- epochMillisToZonedDateTime
- epochNanos
- epochNanosToInstant
- epochNanosToZonedDateTime
- epochSeconds
- epochSecondsToInstant
- epochSecondsToZonedDateTime
- excelToInstant
- excelToZonedDateTime
- formatDate
- formatDateTime
- formatDurationNanos
- hourOfDay
- isAfter
- isAfterOrEqual
- isBefore
- isBeforeOrEqual
- lowerBin
- microsOfMilli
- microsOfSecond
- microsToMillis
- microsToNanos
- microsToSeconds
- millisOfDay
- millisOfSecond
- millisToMicros
- millisToNanos
- millisToSeconds
- minus
- minuteOfDay
- minuteOfHour
- monthOfYear
- nanosOfDay
- nanosOfMilli
- nanosOfSecond
- nanosToMicros
- nanosToMillis
- nanosToSeconds
- now
- nowMillisResolution
- nowSystem
- nowSystemMillisResolution
- parseDuration
- parseDurationNanos
- parseDurationNanosQuiet
- parseDurationQuiet
- parseEpochNanos
- parseEpochNanosQuiet
- parseInstant
- parseInstantQuiet
- parseLocalDate
- parseLocalDateQuiet
- parseLocalTime
- parseLocalTimeQuiet
- parsePeriod
- parsePeriodQuiet
- parseTimePrecision
- parseTimeZone
- parseTimeZoneQuiet
- parseZonedDateTime
- parseZonedDateTimeQuiet
- plus
- secondOfDay
- secondOfMinute
- secondsToMicros
- secondsToMillis
- secondsToNanos
- setClock
- timeZone
- timeZoneAliasAdd
- timeZoneAliasRm
- toDate
- today
- toExcelTime
- toInstant
- toLocalDate
- toLocalTime
- toZonedDateTime
- upperBin
- year
- yearOfCentury

</details>

<details>
<summary>io.deephaven.time.calendar.StaticCalendarMethods.*</summary>

- [io.deephaven.time.calendar.StaticCalendarMethods.\*](/core/javadoc/io/deephaven/time/calendar/StaticCalendarMethods.html)

The `StaticCalendarMethods` class contains convenience methods for the [`Calendar`](/core/javadoc/io/deephaven/time/calendar/Calendar.html) and [`BusinessCalendar`](/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html) interfaces. Importing this class automatically means that these interfaces do not have to be imported for users to access their static methods:

- businessDaysInRange
- calendarTimeZone
- currentDay
- dayOfWeek
- daysInRange
- diffBusinessDay
- diffBusinessNanos
- diffBusinessYear
- diffNonBusinessDay
- diffNonBusinessNanos
- fractionOfBusinessDayComplete
- fractionOfBusinessDayRemaining
- fractionOfStandardBusinessDay
- getBusinessSchedule
- isBusinessDay
- isBusinessTime
- isLastBusinessDayOfMonth
- isLastBusinessDayOfWeek
- name
- nextBusinessDay
- nextBusinessSchedule
- nextDay
- nextNonBusinessDay
- nextNonBusinessDaysInRange
- numberOfBusinessDays
- numberOfDays
- numberOfNonBusinessDays
- previousBusinessDay
- previousBusinessSchedule
- previousDay
- previousNonBusinessDay
- standardBusinessDayLength

</details>

<details>
<summary>io.deephaven.util.QueryConstants.*</summary>

- [io.deephaven.util.QueryConstants.\*](/core/javadoc/io/deephaven/util/QueryConstants.html)

This class contains static constants for commonly used or special values of primitive types. These are:

- MAX_BYTE
- MAX_CHAR
- MAX_DOUBLE
- MAX_FINITE_DOUBLE
- MAX_FINITE_FLOAT
- MAX_FLOAT
- MAX_INT
- MAX_LONG
- MAX_SHORT
- MIN_BYTE
- MIN_CHAR
- MIN_DOUBLE
- MIN_FINITE_DOUBLE
- MIN_FINITE_FLOAT
- MIN_FLOAT
- MIN_INT
- MIN_LONG
- MIN_POS_DOUBLE
- MIN_POS_FLOAT
- MIN_SHORT
- NAN_DOUBLE
- NAN_FLOAT
- NEG_INFINITY_DOUBLE
- NEG_INFINITY_FLOAT
- NULL_BOOLEAN
- NULL_BYTE
- NULL_BYTE_BOXED
- NULL_CHAR
- NULL_CHAR_BOXED
- NULL_DOUBLE
- NULL_DOUBLE_BOXED
- NULL_FLOAT
- NULL_FLOAT_BOXED
- NULL_INT
- NULL_INT_BOXED
- NULL_LONG
- NULL_LONG_BOXED
- NULL_SHORT
- NULL_SHORT_BOXED
- POS_INFINITY_DOUBLE
- POS_INFINITY_FLOAT

</details>

<details>
<summary>java.time.Duration</summary>

- [java.time.Duration](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Duration.html)

A `Duration` is an object representing an amount of time, for example "45.3 seconds".
Importing this class automatically means that users can access the following methods:

- abs
- addTo
- compareTo
- dividedBy
- equals
- get
- getNano
- getSeconds
- getUnits
- hashCode
- isNegative
- isZero
- minus
- minusDays
- minusHours
- minusMillis
- minusMinutes
- minusNanos
- minusSeconds
- multipliedBy
- negated
- plus
- plusDays
- plusHours
- plusMinutes
- plusNanos
- plusSeconds
- subtractFrom
- toDays
- toHours
- toMillis
- toMinutes
- toNanos
- toString
- withNanos
- withSeconds

</details>

<details>
<summary>java.time.Instant</summary>

- [java.time.Instant](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)

An `Instant` is an object that represents an instantaneous point in time. This is useful for creating timestamps. Importing this class automatically means that users can access the following methods:

- adjustInto
- atOffset
- atZone
- compareTo
- equals
- get
- getEpochSecond
- getLong
- getNano
- hashCode
- isAfter
- isBefore
- isSupported
- minus
- minusMillis
- minusNanos
- minusSeconds
- plus
- plusMillis
- plusNanos
- plusSeconds
- query
- range
- toEpochMilli
- toString
- truncatedTo
- until
- with

</details>

<details>
<summary>java.time.LocalDate</summary>

- [java.time.LocalDate](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalDate.html)

A `LocalDate` is a date without a time zone - for example, "2022-09-08". Importing this class gives users access to the following methods:

- adjustInto
- atStartOfDay
- atTime
- compareTo
- equals
- format
- get
- getChronology
- getDayOfMonth
- getDayOfWeek
- getDayOfYear
- getEra
- getLong
- getMonth
- getMonthValue
- getYear
- hashCode
- isAfter
- isBefore
- isEqual
- isLeapYear
- isSupported
- lengthOfMonth
- lengthOfYear
- minus
- minusDays
- minusMonths
- minusWeeks
- minusYears
- plus
- plusDays
- plusMonths
- plusWeeks
- plusYears
- query
- range
- toEpochDay
- toString
- until
- with
- withDayOfMonth
- withDayOfYear
- withMonth
- withYear

</details>

<details>
<summary>java.time.LocalTime</summary>

- [java.time.LocalTime](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/LocalTime.html)

A `LocalTime` is an immutable date-time object that represents a time as a string with the format "HH:MM:SS:nnnnnnnnn". There is no date or time zone; only the local time one would see on a wall clock.

The following methods can be used in query strings:

- adjustInto
- atDate
- atOffset
- compareTo
- equals
- format
- get
- getHour
- getLong
- getMinute
- getNano
- getSecond
- hashCode
- isAfter
- isBefore
- isSupported
- minus
- minusHours
- minusMinutes
- minusNanos
- minusSeconds
- plus
- plusHours
- plusMinutes
- plusNanos
- plusSeconds
- query
- range
- toNanoOfDay
- toSecondOfDay
- toString
- truncatedTo
- until
- with
- withHour
- withMinute
- withNano
- withSecond

</details>

<details>
<summary>java.time.Period</summary>

- [java.time.Period](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/Period.html)

A `Period` is an amount of time in years, months, and days. Importing this class gives users access to the following methods:

- addTo
- equals
- get
- getChronology
- getDays
- getMonths
- getUnits
- getYears
- hashCode
- isNegative
- isZero
- minus
- minusDays
- minusMonths
- minusYears
- multipliedBy
- negated
- normalized
- plus
- plusDays
- plusMonths
- plusYears
- subtractFrom
- toString
- toTotalMonths
- withDays
- withMonths
- withYears

</details>

<details>
<summary>java.time.ZonedDateTime</summary>

- [java.time.ZonedDateTime](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html)

A ZonedDateTime is a date-time with a time-zone in the ISO-8601 calendar system, such as 2023-08-12T12:24:00.000-05:00 America/Chicago.
Importing this class gives users access to methods for manipulating, comparing, modifying, and converting ZonedDateTime objects:

- equals
- format
- get
- getDayOfMonth
- getDayOfWeek
- getDayOfYear
- getHour
- getLong
- getMinute
- getMonth
- getMonthValue
- getNano
- getOffset
- getSecond
- getYear
- getZone
- hashCode
- isSupported
- minus
- minusDays
- minusHours
- minusMinutes
- minusMonths
- minusNanos
- minusSeconds
- minusWeeks
- minusYears
- plus
- plusDays
- plusHours
- plusMinutes
- plusMonths
- plusNanos
- plusSeconds
- plusWeeks
- plusYears
- query
- range
- toLocalDate
- toLocalDateTime
- toLocalTime
- toOffsetDateTime
- toString
- truncatedTo
- until
- with
- withDayOfMonth
- withDayOfYear
- withEarlierOffestAtOverlap
- withFixedOffsetZone
- withHour
- withLaterOffsetAtOverlap
- withMinute
- withMonth
- withNano
- withSecond
- withYear
- withZoneSameInstant
- withZoneSameLocal

</details>

## Related documentation

- [Query language functions](../../../how-to-guides/query-language-functions.md)
- [QueryLibraryImportsDefaults Java class](https://github.com/deephaven/deephaven-core/blob/main/engine/table/src/main/java/io/deephaven/engine/table/lang/impl/QueryLibraryImportsDefaults.java)
