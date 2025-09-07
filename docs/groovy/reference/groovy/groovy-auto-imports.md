---
title: Automatic Groovy Imports
---

At startup, Deephaven automatically imports a variety of important classes and methods that the Deephaven engine uses behind the scenes. It also imports a range of methods that are used in the query language.

In addition to these two categories, Deephaven's Groovy IDE also automatically imports a number of methods that are used frequently in Deephaven. They are imported automatically as a matter of convenience. These methods cannot be used inside query strings (unless they also appear in the list of query language imports). Here is the complete list:

## Automatically imported classes

<details>
<summary>io.deephaven.engine.util.TableTools.*</summary>

- [io.deephaven.engine.util.TableTools.\*](/core/javadoc/io/deephaven/engine/util/TableTools.html)

This class contains methods for working with tables. The following methods are available:

- base64Fingerprint
- booleanCol
- byteCol
- charCol
- col
- colSource
- computeFingerprint
- diff
- diffPair
- doubleCol
- emptyTable
- floatCol
- html
- instantCol
- intCol
- longCol
- merge
- mergeSorted
- newTable
- nullToNullString
- nullTypeAsString
- objColSource
- roundDecimalColumns
- roundDecimalColumnsExcept
- shortCol
- show
- showCommaDelimited
- showWithRowSet
- string
- stringCol
- timeTable
- timeTableBuilder
- typeFromName

</details>

<details>
<summary>io.deephaven.engine.table.impl.util.TableLoggers.*</summary>

- [io.deephaven.engine.table.impl.util.TableLoggers.\*](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html)

This class contains methods for accessing Deephaven tables of instrumentation logs, including query logs and performance logs. The following methods are available:

- processInfoLog
- processMetricsLog
- queryOperationPerformanceLog
- queryPerformanceLog
- serverStateLog
- updatePerformanceLog

</details>

<details>
<summary>io.deephaven.api.*</summary>

- [io.deephaven.api.\*](/core/javadoc/io/deephaven/api/package-summary.html)

This package contains the core classes for the Deephaven API.

</details>

<details>
<summary>io.deephaven.api.filter.*</summary>

- [io.deephaven.api.filter.\*](/core/javadoc/io/deephaven/api/filter/package-summary.html)

This package contains classes for filtering tables.

</details>

<details>
<summary>io.deephaven.engine.table.vectors.ColumnVectors</summary>

- [io.deephaven.engine.table.vectors.ColumnVectors](/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html)

This class provides access to the following instance methods:

- of
- ofByte
- ofChar
- ofDouble
- ofFloat
- ofInt
- ofLong
- ofObject
- ofShort

</details>

<details>
<summary>io.deephaven.engine.table.Table</summary>

- [io.deephaven.engine.table.Table](/core/javadoc/io/deephaven/engine/table/Table.html)

This is an interface that allows users to interact with and modify Deephaven tables. Automatically importing this class gives users access to the following methods:

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
<summary>io.deephaven.engine.table.TableFactory</summary>

- [io.deephaven.engine.table.TableFactory](/core/javadoc/io/deephaven/engine/table/TableFactory.html)

This class contains methods for creating and merging Deephaven tables. The following methods are available:

- emptyTable
- merge
- newTable
- of
- ticket
- timeTable

</details>

<details>
<summary>io.deephaven.engine.table.PartitionedTable</summary>

- [io.deephaven.engine.table.PartitionedTable](/core/javadoc/io/deephaven/engine/table/PartitionedTable.html)

This is an interface that allows users to interact with and modify Deephaven partitioned tables. Automatically importing this class gives users access to the following methods:

- constituentChangesPermitted
- constituentColumnName
- constituentDefinition
- constituentFor
- constituents
- filter
- keyColumnNames
- merge
- partitionedTransform
- proxy
- sort
- table
- transform
- uniqueKeys

</details>

<details>
<summary>io.deephaven.engine.table.PartitionedTableFactory</summary>

- [io.deephaven.engine.table.PartitionedTableFactory](/core/javadoc/io/deephaven/engine/table/PartitionedTableFactory.html)

This is an interface that provides methods for creating and merging Deephaven partitioned tables. Automatically importing this class gives users access to the following methods:

- PartitionedTable.of
- PartitionedTable.ofTables

</details>

<details>
<summary>java.lang.reflect.Array</summary>

- [java.lang.reflect.Array](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/reflect/Array.html)

This class allows Deephaven users to access the following methods:

- Array.get
- Array.getBoolean
- Array.getByte
- Array.getChar
- Array.getDouble
- Array.getFloat
- Array.getInt
- Array.getLength
- Array.getLong
- Array.getShort
- Array.newInstance
- Array.set
- Array.setBoolean
- Array.setByte
- Array.setChar
- Array.setDouble
- Array.setFloat
- Array.setInt
- Array.setLong
- Array.setShort

</details>

<details>
<summary>io.deephaven.util.type.TypeUtils</summary>

- [io.deephaven.util.type.TypeUtils](/core/javadoc/io/deephaven/util/type/TypeUtils.html)

This class contains utility functions to convert primitive types. Methods are static, so they must be used with the class name, e.g., `TypeUtils.box`.

- box
- classForName
- decode64Serializable
- encode64Serializable
- fromString
- getBoxedType
- getErasedType
- getTypeBoxer
- getUnboxedType
- getUnboxedTypeIfBoxed
- isBigNumeric
- isBoxedArithmetic
- isBoxedBoolean
- isBoxedByte
- isBoxedChar
- isBoxedDouble
- isBoxedFloat
- isBoxedInteger
- isBoxedLong
- isBoxedNumeric
- isBoxedShort
- isBoxedType
- isCharacter
- isConvertibleToPrimitive
- isDateTime
- isFloatType
- isNumeric
- isPrimitiveChar
- isPrimitiveNumeric
- isString
- nullConstantForType
- objectToString
- toByteArray
- toDoubleArray
- toFloatArray
- toIntArray
- toLongArray
- toShortArray
- unbox

</details>

<details>
<summary>io.deephaven.util.type.ArrayTypeUtils</summary>

- [io.deephaven.util.type.ArrayTypeUtils](/core/javadoc/io/deephaven/util/type/ArrayTypeUtils.html)

This class contains common utilities for interacting generically with arrays. The imported methods are static, so they must be used with the class name, e.g., `ArrayTypeUtils.getArrayAccessor`.

- booleanNullArray
- boxedToPrimitive
- byteNullArray
- charNullArray
- createArrayAccessor
- doubleNullArray
- equals
- floatNullArray
- getAccessorForElementType
- getArrayAccessor
- getArrayAccessorFromArray
- getBoxedArray
- getUnboxedArray
- getUnboxedByteArray
- getUnboxedCharArray
- getUnboxedDoubleArray
- getUnboxedFloatArray
- getUnboxedIntArray
- getUnboxedLongArray
- getUnboxedShortArray
- intNullArray
- longNullArray
- shortNullArray
- toArray
- toString

</details>

<details>
<summary>io.deephaven.base.string.cache.CompressedString</summary>

- [io.deephaven.base.string.cache.CompressedString](/core/javadoc/io/deephaven/base/string/cache/CompressedString.html)

This class is used for creating immutable `byte[]`-backed representations of strings. The following instance methods are available:

- toCompressedString
- toMappedCompressedString

</details>

<details>
<summary>io.deephaven.base.string.cache.CompressedString.compress</summary>

- [io.deephaven.base.string.cache.CompressedString.compress](https://deephaven.io/core/javadoc/io/deephaven/base/string/cache/CompressedString.html#compress(java.lang.String))

The static method `compress` is imported so that it can be used without the class qualification throughout Deephaven.

</details>

<details>
<summary>java.time.Instant</summary>

- [java.time.Instant](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)

A Java `Instant` is an object that represents a single, precise point in time. This class contains methods for interacting with Instant objects.

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

A Java `LocalDate` is a date without a time zone, such as `2023-08-23`. Deephaven users can automatically use the following instance methods in the IDE:

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

A Java `LocalTime` is a time without a time-zone, such as `10:15:30`. This class contains the following methods for interacting with `LocalTime` objects:

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
<summary>java.time.ZoneId</summary>

- [java.time.ZoneId]https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZoneId.html)

This class contains methods for working with time zones. The following methods are available:

- equals
- getDisplayName
- getId
- getRules
- hashCode
- normalized
- toString

</details>

<details>
<summary>java.time.ZonedDateTime</summary>

- [java.time.ZonedDateTime](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZonedDateTime.html)

A `ZonedDateTime` is a date-time with a time-zone, such as `2007-12-03T10:15:30+01:00 Europe/Paris`. This class contains methods for creating and interacting with ZonedDateTime objects:

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
- withEarlierOffsetAtOverlap
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

<details>
<summary>io.deephaven.engine.context.QueryScopeParam</summary>

- [io.deephaven.engine.context.QueryScopeParam](/core/javadoc/io/deephaven/engine/context/QueryScopeParam.html)

- getName
- getValue

</details>

<details>
<summary>io.deephaven.engine.context.QueryScope</summary>

- [io.deephaven.engine.context.QueryScope](/core/javadoc/io/deephaven/engine/context/QueryScope.html)

- append
- createParam
- getParamNames
- getParams
- hasParamName
- putObjectFields
- putParam
- readParamValue

</details>

<details>
<summary>java.util.*</summary>

- [java.util.\*](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/util/package-summary.html)

This package contains Java's collections framework, and many classes used by Deephaven's backend. While they are callable by users, they are too numerous and used too infrequently to list here.

</details>

<details>
<summary>java.lang.*</summary>

- [java.lang.\*](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/lang/package-summary.html)

This package contains classes that are fundamental to how Java works.

</details>

<details>
<summary>io.deephaven.util.QueryConstants.*</summary>

- [io.deephaven.util.QueryConstants.\*](/core/javadoc/io/deephaven/util/QueryConstants.html)

The following constants are available through this class:

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
<summary>io.deephaven.libs.GroovyStaticImports.*</summary>

- [io.deephaven.libs.GroovyStaticImports.\*](/core/javadoc/io/deephaven/libs/GroovyStaticImports.html)

The following methods are available through this class:

- abs
- absAvg
- acos
- and
- array
- arrayObj
- asin
- atan
- avg
- binSearchIndex
- castDouble
- castInt
- castLong
- ceil
- clamp
- concat
- containsNonFinite
- cor
- cos
- count
- countDistinct
- countDistinctObj
- countNeg
- countObj
- countPos
- countZero
- cov
- cummax
- cummin
- cumprod
- cumsum
- distinct
- distinctObj
- enlist
- exp
- first
- firstIndexOf
- firstIndexOfObj
- firstObj
- floor
- forwardFill
- forwardFillObj
- ifelse
- ifelseObj
- in
- indexOfMax
- indexOfMaxObj
- indexOfMin
- indexOfMinObj
- inObj
- inRange
- isFinite
- isInf
- isNaN
- isNull
- last
- lastObj
- len
- log
- lowerBin
- max
- maxObj
- median
- min
- minObj
- not
- nth
- nthObj
- nullValueFor
- or
- parseBoolean
- parseByte
- parseDouble
- parseFloat
- parseInt
- parseLong
- parseShort
- parseUnsignedInt
- parseUnsignedLong
- percentile
- pow
- product
- random
- randomBool
- randomDouble
- randomFloat
- randomGaussian
- randomInt
- randomLong
- rawBinSearchIndex
- repeat
- replaceIfNaN
- replaceIfNonFinite
- replaceIfNull
- replaceIfNullNaN
- reverse
- reverseObj
- rint
- round
- sequence
- signum
- sin
- sort
- sortDescending
- sortDescendingObj
- sortObj
- sqrt
- std
- ste
- sum
- tan
- tstat
- unbox
- upperBin
- var
- vec
- vecObj
- wavg
- wstd
- wste
- wsum
- wtstat
- wvar

</details>

<details>
<summary>io.deephaven.time.DateTimeUtils.*</summary>

- [io.deephaven.time.DateTimeUtils.\*](/core/javadoc/io/deephaven/time/DateTimeUtils.html)

The `DateTimeUtils` class contains many important methods for working with time in Deephaven. The following methods are available:

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
- parseInsant
- parseInsantQuiet
- parseLocalDate
- parseLocalDateQuiet
- parseLocalTime
- parseLocalTimeQuiet
- parsePeriod
- parsePeriodQuiet
- parseTimePrecision
- parseTimePrecisionQuiet
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
<summary>io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*</summary>

- [io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.\*](/core/javadoc/io/deephaven/engine/table/impl/lang/QueryLanguageFunctionUtils.html)

This class provides access to the following methods:

- binaryAnd
- binaryAndArray
- binaryOr
- binaryOrArray
- booleanPyCast
- byteCast
- bytePyCast
- charCast
- charPyCast
- compareTo
- divide
- divideArray
- doBooleanPyCast
- doStringPyCast
- doubleCast
- doublePyCast
- eq
- eqArray
- floatCast
- floatPyCast
- greater
- greaterArray
- greaterEquals
- greaterEqualsArray
- intCast
- intPyCast
- less
- lessArray
- lessEquals
- lessEqualsArray
- longCast
- longPyCast
- minus
- minusArray
- multiply
- multiplyArray
- negate
- not
- plus
- plusArray
- remainder
- remainderArray
- shortCast
- shortPyCast
- xor
- xorArray

</details>

<details>
<summary>io.deephaven.api.agg.Aggregation.*</summary>

- [io.deephaven.api.agg.Aggregation.\*](/core/javadoc/io/deephaven/api/agg/Aggregation.html)

This class provides aggregation methods that can be applied to a table:

- AggAbsSum
- AggApproxPct
- AggAvg
- AggCount
- AggCountDistinct
- AggCountWhere
- AggDistinct
- AggFirst
- AggFirstRowKey
- AggFormula
- AggFreeze
- AggGroup
- AggLast
- AggLastRowKey
- AggMax
- AggMed
- AggMin
- AggPartition
- AggPct
- AggSortedFirst
- AggSortedLast
- AggStd
- AggSum
- AggTDigest
- AggUnique
- AggVar
- AggWAvg
- AggWSum
- of
- PctOut
- visitAll

</details>

<details>
<summary>io.deephaven.api.updateby.UpdateByOperation.*</summary>

- [io.deephaven.api.updateby.UpdateByOperation.\*](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html)

This class defines operations for use in [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md) operations. The following methods are available:

- CumMax
- CumMin
- CumProd
- CumSum
- Delta
- Ema
- EmMax
- EmMin
- Ems
- EmStd
- Fill
- of
- RollingAvg
- RollingCount
- RollingGroup
- RollingMax
- RollingMin
- RollingProduct
- RollingStd
- RollingSum
- RollingWAvg

</details>

<details>
<summary>io.deephaven.api.updateby.UpdateByControl</summary>

- [io.deephaven.api.updateby.UpdateByControl](/core/javadoc/io/deephaven/api/updateby/UpdateByControl.html)

This interface contains methods for controlling the behavior of [`updateBy`](../table-operations/update-by-operations/updateBy.md) operations. The following methods are available:

- chunkCapacity
- chunkCapacityOrDefault
- initialHashTableSize
- initialHashTableSizeOrDefault
- materialize
- mathContext
- mathContextOrDefault
- maximumLoadFactor
- maximumLoadFactorOrDefault
- maxStaticSparseMemoryOverhead
- maxStaticSparseMemoryOverheadOrDefault
- targetLoadFactor
- targetLoadFactorOrDefault
- useRedirection
- useRedirectionOrDefault

</details>

<details>
<summary>io.deephaven.api.updateby.OperationControl</summary>

- [io.deephaven.api.updateby.OperationControl](/core/javadoc/io/deephaven/api/updateby/OperationControl.html)

This class contains methods for controlling the behavior of [`updateBy`](../table-operations/update-by-operations/updateBy.md) operations. The following methods are available:

- bigValueContext
- bigValueContextOrDefault
- materialize
- onNanValue
- onNanValueOrDefault
- onNegativeDeltaTime
- onNullTime
- onNullValue
- onNullValueOrDefault
- onZeroDeltaTime

</details>

<details>
<summary>io.deephaven.api.updateby.DeltaControl</summary>

- [io.deephaven.api.updateby.DeltaControl](/core/javadoc/io/deephaven/api/updateby/DeltaControl.html)

This class contains methods for controlling the behavior of a [`Delta updateBy operation`](https://deephaven.io/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html#Delta(java.lang.String...)). The following methods are available:

- nullBehavior

As well as the following constants:

- DeltaControl.DEFAULT
- DeltaControl.NULLS_DOMINATES
- DeltaControl.VALUE_DOMINATES
- DeltaControl.ZERO_DOMINATES

</details>

<details>
<summary>io.deephaven.api.updateby.BadDataBehavior</summary>

- [io.deephaven.api.updateby.BadDataBehavior](/core/javadoc/io/deephaven/api/updateby/BadDataBehavior.html)

This class provides methods for handling bad data while processing EMAs:

- valueOf
- values

And the following constants:

- POISON
- RESET
- SKIP
- THROW

</details>

## Related Documentation

- [Backend Imports](./backend-imports.md)
- [Query Language Imports](../query-language/query-library/query-language-function-reference.md)
