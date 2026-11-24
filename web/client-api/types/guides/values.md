---
title: Values
---

# Values

A JavaScript [`Number`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number) is a
64-bit float, exact only for integers up to 2^53, and a JavaScript
[`Date`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date) keeps only
milliseconds. Column types that do not fit arrive wrapped, so no precision is lost on the way to the client; a null cell
is `null`.

The API does not use JavaScript's
[`BigInt`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt) at this time.

| Column type | Value in JavaScript |
|---|---|
| `int`, `short`, `byte`, `float`, `double` | `number` |
| `boolean` | `boolean` |
| `String` | `string` |
| `long` | {@link dh.LongWrapper} |
| `Instant`, `ZonedDateTime` | {@link dh.DateWrapper} |
| `LocalDate` | {@link dh.LocalDateWrapper} |
| `LocalTime` | {@link dh.LocalTimeWrapper} |
| `BigDecimal` | {@link dh.BigDecimalWrapper} |
| `BigInteger` | {@link dh.BigIntegerWrapper} |

{@includeCode ../examples/use-cases.ts#readValues}

## 64-bit integers

{@link dh.LongWrapper.toString | toString} gives the exact value, and {@link dh.LongWrapper.asNumber | asNumber} the
nearest `number`, which differs past 2^53. {@link dh.LongWrapper.ofString | LongWrapper.ofString} makes one to pass
back, wherever a row position or a `long` value is taken.

## Dates and times

A {@link dh.DateWrapper} holds an instant in nanoseconds; {@link dh.DateWrapper.asDate | asDate} converts it to a
`Date`, keeping milliseconds, and {@link dh.DateWrapper.ofJsDate | DateWrapper.ofJsDate} goes the other way. A
{@link dh.LocalDateWrapper} gives its year, month and day, and a {@link dh.LocalTimeWrapper} its hour, minute, second
and nanosecond, with no time zone.

## Big numbers

{@link dh.BigDecimalWrapper} and {@link dh.BigIntegerWrapper} give their exact value as a string with `toString`, and an
approximate `number` with `asNumber`.

## Formatting

The `dh.i18n` classes format and parse values with the same Java-style patterns the Deephaven server uses, so a UI can
show values the way the server would. Each has a static `format` and `parse` taking the pattern, and `getFormat`, which
returns a format for one pattern, cached for reuse.

{@link dh.i18n.DateTimeFormat.format | DateTimeFormat.format} takes a {@link dh.DateWrapper} or {@link dh.LongWrapper}
of nanoseconds, a `Date`, or a number or string of nanoseconds, and formats it in an optional
{@link dh.i18n.TimeZone | TimeZone}, down to the nanosecond. {@link dh.i18n.DateTimeFormat.parse | DateTimeFormat.parse}
reads text back into a {@link dh.DateWrapper}.

{@link dh.i18n.NumberFormat.format | NumberFormat.format} takes a `number`, a {@link dh.LongWrapper}, a
{@link dh.BigDecimalWrapper} or a {@link dh.BigIntegerWrapper}. {@link dh.i18n.NumberFormat.parse | NumberFormat.parse}
reads text back into a `number`.

{@link dh.i18n.TimeZone.getTimeZone | TimeZone.getTimeZone} takes one of the codes listed on that page, such as
`America/New_York`.

{@link dh.LocalDateWrapper} and {@link dh.LocalTimeWrapper} are not accepted by either class; their `toString` gives
`yyyy-MM-dd` and `HH:mm:ss.nnnnnnnnn` text, and their getters give each field to format another way.

## Arrays

Array columns can hold far more than a display needs. The {@link dh.DataOptions.previewOptions | previewOptions} of a
snapshot or subscription limit what the server sends: {@link dh.PreviewOptions.array | array} sends at most that many
elements of each array, plus one more when there are more, so the client can show that the array continues.

{@includeCode ../examples/use-cases.ts#previewArrays}
