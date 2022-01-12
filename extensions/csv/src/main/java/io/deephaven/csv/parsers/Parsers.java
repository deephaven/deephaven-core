package io.deephaven.csv.parsers;

import java.util.ArrayList;
import java.util.List;

/**
 * Standard system parsers for the {@link io.deephaven.csv.reading.CsvReader}.
 */
public class Parsers {
    public static final Parser<?> BOOLEAN = BooleanAsByteParser.INSTANCE;
    public static final Parser<?> BYTE = ByteParser.INSTANCE;
    public static final Parser<?> SHORT = ShortParser.INSTANCE;
    public static final Parser<?> INT = IntParser.INSTANCE;
    public static final Parser<?> LONG = LongParser.INSTANCE;
    public static final Parser<?> FLOAT_FAST = FloatFastParser.INSTANCE;
    public static final Parser<?> FLOAT_STRICT = FloatStrictParser.INSTANCE;
    public static final Parser<?> DOUBLE = DoubleParser.INSTANCE;
    public static final Parser<?> DATETIME = DateTimeAsLongParser.INSTANCE;
    public static final Parser<?> CHAR = CharParser.INSTANCE;
    public static final Parser<?> STRING = StringParser.INSTANCE;
    public static final Parser<?> TIMESTAMP_SECONDS = TimestampSecondsParser.INSTANCE;
    public static final Parser<?> TIMESTAMP_MILLIS = TimestampMillisParser.INSTANCE;
    public static final Parser<?> TIMESTAMP_MICROS = TimestampMicrosParser.INSTANCE;
    public static final Parser<?> TIMESTAMP_NANOS = TimestampNanosParser.INSTANCE;

    /**
     * Notably, BYTE, SHORT, and FLOAT are not in the list of standard parsers. The TIMESTAMP_* parsers are never
     * included by default, because they look like ints/longs.
     */
    public static final List<Parser<?>> DEFAULT = List.of(
            BOOLEAN,
            INT,
            LONG,
            DOUBLE,
            DATETIME,
            CHAR,
            STRING);

    /**
     * The above plus BYTE. The TIMESTAMP_* parsers are never included by default, because they look like ints/longs.
     */
    public static final List<Parser<?>> COMPLETE = List.of(
            BOOLEAN,
            BYTE,
            SHORT,
            INT,
            LONG,
            DOUBLE,
            DATETIME,
            CHAR,
            STRING);

    /**
     * Like COMPLETE but with FLOAT_FAST rather than DOUBLE.
     */
    public static final List<Parser<?>> COMPLETE_FLOAT = List.of(
            BOOLEAN,
            BYTE,
            SHORT,
            INT,
            LONG,
            FLOAT_FAST,
            DATETIME,
            CHAR,
            STRING);

    /**
     * Minimal
     */
    public static final List<Parser<?>> MINIMAL = List.of(
            BOOLEAN,
            LONG,
            DOUBLE,
            DATETIME,
            STRING);

    /**
     * Strings only.
     */
    public static final List<Parser<?>> STRINGS = List.of(STRING);

    /**
     * DateTime, Double, Boolean, Char, String, and timestamp (seconds).
     */
    public static final List<Parser<?>> STANDARD_TIMES = someOtherParsersAnd(Parsers.TIMESTAMP_SECONDS);

    /**
     * DateTime, Double, Boolean, Char, String, and timestamp (milliseconds).
     */
    public static final List<Parser<?>> STANDARD_MILLITIMES = someOtherParsersAnd(Parsers.TIMESTAMP_MILLIS);

    /**
     * DateTime, Double, Boolean, Char, String, and timestamp (microseconds).
     */
    public static final List<Parser<?>> STANDARD_MICROTIMES = someOtherParsersAnd(Parsers.TIMESTAMP_MICROS);

    /**
     * DateTime, Double, Boolean, Char, String, and timestamp (nanoseconds).
     */
    public static final List<Parser<?>> STANDARD_NANOTIMES = someOtherParsersAnd(Parsers.TIMESTAMP_NANOS);

    private static List<Parser<?>> someOtherParsersAnd(final Parser<?> oneMore) {
        final List<Parser<?>> result = new ArrayList<>();
        result.add(BOOLEAN);
        result.add(DATETIME);
        result.add(CHAR);
        result.add(STRING);
        result.add(oneMore);
        return List.copyOf(result);
    }
}
