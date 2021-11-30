package io.deephaven.csv;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.qst.type.Type;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A parser is responsible for parsing strings into parsed types.
 *
 * @param <T> the parsed type
 */
public class Parser<T> {

    /**
     * A parser exception.
     */
    public static class ParserException extends IllegalArgumentException {
        private final String value;

        public ParserException(String value, String message) {
            super(message);
            this.value = value;
        }

        public ParserException(String value, Throwable cause) {
            super(cause);
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    /**
     * A parser that maps the case-insensitive string "true" to {@code true}, and "false" to {@code false}.
     */
    public static final Parser<Boolean> BOOL = new Parser<>(Type.booleanType(), Parser::parseBool);

    /**
     * A parser that delegates to {@link Byte#parseByte(String)}.
     */
    public static final Parser<Byte> BYTE = new Parser<>(Type.byteType(), Byte::parseByte);

    /**
     * A parses that returns the first character of the string if there is exactly one character in the string.
     */
    public static final Parser<Character> CHAR = new Parser<>(Type.charType(), Parser::parseChar);

    /**
     * A parser that delegates to {@link Short#parseShort(String)}.
     */
    public static final Parser<Short> SHORT = new Parser<>(Type.shortType(), Short::parseShort);

    /**
     * A parser that delegates to {@link Integer#parseInt(String)}.
     */
    public static final Parser<Integer> INT = new Parser<>(Type.intType(), Integer::parseInt);

    /**
     * A parser that delegates to {@link Long#parseLong(String)}.
     */
    public static final Parser<Long> LONG = new Parser<>(Type.longType(), Long::parseLong);

    /**
     * A parser that delegates non-trimmable strings to {@link Float#parseFloat(String)}.
     *
     * <p>
     * Note: if the string is trimmable, the parsing fails. This is to remain consistent with the parsing of integral
     * values.
     */
    public static final Parser<Float> FLOAT = new Parser<>(Type.floatType(), Parser::parseFloat);

    /**
     * A parser that delegates non-trimmable strings to {@link Double#parseDouble(String)}.
     *
     * <p>
     * Note: if the string is trimmable, the parsing fails. This is to remain consistent with the parsing of integral
     * values.
     */
    public static final Parser<Double> DOUBLE = new Parser<>(Type.doubleType(), Parser::parseDouble);

    /**
     * A parser that delegates to {@link Instant#parse(CharSequence)}.
     */
    public static final Parser<Instant> INSTANT = new Parser<>(Type.instantType(), Instant::parse);

    /**
     * A parser that delegates to {@link DateTimeUtils#convertDateTime(String)}.
     */
    public static final Parser<Instant> INSTANT_LEGACY = new Parser<>(Type.instantType(), Parser::parseAsDateFormat);

    /**
     * A naive parser, which returns the same string value it was passed in.
     */
    public static final Parser<String> STRING = new Parser<>(Type.stringType(), Function.identity());

    /**
     * A parser that will parse long values as epoch seconds.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer, may be null
     * @param max the maximum instant to infer, may be null
     * @return the epoch second parser
     *
     * @see #epochAnyParser(Parser, Instant, Instant)
     */
    public static Parser<Instant> epochSecondParser(Parser<Long> longParser, Instant min, Instant max) {
        if (min != null && max != null && min.isAfter(max)) {
            throw new IllegalArgumentException(String.format("min is greater that max: %s > %s", min, max));
        }
        return new Parser<>(Type.instantType(), s -> parseAsEpochSeconds(longParser, min, max, s));
    }

    /**
     * A parser that will parse long values as epoch milliseconds.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer, may be null
     * @param max the maximum instant to infer, may be null
     * @return the epoch milli parser
     *
     * @see #epochAnyParser(Parser, Instant, Instant)
     */
    public static Parser<Instant> epochMilliParser(Parser<Long> longParser, Instant min, Instant max) {
        if (min != null && max != null && min.isAfter(max)) {
            throw new IllegalArgumentException(String.format("min is greater that max: %s > %s", min, max));
        }
        return new Parser<>(Type.instantType(), s -> parseAsEpochMillis(longParser, min, max, s));
    }

    /**
     * A parser that will parse long values as epoch microseconds.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer, may be null
     * @param max the maximum instant to infer, may be null
     * @return the epoch micro parser
     *
     * @see #epochAnyParser(Parser, Instant, Instant)
     */
    public static Parser<Instant> epochMicroParser(Parser<Long> longParser, Instant min, Instant max) {
        if (min != null && max != null && min.isAfter(max)) {
            throw new IllegalArgumentException(String.format("min is greater that max: %s > %s", min, max));
        }
        return new Parser<>(Type.instantType(), s -> parseAsEpochMicros(longParser, min, max, s));
    }

    /**
     * A parser that will parse long values as epoch nanoseconds.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer, may be null
     * @param max the maximum instant to infer, may be null
     * @return the epoch nano parser
     *
     * @see #epochAnyParser(Parser, Instant, Instant)
     */
    public static Parser<Instant> epochNanoParser(Parser<Long> longParser, Instant min, Instant max) {
        if (min != null && max != null && min.isAfter(max)) {
            throw new IllegalArgumentException(String.format("min is greater that max: %s > %s", min, max));
        }
        return new Parser<>(Type.instantType(), s -> parseAsEpochNanos(longParser, min, max, s));
    }

    /**
     * Returns four parsers that will parse long values as epoch seconds, milliseconds, epoch microseconds, and epoch
     * nanoseconds based on non-overlapping min/max ranges.
     *
     * <p>
     * Note: the duration between the epoch and the max must be less than 1000 times the duration between the epoch and
     * the min.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer
     * @param max the maximum instant to infer
     * @return the epoch milli and micro parsers
     *
     * @see #epochSecondParser(Parser, Instant, Instant)
     * @see #epochMilliParser(Parser, Instant, Instant)
     * @see #epochMicroParser(Parser, Instant, Instant)
     * @see #epochNanoParser(Parser, Instant, Instant)
     * @see #epochAny21stCentury(Parser)
     */
    public static List<Parser<Instant>> epochAnyParser(Parser<Long> longParser, Instant min, Instant max) {
        if (min.isAfter(max)) {
            throw new IllegalArgumentException(String.format("min is greater that max: %s > %s", min, max));
        }
        if (Duration.between(Instant.EPOCH, max)
                .compareTo(Duration.between(Instant.EPOCH, min).multipliedBy(1000)) >= 0) {
            throw new IllegalArgumentException("Unable to do proper inference on instants, has overlapping range");
        }
        return Arrays.asList(
                epochSecondParser(longParser, min, max),
                epochMilliParser(longParser, min, max),
                epochMicroParser(longParser, min, max),
                epochNanoParser(longParser, min, max));
    }

    /**
     * Returns four parser that will parse long values as epoch seconds, epoch milliseconds, epoch microseconds, and
     * epoch nanoseconds from the 21st century.
     *
     * @param longParser the long parser
     * @return the 21st century epoch second, milli, micro, and nanoseconds parsers
     * @see #epochAnyParser(Parser, Instant, Instant)
     */
    public static List<Parser<Instant>> epochAny21stCentury(Parser<Long> longParser) {
        final Instant min = LocalDate.ofYearDay(2000, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
        final Instant max = LocalDate.ofYearDay(2100, 1).atStartOfDay().toInstant(ZoneOffset.UTC).minusNanos(1);
        return epochAnyParser(longParser, min, max);
    }

    private final Type<T> type;
    private final Function<String, T> function;

    /**
     * Creates a parser. The {@code function} is passed non-null strings, and expected to return the parsed value, or
     * throw an appropriate {@link RuntimeException}.
     *
     * @param type the type
     * @param function the function
     */
    public Parser(Type<T> type, Function<String, T> function) {
        this.type = Objects.requireNonNull(type);
        this.function = Objects.requireNonNull(function);
    }

    public Type<T> type() {
        return type;
    }

    /**
     * Parses {@code value} when non-null, otherwise returns null.
     *
     * <p>
     * This method catches {@link RuntimeException} from {@code function} and converts them to {@link ParserException}.
     *
     * @param value the string to parse
     * @return the parsed value, or null
     * @throws ParserException if {@code value} can't be parsed
     */
    public T parse(String value) {
        if (value == null) {
            return null;
        }
        try {
            return function.apply(value);
        } catch (RuntimeException t) {
            if (t instanceof ParserException) {
                throw t;
            }
            throw new ParserException(value, t);
        }
    }

    /**
     * Checks if {@code this} parser can parse {@code value}.
     *
     * <p>
     * {@code null} values are always return true.
     * 
     * @param value the value
     * @return true if the value can be parsed.
     */
    public boolean canParse(String value) {
        if (value == null) {
            return true;
        }
        try {
            function.apply(value);
        } catch (RuntimeException t) {
            return false;
        }
        return true;
    }

    private static boolean parseBool(String value) {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new ParserException(value, "Value is not a boolean");
    }

    private static char parseChar(String value) {
        if (value.length() != 1) {
            throw new ParserException(value, "Value is not a char");
        }
        return value.charAt(0);
    }

    private static float parseFloat(String value) {
        if (isTrimmable(value)) {
            throw new ParserException(value, "Not parsing floats that are trimmable");
        }
        return Float.parseFloat(value);
    }

    private static double parseDouble(String value) {
        if (isTrimmable(value)) {
            throw new ParserException(value, "Not parsing doubles that are trimmable");
        }
        return Double.parseDouble(value);
    }

    private static boolean isTrimmable(String value) {
        return !value.isEmpty() && (value.charAt(0) <= ' ' || value.charAt(value.length() - 1) <= ' ');
    }

    private static Instant parseAsDateFormat(String value) {
        return DateTimeUtils.convertDateTime(value).getInstant();
    }

    private static Instant parseAsEpochSeconds(Parser<Long> longParser, Instant min, Instant max, String value) {
        final long epochSecond = longParser.parse(value);
        final Instant instant = Instant.ofEpochSecond(epochSecond);
        if (min != null && instant.isBefore(min)) {
            throw new ParserException(value, "Long seconds is less than min instant");
        }
        if (max != null && instant.isAfter(max)) {
            throw new ParserException(value, "Long seconds is greater than max instant");
        }
        return instant;
    }

    private static Instant parseAsEpochMillis(Parser<Long> longParser, Instant min, Instant max, String value) {
        final long epochMilli = longParser.parse(value);
        final Instant instant = Instant.ofEpochMilli(epochMilli);
        if (min != null && instant.isBefore(min)) {
            throw new ParserException(value, "Long millis is less than min instant");
        }
        if (max != null && instant.isAfter(max)) {
            throw new ParserException(value, "Long millis is greater than max instant");
        }
        return instant;
    }

    private static Instant parseAsEpochMicros(Parser<Long> longParser, Instant min, Instant max, String value) {
        final long epochMicro = longParser.parse(value);
        final long epochSecond = Math.floorDiv(epochMicro, 1_000_000);
        final int nanoAdj = (int) Math.floorMod(epochMicro, 1_000_000) * 1_000;
        final Instant instant = Instant.ofEpochSecond(epochSecond, nanoAdj);
        if (min != null && instant.isBefore(min)) {
            throw new ParserException(value, "Long micros is less than min instant");
        }
        if (max != null && instant.isAfter(max)) {
            throw new ParserException(value, "Long micros is greater than max instant");
        }
        return instant;
    }

    private static Instant parseAsEpochNanos(Parser<Long> longParser, Instant min, Instant max, String value) {
        final long epochNano = longParser.parse(value);
        final long epochSecond = Math.floorDiv(epochNano, 1_000_000_000);
        final int nanoAdj = (int) Math.floorMod(epochNano, 1_000_000_000);
        final Instant instant = Instant.ofEpochSecond(epochSecond, nanoAdj);
        if (min != null && instant.isBefore(min)) {
            throw new ParserException(value, "Long nanos is less than min instant");
        }
        if (max != null && instant.isAfter(max)) {
            throw new ParserException(value, "Long nanos is greater than max instant");
        }
        return instant;
    }
}
