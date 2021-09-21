package io.deephaven.db.tables.utils.csv;

import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.qst.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
        public ParserException(String s) {
            super(s);
        }

        public ParserException(Throwable cause) {
            super(cause);
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
     * A parser that delegates to {@link Float#parseFloat(String)}.
     */
    public static final Parser<Float> FLOAT = new Parser<>(Type.floatType(), Float::parseFloat);

    /**
     * A parser that delegates to {@link Double#parseDouble(String)}.
     */
    public static final Parser<Double> DOUBLE = new Parser<>(Type.doubleType(), Double::parseDouble);

    /**
     * A parser that delegates to {@link Instant#parse(CharSequence)}.
     */
    public static final Parser<Instant> INSTANT = new Parser<>(Type.instantType(), Instant::parse);

    /**
     * A parser that delegates to {@link DBTimeUtils#convertDateTime(String)}.
     */
    public static final Parser<Instant> INSTANT_DB = new Parser<>(Type.instantType(), Parser::parseAsDbDateFormat);

    /**
     * A naive parser, which returns the same string value it was passed in.
     */
    public static final Parser<String> STRING = new Parser<>(Type.stringType(), Function.identity());

    /**
     * A parser that will parse long values as epoch milliseconds.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer, may be null
     * @param max the maximum instant to infer, may be null
     * @return the epoch milli parser
     *
     * @see #epochMicroParser(Parser, Instant, Instant)
     * @see #epochMilliAndMicroParsers(Parser, Instant, Instant)
     */
    public static Parser<Instant> epochMilliParser(Parser<Long> longParser, Instant min, Instant max) {
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
     * @see #epochMilliParser(Parser, Instant, Instant)
     * @see #epochMilliAndMicroParsers(Parser, Instant, Instant)
     */
    public static Parser<Instant> epochMicroParser(Parser<Long> longParser, Instant min, Instant max) {
        return new Parser<>(Type.instantType(), s -> parseAsEpochMicros(longParser, min, max, s));
    }

    /**
     * Returns two parsers that will parse long values as epoch milliseconds, or epoch microseconds, based on
     * non-overlapping min/max ranges.
     *
     * <p>
     * The max instant as epoch millis must be less than the min instant as epoch micros.
     *
     * @param longParser the long parser
     * @param min the minimum instant to infer
     * @param max the maximum instant to infer
     * @return the epoch milli and micro parsers
     *
     * @see #epochMilliAndMicroParsers21stCentury(Parser)
     */
    public static List<Parser<Instant>> epochMilliAndMicroParsers(Parser<Long> longParser, Instant min, Instant max) {
        final long minMillis = min.toEpochMilli();
        final long maxMillis = max.toEpochMilli();
        final long minMicros = minMillis * 1_000L;
        if (maxMillis >= minMicros) {
            throw new IllegalArgumentException("Unable to do proper inference on millis/micros, overlapping range");
        }
        return Arrays.asList(epochMilliParser(longParser, min, max), epochMicroParser(longParser, min, max));
    }

    /**
     * Returns two parser that will parse long values as epoch milliseconds, or epoch microseconds, from the 21st
     * century.
     *
     * @param longParser the long parser
     * @return the 21st century epoch milli and micro parsers
     */
    public static List<Parser<Instant>> epochMilliAndMicroParsers21stCentury(Parser<Long> longParser) {
        final Instant min = LocalDate.ofYearDay(2000, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
        final Instant max = LocalDate.ofYearDay(2100, 1).atStartOfDay().toInstant(ZoneOffset.UTC).minusNanos(1);
        return epochMilliAndMicroParsers(longParser, min, max);
    }

    /**
     * Combines multiple parsers together, whereby each parser is tried in-order. The first parsed value will be
     * returned. If none of the parsers are able to parse the string, the first parser's exception will be rethrown, and
     * all of the subsequent parsers' exceptions will be added as suppressed exceptions.
     *
     * @param parsers the parsers
     * @param <T> the parsed type
     * @return the merged parser
     */
    public static <T> Parser<T> merge(List<Parser<T>> parsers) {
        if (parsers.isEmpty()) {
            throw new IllegalArgumentException();
        }
        if (parsers.size() == 1) {
            return parsers.get(0);
        }
        final List<Parser<T>> out = new ArrayList<>();
        for (Parser<T> parser : parsers) {
            extract(parser, out);
        }
        return new Parser<>(parsers.get(0).type(), new Multi<>(out));
    }

    private static <T> void extract(Parser<T> parser, List<Parser<T>> out) {
        if (parser.function instanceof Parser.Multi) {
            out.addAll(((Multi<T>) parser.function).parsers);
        } else {
            out.add(parser);
        }
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
        final T out;
        try {
            out = function.apply(value);
        } catch (RuntimeException t) {
            if (t instanceof ParserException) {
                throw t;
            }
            throw new ParserException(t);
        }
        if (out == null) {
            throw new IllegalStateException(
                    "Parser function returned a null value - parsers should throw an appropriate exception instead");
        }
        return out;
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
    public boolean isParsable(String value) {
        if (value == null) {
            return true;
        }
        final T out;
        try {
            out = function.apply(value);
        } catch (RuntimeException t) {
            return false;
        }
        if (out == null) {
            throw new IllegalStateException(
                    "Parser function returned a null value - parsers should throw an appropriate exception instead");
        }
        return true;
    }

    /**
     * Merges {@code this} parser with {@code other}.
     *
     * @param other the other parser
     * @return the merged parser
     * @see #merge(List)
     */
    public Parser<T> orElse(Parser<T> other) {
        return merge(Arrays.asList(this, other));
    }

    private static class Multi<T> implements Function<String, T> {

        private final List<Parser<T>> parsers;

        public Multi(List<Parser<T>> parsers) {
            this.parsers = Objects.requireNonNull(parsers);
            if (parsers.size() < 2) {
                throw new IllegalArgumentException("Must have at least two parsers");
            }
            final Type<T> type = parsers.get(0).type();
            for (Parser<T> parser : parsers) {
                if (!type.equals(parser.type())) {
                    throw new IllegalArgumentException("Must have equal types");
                }
            }
        }

        @Override
        public T apply(String s) {
            final List<ParserException> exceptions = new ArrayList<>();
            for (Parser<T> parser : parsers) {
                try {
                    return parser.parse(s);
                } catch (ParserException e) {
                    exceptions.add(e);
                }
            }
            final Iterator<ParserException> it = exceptions.iterator();
            final ParserException first = it.next();
            while (it.hasNext()) {
                first.addSuppressed(it.next());
            }
            throw first;
        }
    }

    private static boolean parseBool(String value) {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new ParserException("Value is not a boolean");
    }

    private static char parseChar(String value) {
        if (value.length() != 1) {
            throw new ParserException("Value is not a char");
        }
        return value.charAt(0);
    }

    private static Instant parseAsDbDateFormat(String value) {
        return DBTimeUtils.convertDateTime(value).getInstant();
    }

    private static Instant parseAsEpochMillis(Parser<Long> longParser, Instant minInstant, Instant maxInstant,
            String value) {
        final long epochMilli = longParser.parse(value);
        final Instant instant = Instant.ofEpochMilli(epochMilli);
        if (minInstant != null && instant.isBefore(minInstant)) {
            throw new ParserException("Long millis is less than min instant");
        }
        if (maxInstant != null && instant.isAfter(maxInstant)) {
            throw new ParserException("Long millis is greater than max instant");
        }
        return instant;
    }

    private static Instant parseAsEpochMicros(Parser<Long> longParser, Instant minInstant, Instant maxInstant,
            String value) {
        final long epochMicro = longParser.parse(value);
        final long epochSecond = Math.floorDiv(epochMicro, 1_000_000);
        final int nanoAdj = (int) Math.floorMod(epochMicro, 1_000_000);
        final Instant instant = Instant.ofEpochSecond(epochSecond, nanoAdj);
        if (minInstant != null && instant.isBefore(minInstant)) {
            throw new ParserException("Long micros is less than min instant");
        }
        if (maxInstant != null && instant.isAfter(maxInstant)) {
            throw new ParserException("Long micros is greater than max instant");
        }
        return instant;
    }
}
