package io.deephaven.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A simple wrapper for string-to-array parsing. Parsers are kept in a cache per-delimiter so that we avoid recompiling
 * the pattern regex.
 */
public class ArrayParser {

    private static final Map<String, ArrayParser> parserMap = new HashMap<>();

    private final String delimiter;
    private final Pattern pattern;

    private ArrayParser(String delimiter) {
        this.delimiter = delimiter;
        this.pattern = Pattern.compile(Pattern.quote(delimiter));
    }

    public static ArrayParser getInstance(String delimiter) {
        return parserMap.computeIfAbsent(delimiter, ArrayParser::new);
    }

    private void checkFormat(String value) {
        if (value.length() < 2) {
            throw new InputMismatchException("Value submitted for Array parsing too short.");
        }
        final char start = value.charAt(0);
        if (start != '[' && start != '{' && start != '(') {
            throw new InputMismatchException("Value submitted for Array parsing doesn't match needed format, " +
                    "unexpected opening character: " + start);
        }
        final char end = value.charAt(value.length() - 1);
        final char expectedEnd;
        switch (start) {
            case '[':
                expectedEnd = ']';
                break;
            case '{':
                expectedEnd = '}';
                break;
            case '(':
                expectedEnd = ')';
                break;
            default:
                throw new IllegalStateException("Internal error");
        }
        if (end != expectedEnd) {
            throw new InputMismatchException("Missing expected closing character: " + expectedEnd);
        }
    }

    /**
     * Parse the given string as an array of doubles, based upon the delimiter.
     *
     * @param value string to parse
     * @param strict enforce format strictly
     * @return array of parsed values
     */
    public double[] getDoubleArray(String value, boolean strict) {
        return getArray(value, strict, s -> s.mapToDouble(Double::parseDouble).toArray());
    }

    /**
     * Parse the given string as an array of longs, based upon the delimiter.
     *
     * @param value string the to parse
     * @param strict enforce format strictly
     * @return array of parsed values
     */
    public long[] getLongArray(String value, boolean strict) {
        return getArray(value, strict, s -> s.mapToLong(Long::parseLong).toArray());
    }

    /**
     * Create a properly typed array from the input string based upon the delimiter, given a supplier.
     *
     * @param value The array string value
     * @param strict if strict processing should be used
     * @param elementSupplier a supplier to convert a stream of element strings to items of the correct types
     * @param <T> the type
     * @return an array of values of the specified type
     */
    public <T> T getArray(String value, boolean strict, Function<Stream<String>, T> elementSupplier) {
        return elementSupplier.apply(toStringStream(value, strict));
    }

    /**
     * Convert the input string value to a stream of strings for each element based upon the delimiter.
     *
     * @param value the array as a string
     * @param strict if strict processing should be used
     * @return a stream of strings for each element of the array
     */
    private Stream<String> toStringStream(String value, boolean strict) {
        value = value.trim();
        if (strict) {
            checkFormat(value);
        }

        if (value.length() <= 2) {
            return Stream.empty();
        }

        try {
            return Arrays.stream(pattern.split(value.trim().substring(1, value.length() - 1))).map(String::trim);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Value submitted for Array parsing doesn't match needed format: " + value, e);
        }
    }

    /**
     * Split the input string into an array of strings based on the delimiter.
     *
     * @param value the array as a string
     * @param strict if strict processing should be used
     * @return an array of strings
     */
    public String[] toStringArray(String value, boolean strict) {
        return toStringStream(value, strict).toArray(String[]::new);
    }

    /**
     * Convert the given array to a String.
     *
     * @param array the array
     * @return array encoded as string
     */
    public String encodeArray(double[] array) {
        return array == null
                ? null
                : "[" + Arrays.stream(array).mapToObj(Double::toString).collect(Collectors.joining(delimiter)) + "]";
    }

    /**
     * Convert the given array to a String.
     *
     * @param array the array
     * @return array encoded as string
     */
    public String encodeArray(long[] array) {
        return array == null
                ? null
                : "[" + Arrays.stream(array).mapToObj(Long::toString).collect(Collectors.joining(delimiter)) + "]";
    }
}
