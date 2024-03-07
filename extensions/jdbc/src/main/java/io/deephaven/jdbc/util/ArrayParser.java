//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc.util;

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

    public static synchronized ArrayParser getInstance(String delimiter) {
        return parserMap.computeIfAbsent(delimiter, ArrayParser::new);
    }

    private String checkFormat(String value, boolean strict) {
        if (value.length() < 2) {
            if (strict) {
                throw new InputMismatchException("Value submitted for Array parsing too short.");
            }
            return value;
        }
        final char startChar = value.charAt(0);
        final int start = "[{(".indexOf(startChar);
        final char endChar = value.charAt(value.length() - 1);
        final int end = "]})".indexOf(endChar);

        if (strict && (start != end || start == -1)) {
            throw new InputMismatchException("Value submitted for Array parsing doesn't match needed format, " +
                    "opening character: " + startChar + " closing character: " + endChar);
        }

        return value.substring(start == -1 ? 0 : 1, value.length() - (end == -1 ? 0 : 1));
    }

    /**
     * Parse the given string as an array of doubles, based upon the delimiter.
     *
     * @param value string to parse
     * @param strict fail if the pattern does not begin / end with: [], {}, or ()
     * @return array of parsed values
     */
    public double[] getDoubleArray(String value, boolean strict) {
        return getArray(value, strict, s -> s.mapToDouble(Double::parseDouble).toArray());
    }

    /**
     * Parse the given string as an array of longs, based upon the delimiter.
     *
     * @param value string the to parse
     * @param strict fail if the pattern does not begin / end with: [], {}, or ()
     * @return array of parsed values
     */
    public long[] getLongArray(String value, boolean strict) {
        return getArray(value, strict, s -> s.mapToLong(Long::parseLong).toArray());
    }

    /**
     * Create a properly typed array from the input string based upon the delimiter, given a supplier.
     *
     * @param value The array string value
     * @param strict fail if the pattern does not begin / end with: [], {}, or ()
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
     * @param strict fail if the pattern does not begin / end with: [], {}, or ()
     * @return a stream of strings for each element of the array
     */
    private Stream<String> toStringStream(String value, boolean strict) {
        value = checkFormat(value.trim(), strict);

        if (value.isEmpty()) {
            return Stream.empty();
        }

        try {
            return Arrays.stream(pattern.split(value)).map(String::trim);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Value submitted for Array parsing doesn't match needed format: " + value, e);
        }
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
