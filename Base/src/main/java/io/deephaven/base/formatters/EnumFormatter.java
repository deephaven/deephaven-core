/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.formatters;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.jetbrains.annotations.NotNull;

import java.text.*;
import java.util.Arrays;

/**
 * This class provides a {@code Format} object that converts from and to a comma-separated list of {@code String} values
 * and their binary masks. (The first string corresponds to the value 1, the second to 2, the third to 4, etc.) Because
 * of the use of values of 2, this conversion can handle bitsets. It is however limited to 31 possible enum values.
 */
public class EnumFormatter extends Format {

    private static ThreadLocal<StringBuilder> formatter = new ThreadLocal<StringBuilder>() {
        protected StringBuilder initialValue() {
            return new StringBuilder(64);
        }
    };

    protected final String[] strings;
    protected final TIntObjectHashMap<String> enumsToString = new TIntObjectHashMap<>();
    protected final TObjectIntHashMap<String> stringToEnums = new TObjectIntHashMap<>();
    protected final String possibleValuesString;

    /**
     * Create a formatter for the sequence of {@code enums}, where the i-th enum in the sequence is associated with the
     * value {@code Math.pow(2,i)} (starting with index 0 and value 1).
     */
    public EnumFormatter(String enums[]) {
        strings = Arrays.copyOf(enums, enums.length);
        for (int i = 0; i < enums.length; i++) {
            // assert(enums[i] != null); // you'd think!
            enumsToString.put((int) Math.pow(2, i), enums[i]);
            stringToEnums.put(enums[i], (int) Math.pow(2, i));
        }
        possibleValuesString = Arrays.toString(enums);
    }

    // for subclasses only
    protected EnumFormatter(String enums[], String possibleValues) {
        strings = Arrays.copyOf(enums, enums.length);
        possibleValuesString = possibleValues;
    }

    /**
     * Return a string representation of possible enum values.
     */
    public String getPossibleValues() {
        return possibleValuesString;
    }

    /**
     * Return a string representation of the enum bitset given by the {@code index}.
     */
    public String format(int index) {
        String singleResult = enumsToString.get(index);
        if (singleResult != null) {
            return singleResult;
        }
        // It's not a single value, so let's enumerate powers of two and create a bitset.
        StringBuilder result = null;
        for (int count = 0; index > 0 && count < strings.length; ++count, index >>>= 1) {
            if ((index & 1) != 0) {
                if (result == null) {
                    result = formatter.get();
                    result.setLength(0);
                } else {
                    result.append(',');
                }
                if (count < strings.length && strings[count] != null) {
                    result.append(strings[count]);
                }
            }
        }
        return result == null ? null : result.toString();
    }

    @Deprecated
    public StringBuffer format(Object obj, @NotNull StringBuffer toAppendTo, @NotNull FieldPosition pos) {
        int num = ((Number) obj).intValue();

        int nullValue = obj instanceof Byte ? Byte.MIN_VALUE : Integer.MIN_VALUE;
        if (num != nullValue) {
            toAppendTo.append(format(num));
        }

        return toAppendTo;
    }

    public Object parseObject(String source, @NotNull ParsePosition pos) {
        return null;
    }

    /**
     * Return a binary bitset representation of the comma-separated string {@code s}, with the i-th bit set for the
     * corresponding occurrence of the i-th enum (and i ranging from 0 to the length of the enums passed at
     * construction).
     * <p/>
     * If an enum in s is not recognized, it is silently ignored and contributes no bit to the result.
     */
    public int parse(String s) {
        if (s.length() < 12 && s.indexOf(',') == -1) {
            return stringToEnums.get(s);
        }

        int result = 0;

        for (String ss : s.split(",")) {
            result |= stringToEnums.get(ss);
        }

        return result;
    }

    /**
     * Return a binary bitset representation of the coma-separate string {@code s}, with the i-th bit set for the
     * corresponding occurrence of the i-th enum (and i ranging from 0 to the length of the enums passed at
     * construction).
     * <p/>
     * If an enum in s is not recognized, an exception is thrown.
     */
    public int parseErrorChecking(String s) throws ParseException {
        if (s.equals("")) {
            return 0;
        }
        if (s.length() < 12 && s.indexOf(',') == -1) {
            int val = stringToEnums.get(s);
            if (val == 0) {
                throw new ParseException(
                        "Unparseable enum: string=" + s + ", token=" + s + ", possibleValues=" + possibleValuesString,
                        0);
            }
            return val;
        }

        int result = 0;

        for (String ss : s.split(",")) {
            int val = stringToEnums.get(ss);

            if (val == 0) {
                throw new ParseException(
                        "Unparseable enum: string=" + s + ", token=" + ss + ", possibleValues=" + possibleValuesString,
                        0);
            }

            result |= val;
        }

        return result;
    }
}
