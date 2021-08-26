/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class StringUtils {
    public static String pad(String strText, int iMinimumLength) {
        StringBuffer strBuffer;

        if (strText == null)
            strText = "";

        if (iMinimumLength > 4 && strText.length() >= iMinimumLength)
            strBuffer = new StringBuffer(strText.substring(0, iMinimumLength - 3) + "...");
        else
            strBuffer = new StringBuffer(strText);

        for (int length = strText.length(); length < iMinimumLength; ++length) {
            strBuffer.append(' ');
        }

        return strBuffer.toString();
    }

    public static String makeFirstLetterCapital(String s) {
        if (s == null) {
            return null;
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    public static String makeFirstLetterLower(String s) {
        if (s == null) {
            return null;
        }
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }

    /**
     * As above except it handles things like ETFName -> etfName
     */
    public static String makeFirstLettersLower(String s) {
        if (s == null) {
            return null;
        }
        for (int i = 0; i < s.length(); i++) {
            if (Character.isLowerCase(s.charAt(i))) {
                final int index = Math.max(1, i - 1);
                return s.substring(0, index).toLowerCase() + s.substring(index);
            }
        }
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }

    public static String removeLastChar(String s) {
        return s.substring(0, s.length() - 1);
    }

    public static int caseInsensitiveIndexOf(String source, String target) {
        char first = toUpperCase(target.charAt(0));
        int max = (source.length() - target.length());

        for (int i = 0; i <= max; i++) {
            /* Look for first character. */
            if (toUpperCase(source.charAt(i)) != first) {
                while (++i <= max && toUpperCase(source.charAt(i)) != first);
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + target.length() - 1;
                for (int k = 1; j < end && toUpperCase(source.charAt(j)) == toUpperCase(target.charAt(k)); j++, k++);

                if (j == end) {
                    /* Found whole string. */
                    return i;
                }
            }
        }

        return -1;
    }

    public static char toUpperCase(char c) {
        if (c >= 97 && c <= 122) {
            c -= 32;
        }

        return c;
    }

    public static String joinStrings(final String[] strs, final String joinStr) {
        if (strs.length == 0)
            return "";
        StringBuilder builder = new StringBuilder(strs[0]);
        for (int i = 1; i < strs.length; i++)
            builder.append(joinStr).append(strs[i]);
        return builder.toString();
    }

    public static <T> String joinStrings(final Iterator<T> it, final String joinStr) {
        if (!it.hasNext())
            return "";
        StringBuilder builder = new StringBuilder();
        builder.append(it.next());
        while (it.hasNext()) {
            builder.append(joinStr).append(it.next());
        }
        return builder.toString();
    }

    public static <T> String joinStrings(final Stream<T> strs, final String joinStr) {
        Iterator<T> iterator = strs.iterator();
        return joinStrings(iterator, joinStr);
    }

    public static <T> String joinStrings(final Collection<T> strs, final String joinStr) {
        return joinStrings(strs.iterator(), joinStr);
    }

    public static String joinStrings(final int[] ints, final String joinStr) {
        if (ints.length == 0)
            return "";
        StringBuilder builder = new StringBuilder(ints[0] + "");
        for (int i = 1; i < ints.length; i++)
            builder.append(joinStr).append(ints[i]);
        return builder.toString();
    }

    public static String joinStrings(final double[] doubles, final String joinStr) {
        if (doubles.length == 0)
            return "";
        StringBuilder builder = new StringBuilder(doubles[0] + "");
        for (int i = 1; i < doubles.length; i++)
            builder.append(joinStr).append(doubles[i]);
        return builder.toString();
    }

    public static <T> String joinStrings(final T[] objs, final String joinStr) {
        if (objs.length == 0)
            return "";
        StringBuilder builder = new StringBuilder(objs[0] + "");
        for (int i = 1; i < objs.length; i++)
            builder.append(joinStr).append(objs[i]);
        return builder.toString();
    }

    public static String joinStrings(final long[] longs, final String joinStr) {
        if (longs.length == 0)
            return "";
        StringBuilder builder = new StringBuilder(longs[0] + "");
        for (int i = 1; i < longs.length; i++)
            builder.append(joinStr).append(longs[i]);
        return builder.toString();
    }

    public static String pad(String s, int length, char padChar) {
        if (length <= s.length()) {
            return s;
        }

        StringBuilder buf = new StringBuilder();

        for (int i = s.length(); i < length; i++) {
            buf.append(padChar);
        }

        buf.append(s);

        return buf.toString();
    }
}
