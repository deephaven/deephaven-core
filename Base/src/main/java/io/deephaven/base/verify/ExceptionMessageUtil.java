/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.verify;

/**
 * String utility methods related to assertions.
 * <UL>
 * <LI>(package) String failureMessage(String assertionType, String preamble, String message, String
 * detailMessage)
 * <LI>(package) String valueString(Object)
 * <LI>(public) String valueAndName(value0, String name0, value1, String name1, ... )
 * <LI>(public) String concat(String valueAndName0, String valueAndName1, ... )
 * </UL>
 */
public final class ExceptionMessageUtil {

    // ################################################################
    // failureMessage

    // ----------------------------------------------------------------
    /** Return message string for assertion failure. */
    public static String failureMessage(
        String assertionType, // e.g., "Assertion" or "Requirement"
        String assertedText, // e.g., "asserted" or "required"
        String conditionText,
        String detailMessage) {
        String resultMessage = assertionType + " failed";
        if (conditionText != null && conditionText.length() > 0) {
            resultMessage += ": " + assertedText + " " + conditionText;
            if (detailMessage != null && detailMessage.length() > 0) {
                resultMessage += ", instead " + detailMessage;
            }
        }
        return resultMessage + ".";
    }

    // ################################################################
    // valueString

    // ----------------------------------------------------------------
    /** Return the textual representation of an Object's value. */
    public static String valueString(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof String) {
            return "\"" + o + "\"";
        } else if (o instanceof Character) {
            // noinspection UnnecessaryUnboxing
            return valueString(((Character) o).charValue());
        } else {
            return o.toString();
        }
    }

    // ----------------------------------------------------------------
    /** Return the quoted textual representation of char value. */
    public static String valueString(char c) {
        return "\'" + c + "\'";
    }

    // ----------------------------------------------------------------
    /**
     * Builds a string that consists of <i>count</i> copies of the character <i>c</i>.
     * 
     * @param c character to use repeatedly
     * @param count number of times to repeat <i>c</i>
     * @return new string filled with the character <i>c</i>
     */
    public static String fillChar(char c, int count) {
        Require.gtZero(count, "count");
        char[] buffer = new char[count];
        // note: this is faster than System.arraycopy unless count>~100
        for (int nIndex = 0; nIndex < count; nIndex++) {
            buffer[nIndex] = c;
        }
        return new String(buffer);
    }

    // ################################################################
    // valueAndName

    // ----------------------------------------------------------------
    /** Return the values and names of one or more Objects. */
    public static String valueAndName(
        Object o0, String name0) {
        return name0 + " == " + valueString(o0);
    }

    public static String valueAndName(
        Object o0, String name0,
        Object o1, String name1) {
        return concat(
            valueAndName(o0, name0),
            valueAndName(o1, name1));
    }

    public static String valueAndName(
        Object o0, String name0,
        Object o1, String name1,
        Object o2, String name2) {
        return concat(
            valueAndName(o0, name0),
            valueAndName(o1, name1),
            valueAndName(o2, name2));
    }

    public static String valueAndName(
        Object o0, String name0,
        Object o1, String name1,
        Object o2, String name2,
        Object o3, String name3) {
        return concat(
            valueAndName(o0, name0),
            valueAndName(o1, name1),
            valueAndName(o2, name2),
            valueAndName(o3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more booleans. */
    public static String valueAndName(
        boolean b0, String name0) {
        return name0 + " == " + b0;
    }

    public static String valueAndName(
        boolean b0, String name0,
        boolean b1, String name1) {
        return concat(
            valueAndName(b0, name0),
            valueAndName(b1, name1));
    }

    public static String valueAndName(
        boolean b0, String name0,
        boolean b1, String name1,
        boolean b2, String name2) {
        return concat(
            valueAndName(b0, name0),
            valueAndName(b1, name1),
            valueAndName(b2, name2));
    }

    public static String valueAndName(
        boolean b0, String name0,
        boolean b1, String name1,
        boolean b2, String name2,
        boolean b3, String name3) {
        return concat(
            valueAndName(b0, name0),
            valueAndName(b1, name1),
            valueAndName(b2, name2),
            valueAndName(b3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more chars */
    public static String valueAndName(
        char c0, String name0) {
        return name0 + " == " + valueString(c0);
    }

    public static String valueAndName(
        char c0, String name0,
        char c1, String name1) {
        return concat(
            valueAndName(c0, name0),
            valueAndName(c1, name1));
    }

    public static String valueAndName(
        char c0, String name0,
        char c1, String name1,
        char c2, String name2) {
        return concat(
            valueAndName(c0, name0),
            valueAndName(c1, name1),
            valueAndName(c2, name2));
    }

    public static String valueAndName(
        char c0, String name0,
        char c1, String name1,
        char c2, String name2,
        char c3, String name3) {
        return concat(
            valueAndName(c0, name0),
            valueAndName(c1, name1),
            valueAndName(c2, name2),
            valueAndName(c3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more bytes. */
    public static String valueAndName(
        byte b0, String name0) {
        return name0 + " == " + b0;
    }

    public static String valueAndName(
        byte b0, String name0,
        byte b1, String name1) {
        return concat(
            valueAndName(b0, name0),
            valueAndName(b1, name1));
    }

    public static String valueAndName(
        byte b0, String name0,
        byte b1, String name1,
        byte b2, String name2) {
        return concat(
            valueAndName(b0, name0),
            valueAndName(b1, name1),
            valueAndName(b2, name2));
    }

    public static String valueAndName(
        byte b0, String name0,
        byte b1, String name1,
        byte b2, String name2,
        byte b3, String name3) {
        return concat(
            valueAndName(b0, name0),
            valueAndName(b1, name1),
            valueAndName(b2, name2),
            valueAndName(b3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more shorts. */
    public static String valueAndName(
        short s0, String name0) {
        return name0 + " == " + s0;
    }

    public static String valueAndName(
        short s0, String name0,
        short s1, String name1) {
        return concat(
            valueAndName(s0, name0),
            valueAndName(s1, name1));
    }

    public static String valueAndName(
        short s0, String name0,
        short s1, String name1,
        short s2, String name2) {
        return concat(
            valueAndName(s0, name0),
            valueAndName(s1, name1),
            valueAndName(s2, name2));
    }

    public static String valueAndName(
        short s0, String name0,
        short s1, String name1,
        short s2, String name2,
        short s3, String name3) {
        return concat(
            valueAndName(s0, name0),
            valueAndName(s1, name1),
            valueAndName(s2, name2),
            valueAndName(s3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more ints. */
    public static String valueAndName(
        int i0, String name0) {
        return name0 + " == " + i0;
    }

    public static String valueAndName(
        int i0, String name0,
        int i1, String name1) {
        return concat(
            valueAndName(i0, name0),
            valueAndName(i1, name1));
    }

    public static String valueAndName(
        int i0, String name0,
        int i1, String name1,
        int i2, String name2) {
        return concat(
            valueAndName(i0, name0),
            valueAndName(i1, name1),
            valueAndName(i2, name2));
    }

    public static String valueAndName(
        int i0, String name0,
        int i1, String name1,
        int i2, String name2,
        int i3, String name3) {
        return concat(
            valueAndName(i0, name0),
            valueAndName(i1, name1),
            valueAndName(i2, name2),
            valueAndName(i3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more longs. */
    public static String valueAndName(
        long l0, String name0) {
        return name0 + " == " + l0;
    }

    public static String valueAndName(
        long l0, String name0,
        long l1, String name1) {
        return concat(
            valueAndName(l0, name0),
            valueAndName(l1, name1));
    }

    public static String valueAndName(
        long l0, String name0,
        long l1, String name1,
        long l2, String name2) {
        return concat(
            valueAndName(l0, name0),
            valueAndName(l1, name1),
            valueAndName(l2, name2));
    }

    public static String valueAndName(
        long l0, String name0,
        long l1, String name1,
        long l2, String name2,
        long l3, String name3) {
        return concat(
            valueAndName(l0, name0),
            valueAndName(l1, name1),
            valueAndName(l2, name2),
            valueAndName(l3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more floats. */
    public static String valueAndName(
        float f0, String name0) {
        return name0 + " == " + f0;
    }

    public static String valueAndName(
        float f0, String name0,
        float f1, String name1) {
        return concat(
            valueAndName(f0, name0),
            valueAndName(f1, name1));
    }

    public static String valueAndName(
        float f0, String name0,
        float f1, String name1,
        float f2, String name2) {
        return concat(
            valueAndName(f0, name0),
            valueAndName(f1, name1),
            valueAndName(f2, name2));
    }

    public static String valueAndName(
        float f0, String name0,
        float f1, String name1,
        float f2, String name2,
        float f3, String name3) {
        return concat(
            valueAndName(f0, name0),
            valueAndName(f1, name1),
            valueAndName(f2, name2),
            valueAndName(f3, name3));
    }

    // ----------------------------------------------------------------
    /** Return the values and names of one or more doubles. */
    public static String valueAndName(
        double d0, String name0) {
        return name0 + " == " + d0;
    }

    public static String valueAndName(
        double d0, String name0,
        double d1, String name1) {
        return concat(
            valueAndName(d0, name0),
            valueAndName(d1, name1));
    }

    public static String valueAndName(
        double d0, String name0,
        double d1, String name1,
        double d2, String name2) {
        return concat(
            valueAndName(d0, name0),
            valueAndName(d1, name1),
            valueAndName(d2, name2));
    }

    public static String valueAndName(
        double d0, String name0,
        double d1, String name1,
        double d2, String name2,
        double d3, String name3) {
        return concat(
            valueAndName(d0, name0),
            valueAndName(d1, name1),
            valueAndName(d2, name2),
            valueAndName(d3, name3));
    }

    // ################################################################
    // concat

    // ----------------------------------------------------------------
    /** Return the concatenation of a list of valueAndName strings. */
    public static String concat(
        String valueAndName0,
        String valueAndName1) {
        return valueAndName0 + ", " +
            valueAndName1;
    }

    public static String concat(
        String valueAndName0,
        String valueAndName1,
        String valueAndName2) {
        return valueAndName0 + ", " +
            valueAndName1 + ", " +
            valueAndName2;
    }

    public static String concat(
        String valueAndName0,
        String valueAndName1,
        String valueAndName2,
        String valueAndName3) {
        return valueAndName0 + ", " +
            valueAndName1 + ", " +
            valueAndName2 + ", " +
            valueAndName3;
    }

}
