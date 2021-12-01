package io.deephaven.csv.tokenization;

/**
 * Simple range tests that may be faster than the corresponding Java utilities because they are ASCII-specific.
 */
public class RangeTests {
    /**
     * If the character is lowercase ASCII, converts it to uppercase ASCII. Otherwise leaves it alone.
     * 
     * @param ch The character.
     * @return The converted or unchanged character.
     */
    public static char toUpper(char ch) {
        return isLower(ch) ? (char) (ch - 'a' + 'A') : ch;
    }

    /**
     * Is the character uppercase ASCII?
     * 
     * @param ch The character.
     * @return True if the character is uppercase ASCII. False otherwise.
     */
    public static boolean isUpper(char ch) {
        return ch >= 'A' && ch <= 'Z';
    }

    /**
     * Is the character lowercase ASCII?
     * 
     * @param ch The character.
     * @return True if the character is lowercase ASCII. False otherwise.
     */
    public static boolean isLower(char ch) {
        return ch >= 'a' && ch <= 'z';
    }

    /**
     * Is the character an ASCII digit?
     * 
     * @param ch The character.
     * @return True if the character is an ASCII digit. False otherwise.
     */
    public static boolean isDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }

    /**
     * Is the character space or tab?
     *
     * @param ch The character.
     * @return True if the character is space or tab. False otherwise.
     */
    public static boolean isSpaceOrTab(byte ch) {
        return ch == ' ' || ch == '\t';
    }

    /**
     * Is the value in range for a Java byte?
     * 
     * @param value The value.
     * @return True if the value is in range for a Java byte. False otherwise.
     */
    public static boolean isInRangeForByte(long value) {
        return value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE;
    }

    /**
     * Is the value in range for a Java short?
     * 
     * @param value The value.
     * @return True if the value is in range for a Java short. False otherwise.
     */
    public static boolean isInRangeForShort(long value) {
        return value >= Short.MIN_VALUE && value <= Short.MAX_VALUE;
    }

    /**
     * Is the value in range for a Java int?
     * 
     * @param value The value.
     * @return True if the value is in range for a Java int. False otherwise.
     */
    public static boolean isInRangeForInt(long value) {
        return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
    }

    /**
     * Is the value in range for a Java float?
     * 
     * @param value The value.
     * @return True if the value is in range for a Java float. False otherwise.
     */
    public static boolean isInRangeForFloat(double value) {
        return Double.isNaN(value) ||
                Double.isInfinite(value) ||
                (value >= -Float.MAX_VALUE && value <= Float.MAX_VALUE);
    }

    /**
     * Are all the characters in byte slice ASCII?
     * 
     * @param data The character data.
     * @param begin The inclusive start of the slice.
     * @param end The exclusive end of the slice.
     * @return True if all the characters are ASCII, false otherwise.
     */
    public static boolean isAscii(final byte[] data, final int begin, final int end) {
        for (int cur = begin; cur != end; ++cur) {
            if (data[cur] < 0) {
                return false;
            }
        }
        return true;
    }
}
