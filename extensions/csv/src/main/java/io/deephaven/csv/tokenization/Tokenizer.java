package io.deephaven.csv.tokenization;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.external.FastDoubleParserFromByteArray;
import org.apache.commons.lang3.mutable.*;

import java.time.*;

/**
 * This class provides a variety of methods to efficiently parse various low-level types like booleans, longs, doubles,
 * and datetimes.
 */
public class Tokenizer {
    /**
     * An optional custom time zone parser. Used for clients (such as Deephaven itself) who support custom time zone
     * formats.
     */
    private final CustomTimeZoneParser customTimeZoneParser;
    /**
     * Storage for a temporary "out" variable owned by tryParseDateTime.
     */
    private final MutableLong dateTimeTemp0 = new MutableLong();
    /**
     * Storage for a temporary "out" variable owned by tryParseDateTime.
     */
    private final MutableLong dateTimeTemp1 = new MutableLong();
    /**
     * Storage for a temporary "out" variable owned by tryParseDateTime.
     */
    private final MutableLong dateTimeTemp2 = new MutableLong();
    /**
     * Storage for a temporary "out" variable owned by tryParseDateTime.
     */
    private final MutableObject<ZoneId> dateTimeTempZoneId = new MutableObject<>();
    /**
     * Storage for a temporary "out" variable owned by tryParseDateTime.
     */
    private final MutableBoolean dateTimeTempBoolean = new MutableBoolean();

    public Tokenizer(CustomTimeZoneParser customTimeZoneParser) {
        this.customTimeZoneParser = customTimeZoneParser;
    }

    /**
     * Try to parse the input as a boolean.
     * 
     * @param bs The input text. This slice is *NOT* modified, regardless of success or failure.
     * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
     * @return true if the input was successfully parsed. Otherwise, false.
     */
    public boolean tryParseBoolean(final ByteSlice bs, final MutableBoolean result) {
        final int savedBegin = bs.begin();
        final int savedEnd = bs.end();
        Mutating.trim(bs);
        // Successful if parse was successful AND input was completely consumed.
        final boolean success = Mutating.tryParseBoolean(bs, result) && bs.begin() == bs.end();
        bs.setBegin(savedBegin);
        bs.setEnd(savedEnd);
        return success;
    }

    /**
     * Try to parse the input as a single character in Unicode's Basic Multilingual Plane. This means the character will
     * fit in a single Java "char" without requiring UTF-16 surrogate pairs. Unicode characters that meet this criterion
     * are either in the range U+0000 through U+D7FF, or the range U+E000 through U+FFFF.
     *
     * @param bs The input text. This slice is *NOT* modified, regardless of success or failure.
     * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
     * @return true if the input was successfully parsed. Otherwise, false. The return value is provided in a
     *         {@link MutableInt} because Apache doesn't provide a MutableChar.
     */
    public boolean tryParseBMPChar(final ByteSlice bs, final MutableInt result) {
        final byte[] d = bs.data();
        int o = bs.begin();
        final int end = bs.end();
        if (o == end) {
            return false;
        }
        final int first = byteToInt(d[o++]);
        final int moreExpected;
        int value;
        if ((first & 0x80) == 0) {
            // 0xxxxxxx
            // 1-byte UTF-8 character aka ASCII.
            // Last code point U+007F
            value = first & 0x7F;
            result.setValue(value);
            return o == end;
        }
        if ((first & 0xE0) == 0xC0) {
            // 110xxxxx
            // 2-byte UTF-8 character
            // Last code point U+07FF
            value = first & 0x1F;
            moreExpected = 1;
        } else if ((first & 0xF0) == 0xE0) {
            // 1110xxxx
            // 3-byte UTF-8 character
            // Last code point U+FFFF
            value = first & 0x0F;
            moreExpected = 2;
        } else {
            // 11110xxx
            // 4-byte UTF-8 character
            // This would take us into U+10000 territory, so we reject it.
            return false;
        }

        for (int ii = 0; ii < moreExpected; ++ii) {
            if (o == end) {
                return false;
            }
            final int next = byteToInt(d[o++]);
            if ((next & 0xc0) != 0x80) {
                // bad UTF-8 actually.
                return false;
            }
            value = (value << 6) | (next & 0x3F);
        }

        result.setValue(value);
        return true;
    }

    private static int byteToInt(byte b) {
        return b >= 0 ? b : 256 + b;
    }

    /**
     * Try to parse the input as a long.
     * 
     * @param bs The input text. This slice is *NOT* modified, regardless of success or failure.
     * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
     * @return true if the input was successfully parsed. Otherwise, false.
     */
    public boolean tryParseLong(final ByteSlice bs, final MutableLong result) {
        final int savedBegin = bs.begin();
        final int savedEnd = bs.end();
        Mutating.trim(bs);
        // Successful if parse was successful AND input was completely consumed.
        final boolean success = Mutating.tryParseLong(bs, result) && bs.begin() == bs.end();
        bs.setBegin(savedBegin);
        bs.setEnd(savedEnd);
        return success;
    }

    /**
     * Try to parse the input as a float, using {@link Float#parseFloat}. Most code will prefer to use
     * {@link Tokenizer#tryParseDouble} because it is much faster. This method exists for callers who want the exact
     * semantics of Java's {@link Float#parseFloat} and are willing to pay the performance cost (both of the toString()
     * and of the slower parser).
     *
     * @param bs The input text. This slice is *NOT* modified, regardless of success or failure.
     * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
     * @return true if {@code bs} was successfully parsed as a float. Otherwise, false.
     */
    public boolean tryParseFloatStrict(final ByteSlice bs, final MutableFloat result) {
        try {
            final float res = Float.parseFloat(bs.toString());
            result.setValue(res);
            return true;
        } catch (NumberFormatException nfe) {
            // Normally we would be pretty sad about throwing exceptions in the inner loops of our CSV parsing
            // framework, but the fact of the matter is that the first exception thrown will cause the
            // calling parser to punt to the next parser anyway, so the overall impact is negligible.
            return false;
        }
    }

    /**
     * Try to parse the input as a double.
     * 
     * @param bs The input text. This slice is *NOT* modified, regardless of success or failure.
     * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
     * @return true if {@code bs} was successfully parsed as a double. Otherwise, false.
     */
    public boolean tryParseDouble(final ByteSlice bs, final MutableDouble result) {
        // Our third-party double parser already checks for trailing garbage so we don't have to.
        try {
            final double res = FastDoubleParserFromByteArray.parseDouble(bs.data(), bs.begin(), bs.size());
            result.setValue(res);
            return true;
        } catch (NumberFormatException nfe) {
            // Normally we would be pretty sad about throwing exceptions in the inner loops of our CSV parsing
            // framework, but the fact of the matter is that the first exception thrown will cause the
            // calling parser to punt to the next parser anyway, so the overall impact is negligible.
            return false;
        }
    }

    /**
     * Try to parse the input as a Deephaven DateTime value (represented as nanoseconds since the epoch).
     * 
     * @param bs The input text. This slice is *NOT* modified, regardless of success or failure.
     * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
     * @return true if {@code bs} was successfully parsed as a Deephaven DateTime. Otherwise, false.
     */
    public boolean tryParseDateTime(final ByteSlice bs, final MutableLong result) {
        final int savedBegin = bs.begin();
        // Successful if parse was successful AND input was completely consumed.
        final boolean success = Mutating.tryParseDateTime(bs, customTimeZoneParser,
                dateTimeTemp0, dateTimeTemp1, dateTimeTemp2, dateTimeTempBoolean, dateTimeTempZoneId, result) &&
                bs.begin() == bs.end();
        bs.setBegin(savedBegin);
        return success;
    }

    /**
     * The methods in this utility class obey the following invariants: On success, they update their incoming ByteSlice
     * to point to the end of the sequence. On failure, they leave it unchanged.
     */
    private static final class Mutating {
        /**
         * Modify the input slice to remove leading and trailing whitespace, if any.
         * 
         * @param bs Modified in place to remove leading and trailing whitespace, if any.
         */
        public static void trim(final ByteSlice bs) {
            while (bs.begin() != bs.end() && RangeTests.isSpaceOrTab(bs.front())) {
                bs.setBegin(bs.begin() + 1);
            }
            while (bs.begin() != bs.end() && RangeTests.isSpaceOrTab(bs.back())) {
                bs.setEnd(bs.end() - 1);
            }
        }

        /**
         * If the slice is nonempty and its first character is {@code ch}, then eat the first character.
         * 
         * @param bs If the method returns true, the slice is updated to remove the first character. Otherwise the slice
         *        is unmodified.
         * @return true If the character was eaten, false otherwise.
         */
        private static boolean tryEatChar(final ByteSlice bs, final char ch) {
            if (bs.begin() == bs.end() || bs.front() != ch) {
                return false;
            }
            bs.setBegin(bs.begin() + 1);
            return true;
        }

        /**
         * Parse (a prefix of) the input as a boolean.
         * 
         * @param bs If the method returns true, the slice is updated to remove the characters comprising the result.
         *        Otherwise, the slice is unmodified.
         * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        public static boolean tryParseBoolean(final ByteSlice bs, final MutableBoolean result) {
            final byte[] d = bs.data();
            final int o = bs.begin();
            final int bSize = bs.size();

            if (bSize == 4) {
                if ((d[o] == 't' || d[o] == 'T') &&
                        (d[o + 1] == 'r' || d[o + 1] == 'R') &&
                        (d[o + 2] == 'u' || d[o + 2] == 'U') &&
                        (d[o + 3] == 'e' || d[o + 3] == 'E')) {
                    result.setValue(true);
                    bs.setBegin(bs.end());
                    return true;
                }
                return false;
            }

            if (bSize == 5) {
                if ((d[o] == 'f' || d[o] == 'F') &&
                        (d[o + 1] == 'a' || d[o + 1] == 'A') &&
                        (d[o + 2] == 'l' || d[o + 2] == 'L') &&
                        (d[o + 3] == 's' || d[o + 3] == 'S') &&
                        (d[o + 4] == 'e' || d[o + 4] == 'E')) {
                    result.setValue(false);
                    bs.setBegin(bs.end());
                    return true;
                }
                return false;
            }

            return false;
        }

        /**
         * Parse (a prefix of) the input as a long.
         * 
         * @param bs If the method returns true, the slice is updated to remove the characters comprising the result.
         *        Otherwise, the slice is unmodified.
         * @param result Contains the parsed value if this method returns true. Otherwise, the contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        public static boolean tryParseLong(final ByteSlice bs, final MutableLong result) {
            final int savedBegin = bs.begin();
            if (bs.begin() == bs.end()) {
                return false;
            }
            final char front = (char) bs.front();
            boolean negative = false;
            if (front == '+') {
                bs.setBegin(bs.begin() + 1);
            } else if (front == '-') {
                negative = true;
                bs.setBegin(bs.begin() + 1);
            }
            if (!tryParseWholeNumber(bs, 1, 999, negative, result)) {
                bs.setBegin(savedBegin);
                return false;
            }
            return true;
        }

        /**
         * Parse (a prefix of) the input as a DateTime. Formats are largely ISO except we allow a pluggable timezone
         * parser, used for example to support Deephaven-style time zones.
         * <p>
         * Allowable formats:
         * <p>
         * <ul>
         * <li>2021-11-07T09:00:00Z</li>
         * <li>2021-11-07T09:00:00.1Z</li>
         * <li>2021-11-07T09:00:00.12Z</li>
         * <li>...</li>
         * <li>2021-11-07T09:00:00.123456789Z</li>
         * </ul>
         *
         * <p>
         * Hyphens and colons are optional (all in or all out). The 'T' can also be a space. The Z above is either the
         * literal Z meaning UTC or some other text. If this character is not Z, the method will call out to a pluggable
         * time zone parser to see if the text can be parsed as a time zone. In Deephaven this is used to parse
         * Deephaven time zones like " NY", " MN", " ET", " UTC" etc.
         *
         * <p>
         * Allowable formats in UTC offset style (can be + or -):
         * <p>
         * The offset can be hh or hh:mm or hhmm.
         * <ul>
         * <li>2021-11-07T09:00:00+01</li>
         * <li>2021-11-07T09:00:00.1-02:30</li>
         * <li>2021-11-07T09:00:00.12+0300</li>
         * <li>...</li>
         * <li>2021-11-07T09:00:00.123456789+01:30</li>
         * </ul>
         *
         * @param bs The input text. If the method returns true, the slice will be advanced past the parsed text.
         *        Otherwise (if the method returns false), the slice will be unchanged.
         * @param temp0 A MutableLong for the method to use for temporary storage, so it doesn't have to allocate one.
         * @param temp1 A MutableLong for the method to use for temporary storage, so it doesn't have to allocate one.
         * @param temp2 A MutableLong for the method to use for temporary storage, so it doesn't have to allocate one.
         * @param tempZoneId A MutableObject&lt;ZoneId&gt; for the method to use for temporary storage, so it doesn't
         *        have to allocate one.
         * @param result The DateTime (in nanoseconds since the epoch) if the method returns true. Otherwise, the
         *        contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        private static boolean tryParseDateTime(final ByteSlice bs, final CustomTimeZoneParser customTimeZoneParser,
                final MutableLong temp0, final MutableLong temp1, final MutableLong temp2,
                final MutableBoolean tempBoolean, final MutableObject<ZoneId> tempZoneId,
                final MutableLong result) {
            // The logic proceeds as follows.
            // First we have the required fields:
            // yyyy
            // - (optional, but if absent then no later hyphens or colons)
            // mm
            // - (optional, but presence or absence of punctuation needs to be consistent. Also we can stop here).
            // dd
            // T or space (or we stop here)
            // hh
            // : (optional, but presence or absence of punctuation needs to be consistent. Also we can stop here).
            // MM
            // : (optional, but presence or absence of punctuation needs to be consistent. Also we can stop here).
            // SS
            // . or , (optional, introduces fraction, must be followed by 1-9 decimal digits).
            // Z or + or -:
            // Z means UTC
            // + or - means an offset follows, which itself is
            // hh
            // : (optional)
            // mm (optional)
            // Otherwise we call out to the pluggable time zone parser to see if it can parse a timezone out of the
            // remaining text.
            final int savedBegin = bs.begin();
            if (!tryParseYyyymmdd(bs, temp0, temp1, temp2, tempBoolean)) {
                return false;
            }
            final int year = temp0.intValue();
            final int month = temp1.intValue();
            final int day = temp2.intValue();
            final boolean punctuationRequired = tempBoolean.booleanValue();

            // Require 'T' or ' ' (per RFC 3339).
            if (!tryEatChar(bs, 'T') && !tryEatChar(bs, ' ')) {
                bs.setBegin(savedBegin);
                return false;
            }

            // Reusing result for temporary storage!
            if (!tryParseHHmmssNanos(bs, punctuationRequired, temp0, temp1, temp2, result)) {
                bs.setBegin(savedBegin);
                return false;
            }
            final int hour = temp0.intValue();
            final int minute = temp1.intValue();
            final int second = temp2.intValue();
            final int nanos = result.intValue();

            if (!tryParseIsoTimeZone(bs, tempZoneId, temp0) &&
                    (customTimeZoneParser == null || !customTimeZoneParser.tryParse(bs, tempZoneId, temp0))) {
                bs.setBegin(savedBegin);
                return false;
            }
            final ZoneId zoneIdToUse = tempZoneId.getValue();
            final long secondsOffsetToUse = temp0.getValue();

            final ZonedDateTime zdt = ZonedDateTime.of(year, month, day, hour, minute, second, 0, zoneIdToUse);
            final long zdtSeconds = zdt.toEpochSecond();
            final long adjustedZdtSeconds = zdtSeconds + secondsOffsetToUse;
            final long adjustedZdtNanos = adjustedZdtSeconds * 1_000_000_000L + nanos;
            result.setValue(adjustedZdtNanos);
            return true;
        }

        /**
         * Parse (a prefix of) the input as yyyyMMdd or yyyy-MM-dd.
         *
         * @param bs The input text. If the method returns true, the slice will be advanced past the parsed text.
         *        Otherwise (if the method returns false), the slice will be unchanged.
         * @param yyyy Contains the parsed year if this method returns true. Otherwise, the contents are unspecified.
         * @param mm Contains the parsed month if this method returns true. Otherwise, the contents are unspecified.
         * @param dd Contains the parsed day if this method returns true. Otherwise, the contents are unspecified.
         * @param hasPunctuation Contains whether hyphens were found in the input if this method returns true.
         *        Otherwise, the contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        private static boolean tryParseYyyymmdd(final ByteSlice bs, final MutableLong yyyy,
                final MutableLong mm, final MutableLong dd, final MutableBoolean hasPunctuation) {
            final int savedBegin = bs.begin();
            if (!tryParseWholeNumber(bs, 4, 4, false, yyyy)) {
                return false;
            }

            hasPunctuation.setValue(Mutating.tryEatChar(bs, '-'));

            if (!tryParseWholeNumber(bs, 2, 2, false, mm)) {
                bs.setBegin(savedBegin);
                return false;
            }

            if (hasPunctuation.booleanValue() && !tryEatChar(bs, '-')) {
                bs.setBegin(savedBegin);
                return false;
            }
            if (!tryParseWholeNumber(bs, 2, 2, false, dd)) {
                bs.setBegin(savedBegin);
                return false;
            }
            return true;
        }

        /**
         * Parse (a prefix of) the input as hhmmss.nnnnnn or hh:mm:ss.nnnn and various variants (minutes, seconds, and
         * nanos are optional, and the nanos separator is either period or comma).
         *
         * @param bs The input text. If the method returns true, the slice will be advanced past the parsed text.
         *        Otherwise (if the method returns false), the slice will be unchanged.
         * @param punctuationRequired Indicates whether punctuation (namely colons) is required between the fields.
         * @param hours Contains the parsed hours if this method returns true. Otherwise, the contents are unspecified.
         * @param minutes Contains the parsed minutes if this method returns true. Otherwise, the contents are
         *        unspecified.
         * @param seconds Contains the parsed seconds if this method returns true. Otherwise, the contents are
         *        unspecified.
         * @param nanos Contains the parsed nanos if this method returns true. Otherwise, the contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        private static boolean tryParseHHmmssNanos(final ByteSlice bs, final boolean punctuationRequired,
                final MutableLong hours,
                final MutableLong minutes,
                final MutableLong seconds, final MutableLong nanos) {
            final int savedBegin = bs.begin();
            // Hour
            if (!tryParseWholeNumber(bs, 2, 2, false, hours)) {
                return false;
            }
            // Set defaults for minutes, seconds, nanos, in case we exit early.
            minutes.setValue(0);
            seconds.setValue(0);
            nanos.setValue(0);

            // Minutes, seconds, and nanos are optional.

            // If a colon is required but not present, then the parse is done (this is not an error).
            if (punctuationRequired && !tryEatChar(bs, ':')) {
                return true;
            }

            // Try minutes
            if (!tryParseWholeNumber(bs, 2, 2, false, minutes)) {
                // Next thing is not a number. If we previously ingested a colon, not having a next number is an error.
                // But if we did not ingest a colon, not having a number is ok.
                // If we return false we are obligated to reset the slice.
                minutes.setValue(0); // Sub-parse failed, but we still might return success. So this needs to be
                                     // correct.
                final boolean success = !punctuationRequired;
                if (!success) {
                    bs.setBegin(savedBegin);
                }
                return success;
            }

            // If a colon is required but not present, then the parse is done (this is not an error).
            if (punctuationRequired && !tryEatChar(bs, ':')) {
                return true;
            }

            // Try seconds.
            if (!tryParseWholeNumber(bs, 2, 2, false, seconds)) {
                // Next thing is not a number. If we previously ingested a colon, not having a next number is an error.
                // But if we did not ingest a colon, not having a number is ok.
                // If we return false we are obligated to reset the slice.
                seconds.setValue(0); // Sub-parse failed, but we still might return success. So this needs to be
                                     // correct.
                final boolean success = !punctuationRequired;
                if (!success) {
                    bs.setBegin(savedBegin);
                }
                return success;
            }

            if (!tryEatChar(bs, '.') && !tryEatChar(bs, ',')) {
                // Period (or comma!) introduces fraction. If not present, then stop the parse here (with a success
                // indication)
                return true;
            }

            // Try nanoseconds
            final int beginBeforeNs = bs.begin();
            if (!tryParseWholeNumber(bs, 1, 9, false, nanos)) {
                // If you couldn't get a number, that's a parse fail.
                bs.setBegin(savedBegin);
                return false;
            }

            // Pad to the right with zeroes (that is, in "blah.12", the .12 is 120,000,000 nanos.
            final int length = bs.begin() - beginBeforeNs;
            for (int ii = length; ii < 9; ++ii) {
                nanos.setValue(10 * nanos.getValue());
            }
            return true;
        }

        /**
         * Try to parse (a prefix of) the input as a whole number.
         *
         * @param bs The input text. If the method returns true, the slice will be advanced past the parsed text.
         *        Otherwise (if the method returns false), the slice will be unchanged.
         * @param minSize The parsed number must be at least this many digits. Otherwise, we will return false.
         * @param maxSize The parsed number must be at most this many digits. We will stop the parse after this size,
         *        even if the parse could continue (e.g. even if a digit immediately follows).
         * @param negate If we should negate the parsed number on the way out.
         * @param result Contains the parsed whole number if this method returns true. Otherwise, the contents are
         *        unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        private static boolean tryParseWholeNumber(final ByteSlice bs, final int minSize, final int maxSize,
                final boolean negate, final MutableLong result) {
            final byte[] data = bs.data();
            final int begin = bs.begin();
            final int end = bs.end();
            final int size = bs.size();
            if (size < minSize) {
                return false;
            }
            final int endToUse = Math.min(end, begin + maxSize);
            long res = 0;
            long prevRes = 0;
            int current = begin;
            // We build the number using negative values, because the negative range is slightly longer and this helps
            // us when we happen to parse Long.MIN_VALUE.
            for (; current < endToUse; ++current) {
                final char ch = (char) data[current];
                if (!RangeTests.isDigit(ch)) {
                    break;
                }
                res = res * 10 - (ch - '0');
                if (res > prevRes) {
                    // Overflow.
                    return false;
                }
                prevRes = res;
            }
            if (current == begin) {
                return false;
            }
            if (!negate) {
                // Caller wanted a positive number, but we operate in a negative number system.
                if (res == Long.MIN_VALUE) {
                    // Can't represent the negation of Long.MIN_VALUE.
                    return false;
                }
                res = -res;
            }
            result.setValue(res);
            bs.setBegin(current);
            return true;
        }

        /**
         * Try to parse (a prefix of) the input as an ISO time zone. For convenience/efficiency, the method is allowed
         * to return either a ZoneOffset or a numerical offset in seconds (or both).
         * 
         * @param bs The input text. If the method returns true, the slice will be advanced past the parsed text.
         *        Otherwise (if the method returns false), the slice will be unchanged.
         * @param zoneId Contains the parsed time zone if this method returns true. Otherwise, the contents are
         *        unspecified.
         * @param offsetSeconds Contains a time zone offset in seconds if this method returns true. Otherwise, the
         *        contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        private static boolean tryParseIsoTimeZone(final ByteSlice bs, final MutableObject<ZoneId> zoneId,
                final MutableLong offsetSeconds) {
            if (bs.size() == 0) {
                return false;
            }

            final char front = (char) bs.front();
            if (front == 'Z') {
                zoneId.setValue(ZoneOffset.UTC);
                offsetSeconds.setValue(0);
                bs.setBegin(bs.begin() + 1);
                return true;
            }

            // Try an offset like +02 or +03:30 or -0400
            if (front != '+' && front != '-') {
                return false;
            }
            final boolean negative = front == '-';

            final int savedBegin = bs.begin();
            bs.setBegin(bs.begin() + 1);

            // Reuse offsetSeconds as temp variable
            if (!tryParseWholeNumber(bs, 2, 2, false, offsetSeconds)) {
                bs.setBegin(savedBegin);
                return false;
            }
            final long hours = offsetSeconds.longValue();

            // Optional colon
            tryEatChar(bs, ':');

            long minutes = 0;
            if (bs.size() != 0) {
                // Reuse offsetSeconds as temp variable
                if (!tryParseWholeNumber(bs, 2, 2, false, offsetSeconds)) {
                    bs.setBegin(savedBegin);
                    return false;
                }
                minutes = offsetSeconds.longValue();
            }
            zoneId.setValue(ZoneOffset.UTC);

            // If someone says yyyy-MM-DDThh:mm:ss-05
            // The "-05" means this is meant to be interpreted as UTC-5.
            // If I parse yyyy-MM-DDThh:mm:ss in UTC (without any offset), it will be 5 hours later than
            // what the user intended. So in other words, I need to negate the -05.
            // Put more simply, when it is 1pm in the zone UTC-4, it is 5pm in the zone UTC.
            // So to convert 1pm UTC-4 to 5pm UTC you need to *add* 4.
            final long offset = ((hours * 60) + minutes) * 60;
            offsetSeconds.setValue(negative ? offset : -offset);
            return true;
        }
    }

    /**
     * A pluggable interface for a user-supplied time zone parser.
     */
    public interface CustomTimeZoneParser {
        /**
         * Try to parse a user-defined time zone.
         * 
         * @param bs The input text. If the method returns true, the slice will be advanced past the parsed text.
         *        Otherwise (if the method returns false), the slice will be unchanged.
         * @param zoneId Contains the parsed time zone if this method returns true. Otherwise, the contents are
         *        unspecified.
         * @param offsetSeconds Contains a time zone offset in seconds if this method returns true. Otherwise, the
         *        contents are unspecified.
         * @return true if the input was successfully parsed. Otherwise, false.
         */
        boolean tryParse(final ByteSlice bs, final MutableObject<ZoneId> zoneId, final MutableLong offsetSeconds);
    }
}
