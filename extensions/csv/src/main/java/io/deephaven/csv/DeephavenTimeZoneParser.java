package io.deephaven.csv;

import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.MutableLong;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.time.TimeZone;

import java.time.ZoneId;


public final class DeephavenTimeZoneParser implements Tokenizer.CustomTimeZoneParser {
    private static final String DEEPHAVEN_TZ_PREFIX = "TZ_";
    private static final int MAX_DEEPHAVEN_TZ_LENGTH = 3;
    private static final TIntObjectHashMap<ZoneId> ZONE_ID_MAP = createZoneIdMap();

    private int lastTzKey = -1;
    private ZoneId lastZoneId = null;

    public DeephavenTimeZoneParser() {

    }

    @Override
    public boolean tryParse(ByteSlice bs, MutableObject<ZoneId> zoneId, MutableLong offsetSeconds) {
        if (bs.size() == 0 || bs.front() != ' ') {
            return false;
        }
        final int savedBegin = bs.begin();
        bs.setBegin(bs.begin() + 1);
        final int tzKey = tryParseTzKey(bs);
        if (tzKey < 0) {
            bs.setBegin(savedBegin);
            return false;
        }
        if (tzKey != lastTzKey) {
            final ZoneId res = ZONE_ID_MAP.get(tzKey);
            if (res == null) {
                bs.setBegin(savedBegin);
                return false;
            }
            lastTzKey = tzKey;
            lastZoneId = res;
        }
        zoneId.setValue(lastZoneId);
        offsetSeconds.setValue(0);
        return true;
    }

    /**
     * Take up to three uppercase characters from a TimeZone string and pack them into an integer.
     *
     * @param bs A ByteSlice holding the timezone key.
     * @return The characters packed into an int, or -1 if there are too many or too few characters, or if the
     *         characters are not uppercase ASCII.
     */
    private static int tryParseTzKey(final ByteSlice bs) {
        int res = 0;
        int current;
        for (current = bs.begin(); current != bs.end(); ++current) {
            if (current - bs.begin() > MAX_DEEPHAVEN_TZ_LENGTH) {
                return -1;
            }
            final char ch = RangeTests.toUpper((char) bs.data()[current]);
            if (!RangeTests.isUpper(ch)) {
                // If it's some nonalphabetic character
                break;
            }
            res = res * 26 + (ch - 'A');
        }
        if (current - bs.begin() == 0) {
            return -1;
        }
        bs.setBegin(current);
        return res;
    }

    private static TIntObjectHashMap<ZoneId> createZoneIdMap() {
        final TIntObjectHashMap<ZoneId> zoneIdMap = new TIntObjectHashMap<>();
        for (TimeZone zone : TimeZone.values()) {
            final String zname = zone.name();
            if (!zname.startsWith(DEEPHAVEN_TZ_PREFIX)) {
                throw new RuntimeException("Logic error: unexpected enum in DBTimeZone: " + zname);
            }
            final String zSuffix = zname.substring(DEEPHAVEN_TZ_PREFIX.length());
            final int zlen = zSuffix.length();
            if (zlen > MAX_DEEPHAVEN_TZ_LENGTH) {
                throw new RuntimeException("Logic error: unexpectedly-long enum in DBTimeZone: " + zname);
            }
            final byte[] data = new byte[zlen];
            for (int ii = 0; ii < zlen; ++ii) {
                final char ch = zSuffix.charAt(ii);
                if (!RangeTests.isUpper(ch)) {
                    throw new RuntimeException("Logic error: unexpected character in DBTimeZone name: " + zname);
                }
                data[ii] = (byte) ch;
            }
            final ByteSlice bs = new ByteSlice(data, 0, data.length);
            final int tzKey = tryParseTzKey(bs);
            if (tzKey < 0) {
                throw new RuntimeException("Logic error: can't parse DBTimeZone as key: " + zname);
            }
            final ZoneId zoneId = zone.getTimeZone().toTimeZone().toZoneId();
            zoneIdMap.put(tzKey, zoneId);
        }
        return zoneIdMap;
    }
}
