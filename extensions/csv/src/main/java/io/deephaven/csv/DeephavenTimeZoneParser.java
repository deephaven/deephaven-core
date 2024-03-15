//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.csv;

import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.MutableLong;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.time.TimeZoneAliases;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;

public final class DeephavenTimeZoneParser implements Tokenizer.CustomTimeZoneParser {
    private static final int MAX_TZ_LENGTH = computeMaxTimeZoneLength();
    private static final TIntObjectHashMap<ZoneId> ZONE_ID_MAP = createZoneIdMap();

    private int lastTzKey = -1;
    private ZoneId lastZoneId = null;

    public DeephavenTimeZoneParser() {}

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

    private static int computeMaxTimeZoneLength() {
        int rst = 0;

        for (String tz : TimeZoneAliases.getAllZones().keySet()) {
            rst = Math.max(tz.length(), rst);
        }

        return rst;
    }

    /**
     * Take up to three uppercase characters from a time zone string and pack them into an integer.
     *
     * @param bs A ByteSlice holding the timezone key.
     * @return The characters packed into an int, or -1 if there are too many or too few characters, or if the
     *         characters are not uppercase ASCII.
     */
    private static int tryParseTzKey(final ByteSlice bs) {
        int res = 0;
        int current;
        for (current = bs.begin(); current != bs.end(); ++current) {
            if (current - bs.begin() > MAX_TZ_LENGTH) {
                return -1;
            }
            final char ch = RangeTests.toUpper((char) bs.data()[current]);
            res = res * 26 + (ch - 'A');
        }
        if (current - bs.begin() == 0) {
            return -1;
        }
        bs.setBegin(current);
        return Math.abs(res);
    }

    private static TIntObjectHashMap<ZoneId> createZoneIdMap() {
        final TIntObjectHashMap<ZoneId> zoneIdMap = new TIntObjectHashMap<>();
        for (Map.Entry<String, ZoneId> entry : TimeZoneAliases.getAllZones().entrySet()) {
            final String zname = entry.getKey();
            final ZoneId zoneId = entry.getValue();
            if (zname.length() > MAX_TZ_LENGTH) {
                throw new RuntimeException("Logic error: unexpectedly-long time zone name: " + zname);
            }
            final byte[] data = zname.getBytes(StandardCharsets.UTF_8);
            final ByteSlice bs = new ByteSlice(data, 0, data.length);
            final int tzKey = tryParseTzKey(bs);
            if (tzKey < 0) {
                throw new RuntimeException("Logic error: can't parse time zone as key: " + zname);
            }

            final ZoneId previous = zoneIdMap.put(tzKey, zoneId);

            if (previous != null) {
                throw new RuntimeException("Time zone hashing collision: " + zname + " " + tzKey);
            }
        }
        return zoneIdMap;
    }
}
