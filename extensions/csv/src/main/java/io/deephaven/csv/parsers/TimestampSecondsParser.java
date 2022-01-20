package io.deephaven.csv.parsers;

/**
 * The parser for "seconds since Unix epoch".
 */
public final class TimestampSecondsParser extends TimestampParserBase {
    public static final TimestampSecondsParser INSTANCE = new TimestampSecondsParser();

    private TimestampSecondsParser() {
        super(SECOND_SCALE);
    }
}
