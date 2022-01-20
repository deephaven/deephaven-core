package io.deephaven.csv.parsers;

/**
 * The parser for "milliseconds since Unix epoch".
 */
public class TimestampMillisParser extends TimestampParserBase {
    public static final TimestampMillisParser INSTANCE = new TimestampMillisParser();

    private TimestampMillisParser() {
        super(MILLISECOND_SCALE);
    }
}
