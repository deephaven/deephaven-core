package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

/**
 * The base class for various timestamp parsers. These parsers parse longs, scale them by some appropriate value, and
 * then feed them to the sink for the Deephaven DateTime (as long) type.
 */
public abstract class TimestampParserBase implements Parser<long[]> {
    protected static final long SECOND_SCALE = 1_000_000_000;
    protected static final long MILLISECOND_SCALE = 1_000_000;
    protected static final long MICROSECOND_SCALE = 1_000;
    protected static final long NANOSECOND_SCALE = 1;

    private final long scale;
    private final long minValue;
    private final long maxValue;

    /**
     * @param scale: 1 for seconds, 1000 for millis, 1_000_000 for micros, 1_000_000_000 for nanos
     */
    protected TimestampParserBase(long scale) {
        this.scale = scale;
        minValue = Long.MIN_VALUE / scale;
        maxValue = Long.MAX_VALUE / scale;
    }

    @NotNull
    @Override
    public ParserContext<long[]> makeParserContext(final Parser.GlobalContext gctx, final int chunkSize) {
        final Sink<long[]> sink = gctx.sinkFactory.forTimestampAsLong();
        return new ParserContext<>(sink, null, new long[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<long[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableLong longHolder = new MutableLong();
        final Tokenizer t = gctx.tokenizer;
        final boolean[] nulls = gctx.nullChunk();

        final Sink<long[]> sink = pctx.sink();
        final Long reservedValue = gctx.sinkFactory.reservedLong();
        final long[] values = pctx.valueChunk();

        long current = begin;
        int chunkIndex = 0;
        do {
            if (chunkIndex == values.length) {
                sink.write(values, nulls, current, current + chunkIndex, appending);
                current += chunkIndex;
                chunkIndex = 0;
            }
            if (current + chunkIndex == end) {
                break;
            }
            if (gctx.isNullCell(ih)) {
                nulls[chunkIndex++] = true;
                continue;
            }
            if (!t.tryParseLong(ih.bs(), longHolder)) {
                break;
            }
            final long value = longHolder.longValue();
            if (value < minValue || value > maxValue) {
                break;
            }
            if (reservedValue != null && value == reservedValue) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            if (ih.bs().size() > 1) {
                gctx.isNullOrWidthOneSoFar = false;
            }
            values[chunkIndex] = value * scale;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
