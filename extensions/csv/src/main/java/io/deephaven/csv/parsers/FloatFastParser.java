package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the float type. Uses the FastDoubleParser library. Callers who want the exact semantics of
 * {@link Float#parseFloat} should use the {@link FloatStrictParser} instead. Most callers won't care, but the reason we
 * provide two parsers is that there are some inputs for which {@code Float.parseFloat(input)} differs slightly from
 * {@code (float)Double.parseDouble(input)}. Callers that want exactly the answer that {@link Float#parseFloat} provides
 * should use {@link FloatStrictParser}.
 */
public final class FloatFastParser implements Parser<float[]> {
    public static final FloatFastParser INSTANCE = new FloatFastParser();

    private FloatFastParser() {}

    @NotNull
    @Override
    public ParserContext<float[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<float[]> sink = gctx.sinkFactory.forFloat();
        return new ParserContext<>(sink, null, new float[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<float[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableDouble doubleHolder = new MutableDouble();
        final Tokenizer t = gctx.tokenizer;
        final boolean[] nulls = gctx.nullChunk();

        final Sink<float[]> sink = pctx.sink();
        final Float reservedValue = gctx.sinkFactory.reservedFloat();
        final float[] values = pctx.valueChunk();

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
            if (!t.tryParseDouble(ih.bs(), doubleHolder)) {
                break;
            }
            final double value = doubleHolder.doubleValue();
            if (!RangeTests.isInRangeForFloat(value)) {
                break;
            }
            if (reservedValue != null && value == reservedValue) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            if (ih.bs().size() > 1) {
                gctx.isNullOrWidthOneSoFar = false;
            }
            values[chunkIndex] = (float) value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
