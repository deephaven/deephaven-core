package io.deephaven.csv.parsers;

import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.jetbrains.annotations.NotNull;

/**
 * The parser for the float type. Uses the builtin Java {@link Float#parseFloat} method. Callers who want faster parsing
 * and don't need the exact semantics of {@link Float#parseFloat} should use the {@link FloatFastParser} instead. Most
 * callers won't care, but the reason we provide two parsers is that there are some inputs for which
 * {@code Float.parseFloat(input)} differs slightly from {@code (float)Double.parseDouble(input)}.
 */
public final class FloatStrictParser implements Parser<float[]> {
    public static final FloatStrictParser INSTANCE = new FloatStrictParser();

    private FloatStrictParser() {}

    @NotNull
    @Override
    public ParserContext<float[]> makeParserContext(final GlobalContext gctx, final int chunkSize) {
        final Sink<float[]> sink = gctx.sinkFactory.forFloat();
        return new ParserContext<>(sink, null, new float[chunkSize]);
    }

    @Override
    public long tryParse(final GlobalContext gctx, final ParserContext<float[]> pctx, IteratorHolder ih,
            final long begin, final long end, final boolean appending) throws CsvReaderException {
        final MutableFloat floatHolder = new MutableFloat();
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
            if (!t.tryParseFloatStrict(ih.bs(), floatHolder)) {
                break;
            }
            final float value = floatHolder.floatValue();
            if (reservedValue != null && value == reservedValue) {
                // If a reserved value is defined, it must not be present in the input.
                break;
            }
            if (ih.bs().size() > 1) {
                gctx.isNullOrWidthOneSoFar = false;
            }
            values[chunkIndex] = value;
            nulls[chunkIndex] = false;
            ++chunkIndex;
        } while (ih.tryMoveNext());
        sink.write(values, nulls, current, current + chunkIndex, appending);
        return current + chunkIndex;
    }
}
