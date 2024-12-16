//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.mutable.MutableInt;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.function.Supplier;

public class BigDecimalChunkWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>>
        extends FixedWidthChunkWriter<SOURCE_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "BigDecimalWriter";

    private final ArrowType.Decimal decimalType;

    public BigDecimalChunkWriter(
            @Nullable final ChunkTransformer<SOURCE_CHUNK_TYPE> transformer,
            final ArrowType.Decimal decimalType,
            @NotNull final Supplier<SOURCE_CHUNK_TYPE> emptyChunkSupplier,
            final int elementSize,
            final boolean dhNullable,
            final boolean fieldNullable) {
        super(transformer, emptyChunkSupplier, elementSize, dhNullable, fieldNullable);
        this.decimalType = decimalType;
    }

    @Override
    protected int computeNullCount(
            @NotNull final Context context,
            @NotNull final RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        subset.forAllRowKeys(row -> {
            if (context.getChunk().asObjectChunk().isNull((int) row)) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        subset.forAllRowKeys(row -> {
            serContext.setNextIsNull(context.getChunk().asObjectChunk().isNull((int) row));
        });
    }

    @Override
    protected void writePayload(
            @NotNull final Context context,
            @NotNull final DataOutput dos,
            @NotNull final RowSequence subset) {
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] nullValue = new byte[byteWidth];
        // note that BigInteger's byte array requires one sign bit; note we negate so the BigInteger#and keeps sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE)
                .negate();

        subset.forAllRowKeys(rowKey -> {
            try {
                BigDecimal value = context.getChunk().<BigDecimal>asObjectChunk().get((int) rowKey);

                if (value.scale() != scale) {
                    value = value.setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                }

                byte[] bytes = value.unscaledValue().and(truncationMask).toByteArray();
                int numZeroBytes = byteWidth - bytes.length;
                Assert.geqZero(numZeroBytes, "numZeroBytes");
                if (numZeroBytes > 0) {
                    dos.write(nullValue, 0, numZeroBytes);
                }
                dos.write(bytes);

            } catch (final IOException e) {
                throw new UncheckedDeephavenException(
                        "Unexpected exception while draining data to OutputStream: ", e);
            }
        });
    }
}
