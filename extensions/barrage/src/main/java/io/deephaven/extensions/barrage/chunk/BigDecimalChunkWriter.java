//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
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
import java.util.Arrays;
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
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> {
            if (objectChunk.isNull((int) row)) {
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
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(objectChunk.isNull((int) row)));
    }

    @Override
    protected void writePayload(
            @NotNull final Context context,
            @NotNull final DataOutput dos,
            @NotNull final RowSequence subset) {
        final int byteWidth = decimalType.getBitWidth() / 8;
        final int scale = decimalType.getScale();
        final byte[] zeroValue = new byte[byteWidth];
        final byte[] minusOneValue = new byte[byteWidth];
        Arrays.fill(minusOneValue, (byte) -1);

        // reserve the leading bit for the sign
        final BigInteger truncationMask = BigInteger.ONE.shiftLeft(byteWidth * 8 - 1)
                .subtract(BigInteger.ONE);

        final ObjectChunk<BigDecimal, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(rowKey -> {
            try {
                BigDecimal value = objectChunk.get((int) rowKey);
                if (value == null) {
                    dos.write(zeroValue, 0, zeroValue.length);
                    return;
                }

                if (value.scale() != scale) {
                    value = value.setScale(scale, RoundingMode.HALF_UP);
                }

                final BigInteger truncatedValue;
                boolean isNegative = value.signum() < 0;
                if (isNegative) {
                    // negative values are sign extended to match truncationMask's byte length; operate on abs-value
                    truncatedValue = value.unscaledValue().negate().and(truncationMask).negate();
                } else {
                    truncatedValue = value.unscaledValue().and(truncationMask);
                }
                byte[] bytes = truncatedValue.toByteArray();
                // toByteArray is BigEndian, but arrow default is LE, so must swap order
                for (int ii = 0; ii < bytes.length / 2; ++ii) {
                    byte tmp = bytes[ii];
                    bytes[ii] = bytes[bytes.length - 1 - ii];
                    bytes[bytes.length - 1 - ii] = tmp;
                }

                int numZeroBytes = byteWidth - bytes.length;
                Assert.geqZero(numZeroBytes, "numZeroBytes");
                dos.write(bytes);
                if (numZeroBytes > 0) {
                    dos.write(isNegative ? minusOneValue : zeroValue, 0, numZeroBytes);
                }
            } catch (final IOException e) {
                throw new UncheckedDeephavenException(
                        "Unexpected exception while draining data to OutputStream: ", e);
            }
        });
    }
}
