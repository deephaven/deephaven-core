/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.parquet.table.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.IntBuffer;
import java.util.Map;

/**
 * Classes that implement this interface are responsible for converting data from individual DH columns into buffers
 * to be written out to the Parquet file.
 *
 * @param <B>
 */
public interface TransferObject<B> extends SafeCloseable {
    static <DATA_TYPE> TransferObject<?> create(
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            @NotNull final Class<DATA_TYPE> columnType,
            @NotNull final ParquetInstructions instructions) {
        if (int.class.equals(columnType)) {
            return IntTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (long.class.equals(columnType)) {
            return LongTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (double.class.equals(columnType)) {
            return DoubleTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (float.class.equals(columnType)) {
            return FloatTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (Boolean.class.equals(columnType)) {
            return BooleanTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (short.class.equals(columnType)) {
            return new ShortTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (char.class.equals(columnType)) {
            return new CharTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (byte.class.equals(columnType)) {
            return new ByteTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        } else if (String.class.equals(columnType)) {
            return new StringTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }

        // If there's an explicit codec, we should disregard the defaults for these CodecLookup#lookup() will properly
        // select the codec assigned by the instructions, so we only need to check and redirect once.
        if (!CodecLookup.explicitCodecPresent(instructions.getCodecName(columnDefinition.getName()))) {
            if (BigDecimal.class.equals(columnType)) {
                // noinspection unchecked
                final ColumnSource<BigDecimal> bigDecimalColumnSource = (ColumnSource<BigDecimal>) columnSource;
                final BigDecimalUtils.PrecisionAndScale precisionAndScale = TypeInfos.getPrecisionAndScale(
                        computedCache, columnDefinition.getName(), tableRowSet, () -> bigDecimalColumnSource);
                final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(
                        precisionAndScale.precision, precisionAndScale.scale, -1);
                return new CodecTransfer<>(bigDecimalColumnSource, codec, tableRowSet, instructions.getTargetPageSize());
            } else if (BigInteger.class.equals(columnType)) {
                return new CodecTransfer<>(columnSource, new BigIntegerParquetBytesCodec(-1), tableRowSet,
                        instructions.getTargetPageSize());
            }
        }

        @Nullable final Class<?> dataType = columnDefinition.getDataType();
        @Nullable final Class<?> componentType = columnDefinition.getComponentType();
        if (dataType.isArray()) {
            if (int.class.equals(componentType)) {
                return new IntArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
//            else if (long.class.equals(componentType)) {
//                return LongArrayTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (double.class.equals(componentType)) {
//                return DoubleArrayTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (float.class.equals(componentType)) {
//                return FloatArrayTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (Boolean.class.equals(componentType)) {
//                return BooleanArrayTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (short.class.equals(componentType)) {
//                return new ShortArrayTransfer(columnSource, instructions.getTargetPageSize());
//            } else if (char.class.equals(componentType)) {
//                return new CharArrayTransfer(columnSource, instructions.getTargetPageSize());
//            } else if (byte.class.equals(componentType)) {
//                return new ByteArrayTransfer(columnSource, instructions.getTargetPageSize());
//            } else if (String.class.equals(componentType)) {
//                return new StringArrayTransfer(columnSource, instructions.getTargetPageSize());
//            }
//            // else if (explicit codec provided)
//            // else if (big decimal)
//            // else if (big integer)
        }
        if (Vector.class.isAssignableFrom(dataType)) {
            if (int.class.equals(componentType)) {
                return new IntVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
//            } else if (long.class.equals(componentType)) {
//                return LongVectorTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (double.class.equals(componentType)) {
//                return DoubleVectorTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (float.class.equals(componentType)) {
//                return FloatVectorTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (Boolean.class.equals(componentType)) {
//                return BooleanVectorTransfer.create(columnSource, instructions.getTargetPageSize());
//            } else if (short.class.equals(componentType)) {
//                return new ShortVectorTransfer(columnSource, instructions.getTargetPageSize());
//            } else if (char.class.equals(componentType)) {
//                return new CharVectorTransfer(columnSource, instructions.getTargetPageSize());
//            } else if (byte.class.equals(componentType)) {
//                return new ByteVectorTransfer(columnSource, instructions.getTargetPageSize());
            } else if (String.class.equals(componentType)) {
                return new StringVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
//            // else if (explicit codec provided)
//            // else if (big decimal)
//            // else if (big integer)
        }

        // Following will properly select the specific codec if assigned for this column, else will get the default
        final ObjectCodec<? super DATA_TYPE> codec = CodecLookup.lookup(columnDefinition, instructions);
        return new CodecTransfer<>(columnSource, codec, tableRowSet, instructions.getTargetPageSize());
    }

    // TODO Rewrite the comments for this file

    /**
     * Transfer one page size worth of fetched data into an internal buffer, which can then be accessed using
     * {@link TransferObject#getBuffer()}. The target page size is passed in the constructor.
     *
     * @return The number of fetched data entries copied into the buffer. This can be different from the total
     * number of entries fetched in case of variable-width types (e.g. strings) when used with additional
     * page size limits while copying.
     */
    int transferOnePageToBuffer();

    /**
     * Check if there is any fetched data which can be copied into buffer
     */
    boolean hasMoreDataToBuffer();

    /**
     * Get the buffer suitable for writing to a Parquet file
     *
     * @return the buffer
     */
    B getBuffer();

    // TODO Add comments here
    default IntBuffer getRepeatCount() {
        throw new UnsupportedOperationException("Only supported for array and vector transfer objects");
    }
}
