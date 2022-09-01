/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.util.QueryConstants;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.IntBuffer;

/**
 * The base class for writing vectors into individual pages.
 *
 * @param <BUFFER_TYPE>
 */
public abstract class AbstractBulkValuesWriter<BUFFER_TYPE> extends ValuesWriter
        implements BulkWriter<BUFFER_TYPE> {
    private static final int RL_FIRST_ELEM = 0;
    private static final int RL_CONSECUTIVE_ELEM = 1;

    // These DL values are only relevant for Vector types
    private static final int DL_VECTOR_NULL_VECTOR = 0;
    private static final int DL_VECTOR_EMPTY_VECTOR = 1;
    private static final int DL_VECTOR_NULL_ELEMENT = 2;
    private static final int DL_VECTOR_ACTUAL_VALUE = 3;

    // These DL values are only relevant for individual items
    static final int DL_ITEM_NULL = 0;
    static final int DL_ITEM_PRESENT = 1;

    @Override
    public int writeBulkVector(@NotNull final BUFFER_TYPE bulkValues,
            @NotNull final IntBuffer vectorSizes,
            @NotNull final RunLengthBitPackingHybridEncoder rlEncoder,
            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
            final int nonNullValueCount) throws IOException {
        final IntBuffer nullsOffsets = writeBulkFilterNulls(bulkValues, nonNullValueCount).nullOffsets;
        return applyDlAndRl(vectorSizes, rlEncoder, dlEncoder, nullsOffsets);
    }

    /**
     * <p>
     * Compute the definition levels and repetition levels. These are how Parquet encodes data that has a complex
     * structure. In this case, the most complex structure we encode are Deephaven Vectors.
     * </p>
     * <br/>
     *
     * <p>
     * Definition and Repetition levels are used to define how the complex structure is encoded into multiple columns.
     * Repetition levels define the number of common prefix elements an item shares with its immediate predecessor,
     * where the value 0 represents the root node of a single instance of a structure. The best way to think about this
     * is to understand how it is read back - see
     * <a href="https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36632.pdf">Dremel Paper</a> pgs 3
     * and 4.
     * </p>
     * <br/>
     *
     * <p>
     * Basically, take the data structure being encoded and draw a Finite State Machine. If a repetition level less than
     * the depth of that element is read, it means that the value belongs to a different instance of the parent item,
     * and processing should advance to the next field. If the value is allowed to be repeated, and the RL is equal to
     * the current field depth then keep reading values from the same field.
     * </p>
     * <br/>
     *
     * <p>
     * In our case, we are storing Vectors, which are only this
     * </p>
     * 
     * <pre>
     * message Vector {
     *     repeated int32 value
     * }
     * </pre>
     *
     * <p>
     * So we can have a repetition level of 0 or 1, where 0 indicates the first element of a new vector and 1 indicates
     * another entry in the current vector.
     * </p>
     *
     * <p>
     * Definition levels are used to encode how many preceeding optional fields are defined at a given point. For
     * Vectors, as defined above, this gives us three different possible values for the definition value of a single
     * item:
     * <ul>
     * 0 - The array itself at this position is a null 1 - The array exists, but is empty 2 - The element at this
     * position in the array is null 3 - The value in the array exists and is non null
     * </ul>
     * </p>
     *
     * @param vectorSizes a buffer containing the sizes of each vector being written
     * @param rlEncoder the encoder for repetition levels
     * @param dlEncoder the encoder for definition levels
     * @param nullsOffsets a buffer containing the offset of the next null value.
     *
     * @return the total number of values written.
     * @throws IOException if writing failed
     */
    int applyDlAndRl(@NotNull final IntBuffer vectorSizes,
            @NotNull final RunLengthBitPackingHybridEncoder rlEncoder,
            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
            @NotNull final IntBuffer nullsOffsets) throws IOException {
        int valueCount = 0;
        int leafCount = 0;

        nullsOffsets.flip();
        int nextNullOffset = nullsOffsets.hasRemaining() ? nullsOffsets.get() : Integer.MAX_VALUE;
        while (vectorSizes.hasRemaining()) {
            int length = vectorSizes.get();
            if (length != QueryConstants.NULL_INT) {
                if (length == 0) {
                    dlEncoder.writeInt(DL_VECTOR_EMPTY_VECTOR);
                } else {
                    if (leafCount == nextNullOffset) {
                        nextNullOffset = nullsOffsets.hasRemaining() ? nullsOffsets.get() : Integer.MAX_VALUE;
                        dlEncoder.writeInt(DL_VECTOR_NULL_ELEMENT);
                    } else {
                        dlEncoder.writeInt(DL_VECTOR_ACTUAL_VALUE);
                    }
                    leafCount++;
                }

                valueCount++;
                rlEncoder.writeInt(RL_FIRST_ELEM);
                for (int i = 1; i < length; i++) {
                    if (leafCount++ == nextNullOffset) {
                        nextNullOffset = nullsOffsets.hasRemaining() ? nullsOffsets.get() : Integer.MAX_VALUE;
                        dlEncoder.writeInt(DL_VECTOR_NULL_ELEMENT);
                    } else {
                        dlEncoder.writeInt(DL_VECTOR_ACTUAL_VALUE);
                    }
                    rlEncoder.writeInt(RL_CONSECUTIVE_ELEM);
                    valueCount++;
                }
            } else {
                valueCount++;
                dlEncoder.writeInt(DL_VECTOR_NULL_VECTOR);
                rlEncoder.writeInt(RL_FIRST_ELEM);
            }
        }
        return valueCount;
    }
}
