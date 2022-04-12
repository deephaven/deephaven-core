/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * An Ungrouped Column sourced for the Boxed Type Byte.
 * <p>
 * The UngroupedBoxedC-harVectorColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedByteObjectVectorColumnSource extends UngroupedObjectVectorColumnSource<Byte> {

    public UngroupedBoxedByteObjectVectorColumnSource(ColumnSource<ObjectVector<Byte>> innerSource) {
        super(innerSource);
    }

    @Override
    public byte getByte(long index) {
        final Byte result = get(index);
        return result == null ? NULL_BYTE : result;
    }

    @Override
    public byte getPrevByte(long index) {
        final Byte result = getPrev(index);
        return result == null ? NULL_BYTE : result;
    }
}
