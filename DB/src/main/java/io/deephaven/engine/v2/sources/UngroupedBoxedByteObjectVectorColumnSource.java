/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharObjectVectorColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.vector.ObjectVector;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * An Ungrouped Column sourced for the Boxed Type Byte.
 * <p>
 * The UngroupedBoxedC-harDbArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.v2.sources.Replicate.
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
