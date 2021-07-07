/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit UngroupedBoxedCharArrayColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * An Ungrouped Column sourced for the Boxed Type Byte.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedByteArrayColumnSource extends UngroupedColumnSource<Byte> implements MutableColumnSourceGetDefaults.ForObject<Byte> {
    private ColumnSource<Byte[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedByteArrayColumnSource(ColumnSource<Byte[]> innerSource) {
        super(Byte.class);
        this.innerSource = innerSource;
    }

    @Override
    public Byte get(long index) {
        final byte result = getByte(index);
        return (result == NULL_BYTE?null:result);
    }


    @Override
    public byte getByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        Byte[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_BYTE;
        }
        return array[offset];
    }


    @Override
    public Byte getPrev(long index) {
        final byte result = getPrevByte(index);
        return (result == NULL_BYTE?null:result);
    }

    @Override
    public byte getPrevByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
        Byte[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_BYTE;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
