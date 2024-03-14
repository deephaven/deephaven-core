//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit UngroupedBoxedCharArrayColumnSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * An Ungrouped Column sourced for the Boxed Type Byte.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedByteArrayColumnSource extends UngroupedColumnSource<Byte>
        implements MutableColumnSourceGetDefaults.ForObject<Byte> {
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
    public Byte get(long rowKey) {
        final byte result = getByte(rowKey);
        return (result == NULL_BYTE ? null : result);
    }


    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        long segment = rowKey >> base;
        int offset = (int) (rowKey & ((1 << base) - 1));
        Byte[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_BYTE;
        }
        return array[offset];
    }


    @Override
    public Byte getPrev(long rowKey) {
        final byte result = getPrevByte(rowKey);
        return (result == NULL_BYTE ? null : result);
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1 << getPrevBase()) - 1));
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
