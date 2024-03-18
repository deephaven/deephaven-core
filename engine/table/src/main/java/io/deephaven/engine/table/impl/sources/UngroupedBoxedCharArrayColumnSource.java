//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * An Ungrouped Column sourced for the Boxed Type Character.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedCharArrayColumnSource extends UngroupedColumnSource<Character>
        implements MutableColumnSourceGetDefaults.ForObject<Character> {
    private ColumnSource<Character[]> innerSource;

    @Override
    public Class<?> getComponentType() {
        return null;
    }


    public UngroupedBoxedCharArrayColumnSource(ColumnSource<Character[]> innerSource) {
        super(Character.class);
        this.innerSource = innerSource;
    }

    @Override
    public Character get(long rowKey) {
        final char result = getChar(rowKey);
        return (result == NULL_CHAR ? null : result);
    }


    @Override
    public char getChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        long segment = rowKey >> base;
        int offset = (int) (rowKey & ((1 << base) - 1));
        Character[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_CHAR;
        }
        return array[offset];
    }


    @Override
    public Character getPrev(long rowKey) {
        final char result = getPrevChar(rowKey);
        return (result == NULL_CHAR ? null : result);
    }

    @Override
    public char getPrevChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        long segment = rowKey >> getPrevBase();
        int offset = (int) (rowKey & ((1 << getPrevBase()) - 1));
        Character[] array = innerSource.getPrev(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_CHAR;
        }
        return array[offset];
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }
}
