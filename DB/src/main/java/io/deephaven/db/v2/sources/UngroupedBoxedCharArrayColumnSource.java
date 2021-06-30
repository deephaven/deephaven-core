package io.deephaven.db.v2.sources;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * An Ungrouped Column sourced for the Boxed Type Character.
 * <p>
 * The UngroupedBoxedC-harArrayColumnSource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class UngroupedBoxedCharArrayColumnSource extends UngroupedColumnSource<Character> implements MutableColumnSourceGetDefaults.ForObject<Character> {
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
    public Character get(long index) {
        final char result = getChar(index);
        return (result == NULL_CHAR?null:result);
    }


    @Override
    public char getChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        long segment = index>>base;
        int offset = (int) (index & ((1<<base) - 1));
        Character[] array = innerSource.get(segment);
        if (array == null || offset >= array.length || array[offset] == null) {
            return NULL_CHAR;
        }
        return array[offset];
    }


    @Override
    public Character getPrev(long index) {
        final char result = getPrevChar(index);
        return (result == NULL_CHAR?null:result);
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        long segment = index>> getPrevBase();
        int offset = (int) (index & ((1<< getPrevBase()) - 1));
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
