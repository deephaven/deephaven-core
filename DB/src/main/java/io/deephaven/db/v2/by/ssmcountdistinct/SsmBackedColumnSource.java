package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnSource} that provides {@link DbArrayBase DBArrays} of type T, backed by a same
 * typed {@link SegmentedSortedMultiSet}.
 *
 * @param <K> The SSM Type
 * @param <T> The provided Array type
 */
public interface SsmBackedColumnSource<K extends SegmentedSortedMultiSet, T extends DbArrayBase>
    extends ColumnSource<T> {

    /**
     * Create an appropriate instance for the specified type
     * 
     * @param type
     * @return
     */
    @SuppressWarnings("rawtypes")
    static SsmBackedColumnSource create(@NotNull final Class<?> type) {
        if (type == char.class || type == Character.class) {
            return new CharSsmBackedSource();
        } /*
           * else if(type == byte.class || type == Byte.class) { return new ByteSsmBackedSource(); }
           * else if(type == short.class || type == Short.class) { return new
           * ShortSsmBackedSource(); } else if(type == int.class || type == Integer.class) { return
           * new IntSsmBackedSource(); } else if(type == long.class || type == Long.class || type ==
           * DBDateTime.class) { return new LongSsmBackedSource(); } else if(type == float.class ||
           * type == Float.class) { return new FloatSsmBackedSource(); } else if(type ==
           * double.class || type == Double.class) { return new DoubleSsmBackedSource(); } else {
           * return new ObjectSsmBackedSource(type); }
           */
        throw new IllegalStateException("NOPE");
    }

    ObjectArraySource<K> getUnderlyingSource();

    /**
     * Get the current SSM at the specified key. This does not permute it in any way.
     *
     * @param key the key to get the ssm for.
     * @return the SSM
     */
    K getCurrentSsm(long key);

    /**
     * Get the ssm at the specified key, creating one if none existed. This method will update the
     * current previous tracking state of the SSM.
     *
     * @param key the key to get the ssm for.
     * @return the SSM at the key, or a new one.
     */
    K getOrCreate(long key);

    /**
     * Set the SSM at the specified key to null
     *
     * @param key the key to get the ssm for.
     */
    void clear(long key);

    /**
     * Ensure the source has at least `capacity` capacity
     * 
     * @param capacity the capacity to ensure.
     */
    void ensureCapacity(long capacity);

    /**
     * Clear out any tracked deltas from recent computations.
     *
     * @param indices the set of indices to clear deltas for.
     */
    void clearDeltas(Index indices);
}
