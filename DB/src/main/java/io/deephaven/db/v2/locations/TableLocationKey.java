package io.deephaven.db.v2.locations;

import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.string.cache.StringCompatible;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * Interface that specifies key fields for any TableLocation.
 */
public interface TableLocationKey extends NamedImplementation, LogOutputAppendable {

    // TODO: Add support for arbitrary-dimension partitioning, with zero or multiple internal and column partitions, possibly interleaved.

    /**
     * Constant used to designate partition values for unpartitioned tables, i.e. "simple" tables.
     * Also used for fully-standalone locations, e.g. for tables persisted or loaded outside of a
     * Database/TableDataService framework.
     */
    String NULL_PARTITION = "\0";

    /**
     * @return The internal partition that encloses the identified table location, or null if none exists
     */
    @NotNull CharSequence getInternalPartition();

    /**
     * @return The column partition that encloses the identified table location, or null if none exists
     */
    @NotNull CharSequence getColumnPartition();

    //------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    //------------------------------------------------------------------------------------------------------------------

    @Override
    default LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getImplementationName())
                .append('[').append(getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition())
                .append('/').append(getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                .append(']');
    }

    default String toStringHelper() {
        return getImplementationName()
                + '[' + (getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition())
                + '/' + (getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                + ']';
    }

    //------------------------------------------------------------------------------------------------------------------
    // KeyedObjectKey implementation
    //------------------------------------------------------------------------------------------------------------------

    abstract class KeyedObjectKeyImpl<VALUE_TYPE> implements KeyedObjectKey<TableLocationKey, VALUE_TYPE> {

        @Override
        public final int hashKey(@NotNull final TableLocationKey key) {
            // NB: This approach matches Objects.hash(Object... values), minus the boxing for varargs.
            return hash(key);
        }

        @Override
        public final boolean equalKey(@NotNull final TableLocationKey key,
                                      @NotNull final VALUE_TYPE value) {
            return areEqual(key, getKey(value));
        }
    }

    static int hash(@NotNull final TableLocationKey key) {
        return (31
                + StringCompatible.hash(key.getInternalPartition())) * 31
                + StringCompatible.hash(key.getColumnPartition());
    }

    static boolean areEqual(@NotNull final TableLocationKey key1, @NotNull final TableLocationKey key2) {
        if (key1 == key2) {
            return true;
        }
        return CharSequenceUtils.contentEquals(key1.getInternalPartition(), key2.getInternalPartition())
                && CharSequenceUtils.contentEquals(key1.getColumnPartition(), key2.getColumnPartition());
    }

    final class SelfKeyedObjectKeyImpl<VALUE_TYPE extends TableLocationKey> extends KeyedObjectKeyImpl<VALUE_TYPE> {

        private static final KeyedObjectKey<TableLocationKey, ? extends TableLocationKey> INSTANCE = new SelfKeyedObjectKeyImpl<>();

        private SelfKeyedObjectKeyImpl() {
        }

        @Override
        public TableLocationKey getKey(@NotNull final VALUE_TYPE value) {
            return value;
        }
    }

    /**
     * Re-usable key object for TableLocationKeys that disregards the implementation class.
     */
    static <VALUE_TYPE extends TableLocationKey> KeyedObjectKey<TableLocationKey, VALUE_TYPE> getKeyedObjectKey() {
        //noinspection unchecked
        return (KeyedObjectKey<TableLocationKey, VALUE_TYPE>) SelfKeyedObjectKeyImpl.INSTANCE;
    }

    //------------------------------------------------------------------------------------------------------------------
    // Comparator/Comparable implementation
    //------------------------------------------------------------------------------------------------------------------

    final class ComparatorImpl implements Comparator<TableLocationKey> {

        private ComparatorImpl() {
        }

        @Override
        public int compare(@NotNull final TableLocationKey tableLocationKey1,
                           @NotNull final TableLocationKey tableLocationKey2) {
            if (tableLocationKey1 == tableLocationKey2) {
                return 0;
            }
            int comparison;
            // NB: It's important for various applications that column partitions compare at higher priority than internal partitions.
            return 0 != (comparison = comparePartitions(tableLocationKey1.getColumnPartition(),   tableLocationKey2.getColumnPartition())  ) ? comparison :
                   0 != (comparison = comparePartitions(tableLocationKey1.getInternalPartition(), tableLocationKey2.getInternalPartition())) ? comparison :
                   0;
        }

        private static int comparePartitions(@NotNull final CharSequence partition1,
                                             @NotNull final CharSequence partition2) {
            if (partition1 == partition2) {
                return 0;
            }
            return CharSequenceUtils.CASE_SENSITIVE_COMPARATOR.compare(partition1, partition2);
        }
    }

    /**
     * Re-usable Comparator for TableLocationKey objects that disregards the implementation class.
     */
    Comparator<TableLocationKey> COMPARATOR = new ComparatorImpl();
}
