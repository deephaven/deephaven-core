package io.deephaven.db.v2.locations;

import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.string.cache.StringCompatible;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;


/**
 * Interface that specifies key fields for any table-keyed object (i.e. TableLocationProvider).
 */
public interface TableKey extends NamedImplementation, LogOutputAppendable {

    // TODO: Add support for arbitrary n-tuple keys.

    /**
     * Constant used to designate namespace or table name values for fully-standalone tables/locations,
     * e.g. for tables persisted or loaded outside of a Database/TableDataService framework.
     */
    String NULL_NAME = "\0";

    /**
     * @return The namespace enclosing this table
     */
    @NotNull CharSequence getNamespace();

    /**
     * @return The name of this table
     */
    @NotNull CharSequence getTableName();

    /**
     * @return The type of this table (which generally dictates storage, metadata-availability, etc)
     */
    @NotNull TableType getTableType();

    //------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getImplementationName())
                .append('[').append(getNamespace())
                .append('/').append(getTableName())
                .append('/').append(getTableType().getDescriptor())
                .append(']');
    }

    @FinalDefault
    default String toStringHelper() {
        return getImplementationName()
                + '[' + getNamespace()
                + '/' + getTableName()
                + '/' + getTableType().getDescriptor()
                + ']';
    }

    /**
     * Optional toString path with more implementation detail.
     * @return detailed conversion to string
     */
    default String toStringDetailed() {
        return toStringHelper();
    }

    //------------------------------------------------------------------------------------------------------------------
    // KeyedObjectKey implementation for TableLocationProvider
    //------------------------------------------------------------------------------------------------------------------

    abstract class KeyedObjectKeyImpl<VALUE_TYPE> implements KeyedObjectKey<TableKey, VALUE_TYPE> {

        @Override
        public int hashKey(@NotNull final TableKey key) {
            // NB: This approach matches Objects.hash(Object... values), minus the boxing for varargs.
            return hash(key);
        }

        @Override
        public boolean equalKey(@NotNull final TableKey key,
                                @NotNull final VALUE_TYPE value) {
            return areEqual(key, getKey(value));
        }
    }

    static int hash(@NotNull final TableKey key) {
        return ((31
                + StringCompatible.hash(key.getNamespace())) * 31
                + StringCompatible.hash(key.getTableName())) * 31
                + key.getTableType().hashCode();
    }

    static boolean areEqual(@NotNull final TableKey key1, @NotNull final TableKey key2) {
        if (key1 == key2) {
            return true;
        }
        return CharSequenceUtils.contentEquals(key1.getNamespace(), key2.getNamespace())
                && CharSequenceUtils.contentEquals(key1.getTableName(), key2.getTableName())
                && key1.getTableType() == key2.getTableType();
    }

    final class SelfKeyedObjectKeyImpl<VALUE_TYPE extends TableKey> extends KeyedObjectKeyImpl<VALUE_TYPE> {

        private static final KeyedObjectKey<TableKey, ? extends TableKey> INSTANCE = new SelfKeyedObjectKeyImpl<>();

        private SelfKeyedObjectKeyImpl() {
        }

        @Override
        public TableKey getKey(@NotNull final VALUE_TYPE value) {
            return value;
        }
    }

    /**
     * Re-usable key object for TableKeys that disregards the implementation class.
     */
    static <VALUE_TYPE extends TableKey> KeyedObjectKey<TableKey, VALUE_TYPE> getKeyedObjectKey() {
        //noinspection unchecked
        return (KeyedObjectKey<TableKey, VALUE_TYPE>) SelfKeyedObjectKeyImpl.INSTANCE;
    }
}
