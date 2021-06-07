package io.deephaven.db.v2.locations;

import io.deephaven.base.string.cache.CharSequenceAdapterBuilder;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * A simple implementation of TableKey for use in hash lookups, or as a parent class for complex sub-classes.
 */
public abstract class TableLookupKey<NAME_TYPE extends CharSequence> implements TableKey, Serializable {

    private static final long serialVersionUID = 1L;

    protected @NotNull final NAME_TYPE namespace;
    protected @NotNull final NAME_TYPE tableName;

    private @NotNull TableType tableType;

    private TableLookupKey(@NotNull final NAME_TYPE namespace,
                           @NotNull final NAME_TYPE tableName,
                           @NotNull final TableType tableType) {
        this.namespace = Require.neqNull(namespace, "namespace");
        this.tableName = Require.neqNull(tableName, "tableName");
        this.tableType = Require.neqNull(tableType, "tableType"); // Can't use setTableType due to its overrides
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableKey implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final NAME_TYPE getNamespace() {
        return namespace;
    }

    @Override
    public @NotNull final NAME_TYPE getTableName() {
        return tableName;
    }

    @Override
    public @NotNull final TableType getTableType() {
        return tableType;
    }

    void setTableType(@NotNull final TableType tableType) {
        this.tableType = Require.neqNull(tableType, "tableType");
    }

    //------------------------------------------------------------------------------------------------------------------
    // Immutable sub-class
    //------------------------------------------------------------------------------------------------------------------

    /**
     * An immutable TableLookupKey
     */
    public static final class Immutable extends TableLookupKey<String> implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * Create an immutable TableLookupKey.
         *
         * @param namespace the namespace
         * @param tableName the name within the namespace
         * @param tableType the type of table (permanent, intraday, user, etc.)
         */
        public Immutable(@NotNull final String namespace,
                         @NotNull final String tableName,
                         @NotNull final TableType tableType) {
            super(namespace, tableName, tableType);
        }

        /**
         * Create an immutable TableLookupKey from another TableKey.
         *
         * @param other the TableKey to copy.
         */
        public Immutable(@NotNull final TableKey other) {
            super(other.getNamespace().toString(), other.getTableName().toString(), other.getTableType());
        }

        @Override
        public String getImplementationName() {
            return "TableLookupKey.Immutable";
        }

        @Override
        public final void setTableType(@NotNull final TableType tableType) {
            throw new UnsupportedOperationException();
        }
        @Override
        public int hashCode() {
            return TableKey.hash(this);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof Immutable)) {
                return false;
            }
            return TableKey.areEqual(this, (Immutable) obj);
        }
    }

    public static TableLookupKey<String> getImmutableKey(@NotNull final TableKey tableKey) {
        return tableKey instanceof Immutable ? (Immutable)tableKey : new Immutable(tableKey);
    }

    //------------------------------------------------------------------------------------------------------------------
    // Re-usable sub-class
    //------------------------------------------------------------------------------------------------------------------

    public static final class Reusable extends TableLookupKey<CharSequenceAdapterBuilder> {

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("WeakerAccess")
        public Reusable() {
            // NB: The initial (non-null) value of tableType is arbitrary in this case.
            super(new CharSequenceAdapterBuilder(), new CharSequenceAdapterBuilder(), TableType.STANDALONE_SPLAYED);
        }

        @Override
        public String getImplementationName() {
            return "TableLookupKey.Reusable";
        }

        private Object writeReplace() throws ObjectStreamException {
            return getImmutableKey(this);
        }
    }
}
