package io.deephaven.db.v2.locations;

import io.deephaven.base.string.cache.CharSequenceAdapterBuilder;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * A simple implementation of TableLocationKey for use in hash lookups.
 */
@SuppressWarnings("WeakerAccess")
public abstract class TableLocationLookupKey<TYPE extends CharSequence> implements TableLocationKey, Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull
    protected final TYPE internalPartition;
    @NotNull
    protected final TYPE columnPartition;

    private TableLocationLookupKey(@NotNull final TYPE internalPartition,
                                   @NotNull final TYPE columnPartition) {
        this.internalPartition = Require.neqNull(internalPartition, "internalPartition");
        this.columnPartition = Require.neqNull(columnPartition, "columnPartition");
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationKey implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final TYPE getInternalPartition() {
        return internalPartition;
    }

    @Override
    @NotNull
    public final TYPE getColumnPartition() {
        return columnPartition;
    }

    //------------------------------------------------------------------------------------------------------------------
    // Immutable sub-class
    //------------------------------------------------------------------------------------------------------------------

    public static final class Immutable extends TableLocationLookupKey<String> {

        private static final long serialVersionUID = 1L;

        public Immutable(@NotNull final String internalPartition,
                         @NotNull final String columnPartition) {
            super(internalPartition, columnPartition);
        }

        public Immutable(@NotNull final TableLocationKey other) {
            super(other.getInternalPartition().toString(), other.getColumnPartition().toString());
        }

        @Override
        public String getImplementationName() {
            return "TableLocationLookupKey.Immutable";
        }

        @Override
        public int hashCode() {
            return TableLocationKey.hash(this);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof Immutable)) {
                return false;
            }
            return TableLocationKey.areEqual(this, (Immutable) obj);
        }
    }

    public static TableLocationLookupKey<String> getImmutableKey(@NotNull final TableLocationKey locationKey) {
        return locationKey instanceof Immutable ? (Immutable)locationKey : new Immutable(locationKey);
    }

    //------------------------------------------------------------------------------------------------------------------
    // Re-usable sub-class
    //------------------------------------------------------------------------------------------------------------------

    public static class Reusable extends TableLocationLookupKey<CharSequenceAdapterBuilder> {

        private static final long serialVersionUID = 1L;

        public Reusable() {
            super(new CharSequenceAdapterBuilder(), new CharSequenceAdapterBuilder());
        }

        @Override
        public String getImplementationName() {
            return "TableLocationLookupKey.Reusable";
        }

        private Object writeReplace() throws ObjectStreamException {
            return getImmutableKey(this);
        }
    }
}
