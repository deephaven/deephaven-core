/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.tablelogger;

import io.deephaven.util.type.TypeUtils;

import java.io.IOException;

/**
 * Interface for writing out values in a row.
 */
public interface Row {
    /**
     * Gets a setter for a column.
     *
     * @param name column name
     * @return setter for the column.
     */
    RowSetter getSetter(String name);

    /**
     * Gets a typed setter for a column.
     *
     * @param name column name
     * @param tClass the type for the typed RowSetter
     * @return setter for the column.
     */
    default <T> RowSetter<T> getSetter(String name, Class<T> tClass) {
        RowSetter setter = getSetter(name);

        final Class unboxedType;
        // noinspection unchecked
        if (tClass.isAssignableFrom(setter.getType())
            || (unboxedType = TypeUtils.getUnboxedType(tClass)) != null
                && unboxedType.isAssignableFrom(setter.getType())) {
            // noinspection unchecked
            return (RowSetter<T>) setter;
        }
        throw new ClassCastException(name + " is of type " + setter.getType() + ", not of type "
            + tClass + (unboxedType == null ? "" : " or " + unboxedType));
    }

    /**
     * Writes out a new row (values set using setters).
     *
     * @throws IOException problem writing the row
     */
    void writeRow() throws IOException;

    /**
     * Number of rows written out.
     *
     * @deprecated {@link Row#size()} is somewhat ambiguously specified in the interface and its
     *             implementations. Some implementations keep track of all rows written. Others keep
     *             track of number of rows buffered.
     *             <p>
     *             It seems safer to simply not allow the question to be asked.
     *
     * @return number of rows written out.
     */
    @Deprecated
    long size();

    /**
     * Per-row transaction flags.
     *
     * In Deephaven, a transaction is a group of rows that must be made visible to applications
     * entirely, or not at all.
     */
    enum Flags {
        /** This row does not start or stop a transaction. */
        None,
        /** This row is the first row in a transaction. */
        StartTransaction,
        /** This row is the last row in a transaction. */
        EndTransaction,
        /** This row is the only row in a transaction. */
        SingleRow,
    }

    void setFlags(Flags flags);

    /**
     * For rows that are to be used with file managers that allow dynamic column partition
     * selection, set the column partition value.
     *
     * @param columnPartitionValue the column partition value
     */
    default void setColumnPartitionValue(final String columnPartitionValue) {
        throw new UnsupportedOperationException(
            "Default Row implementation does not support setColumnPartitionValue()");
    }

    /**
     * For rows that are to be used with file managers that allow dynamic column partition
     * selection, retrieve the column partition value.
     *
     * @return the previously-set column partition value
     */
    default String getColumnPartitionValue() {
        throw new UnsupportedOperationException(
            "Default Row implementation does not support getColumnPartitionValue()");
    }
}
