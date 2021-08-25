/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.tablelogger;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Interface for writing table data out.
 */
public interface TableWriter<R extends Row> extends Row {

    RowSetter NULL_SETTER = new RowSetter() {
        @Override
        public void set(Object value) {}

        @Override
        public void setBoolean(Boolean value) {}

        @Override
        public void setByte(byte value) {}

        @Override
        public void setChar(char value) {}

        @Override
        public void setDouble(double value) {}

        @Override
        public void setFloat(float value) {}

        @Override
        public void setInt(int value) {}

        @Override
        public void setLong(long value) {}

        @Override
        public void setShort(short value) {}

        @Override
        public Class getType() {
            return null;
        }
    };

    /**
     * {@inheritDoc}
     * <p>
     * The implementation is likely to delegate to {@link Row#getSetter(String)} in a default Row instance.
     */
    @Override // Row
    RowSetter getSetter(String name);

    /**
     * {@inheritDoc}
     * <p>
     * The implementation is likely to delegate to {@link Row#getSetter(String, Class)} in a default Row instance.
     */
    @Override // Row
    default <T> RowSetter<T> getSetter(@NotNull final String name, @NotNull final Class<T> tClass) {
        // use the inherited default implementation
        return Row.super.getSetter(name, tClass);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The implementation is likely to delegate to {@link Row#setFlags(Flags)} in a default Row instance.
     */
    @Override // Row
    void setFlags(Row.Flags flags);

    /**
     * Get a writer for a Row entries. This is likely to be newly created, so callers should cache this value. In
     * practice, TableWriter implementations generally cache the result of the first call to this method as a primary
     * writer.
     *
     * @return a Row, likely newly created
     */
    R getRowWriter();

    /**
     * {@inheritDoc}
     * <p>
     * The implementation is likely to delegate to {@link Row#writeRow()} in a default Row instance.
     *
     * @implNote This method is used as part of the import portion of the table recording process in TableListeners
     */
    @Override // Row
    void writeRow() throws IOException;

    /**
     * Closes the writer.
     *
     * @throws IOException problem closing the writer.
     */
    void close() throws IOException;

    /**
     * Gets the column types for the table.
     *
     * @return column types for the table.
     */
    Class[] getColumnTypes();

    /**
     * Gets the column names for the table.
     *
     * @return column names for the table.
     */
    String[] getColumnNames();

    /**
     * Flushes data out.
     *
     * @throws IOException problem flushing data out.
     */
    void flush() throws IOException;
}
