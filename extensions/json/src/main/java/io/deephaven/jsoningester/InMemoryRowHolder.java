/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.util.type.TypeUtils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds data in memory simulating a single row of Deephaven data.
 * Intended for use caching data that can be expensive to process but can be multithreaded, and has to be
 * held for a fixed ordering of writes.
 */
class InMemoryRowHolder {

    private final Object[] data;
    private int dataPosition = 0;
    private long messageNumber;
    private boolean isEmpty = false;
    private String originalText = null;
    private Exception parseException = null;
    private Row.Flags flags = Row.Flags.SingleRow;

    private final Map<String, SingleRowSetter> setters = new HashMap<>();

    public InMemoryRowHolder(final int numHolders) {
        data = new Object[numHolders];
    }

    /**
     * Create a new instance of this class.
     *
     * @param columnName The name of the Deephaven column this RowSetter will emulate
     * @param type       The class of the Deephaven column this RowSetter will emulate
     * @return a RowSetter that stores data to an in-memory store only
     */
    public @SuppressWarnings("rawtypes") SingleRowSetter getSetter(final String columnName, @SuppressWarnings("rawtypes") final Class type) {
        return setters.computeIfAbsent(columnName, setter -> new SingleRowSetter(type));
    }

    public long getMessageNumber() {
        return messageNumber;
    }

    public void setMessageNumber(final long messageNumber) {
        this.messageNumber = messageNumber;
    }

    /**
     * Indicate whether this holder is a placeholder to indicate a message with no elements.
     * @return True if this holder should not cause a row to be created in the database, false otherwise.
     */
    public boolean getIsEmpty() { return isEmpty; }

    public void setIsEmpty(final boolean isEmpty) { this.isEmpty = isEmpty; }

    /**
     * Returns a caught exception that occurred during parsing this message.
     * @return the exception that occurred while processing this message, null if no exception occurred
     */
    public Exception getParseException() {
        return parseException;
    }

    public void setParseException(Exception parseException) {
        this.parseException = parseException;
    }

    /**
     * The original message text for error reporting.
     * @return the original message text
     */
    public String getOriginalText() {
        return originalText;
    }

    public void setOriginalText(String originalText) {
        this.originalText = originalText;
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored. Note that we DO allow null Booleans
     */
    public Boolean getBoolean(final int position) {
        return (Boolean)data[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public byte getByte(final int position) {
        return TypeUtils.unbox((Byte)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public char getChar(final int position) {
        return TypeUtils.unbox((Character)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public double getDouble(final int position) {
        return TypeUtils.unbox((Double)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public float getFloat(final int position) {
        return TypeUtils.unbox((Float)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public int getInt(final int position) {
        return TypeUtils.unbox((Integer)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public long getLong(final int position) {
        return TypeUtils.unbox((Long)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public short getShort(final int position) {
        return TypeUtils.unbox((Short)data[position]);
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The name of the Deephaven column storing this datum.
     * @return The value that was previously stored.
     */
    public Object getObject(final int position) {
        return data[position];
    }

    public Object getObject(final String columnName) { return data[setters.get(columnName).getThisPosition()];}

    public Row.Flags getFlags() {
        return flags;
    }

    public void startTransaction() {
        this.flags = Row.Flags.StartTransaction;
    }
    public void inTransaction() {
        this.flags = Row.Flags.None;
    }
    public void endTransaction() {
        this.flags = Row.Flags.EndTransaction;
    }
    public void singleRow() {
        this.flags = Row.Flags.SingleRow;
    }

    public void copyDataFrom(final InMemoryRowHolder existingHolder, final int startingPosition) {
        System.arraycopy(existingHolder.data, 0, data, 0, startingPosition);
        existingHolder.setters.entrySet().stream().filter((e -> e.getValue().thisPosition < startingPosition))
                .sorted(Comparator.comparingInt(e -> e.getValue().thisPosition))
                .forEach(e -> setters.put(e.getKey(), e.getValue()));
        dataPosition = startingPosition;
        messageNumber = existingHolder.messageNumber;
        parseException = existingHolder.parseException;
    }

    public int getDataPosition() {
        return dataPosition;
    }

    /**
     * A RowSetter implementation that stores data for a single row into an in-memory store.
     */
    @SuppressWarnings("rawtypes")
    class SingleRowSetter implements RowSetter {
        @SuppressWarnings("rawtypes") private final Class type;
        private final int thisPosition;

        /**
         * Create a new instance of this class.
         *
         * @param type      The class of the Deephaven column being populated.
         */
        public SingleRowSetter(@SuppressWarnings("rawtypes") final Class type) {
            this.type = type;
            this.thisPosition = dataPosition++;
        }

        int getThisPosition() {
            return thisPosition;
        }

        @Override
        public void set(final Object value) {
            data[thisPosition] = value;
        }

        @Override
        public void setBoolean(final Boolean value) {
            data[thisPosition] = value;
        }

        @Override
        public void setByte(final byte value) {
            data[thisPosition] = value;
        }

        @Override
        public void setChar(final char value) {
            data[thisPosition] = value;
        }

        @Override
        public void setDouble(final double value) {
            data[thisPosition] = value;
        }

        @Override
        public void setFloat(final float value) {
            data[thisPosition] = value;
        }

        @Override
        public void setInt(final int value) {
            data[thisPosition] = value;
        }

        @Override
        public void setLong(final long value) {
            data[thisPosition] = value;
        }

        @Override
        public void setShort(final short value) {
            data[thisPosition] = value;
        }

        @Override
        public @SuppressWarnings("rawtypes") Class getType() {
            return type;
        }
    }
}
