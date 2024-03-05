/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.jsoningester.msg.MessageMetadata;
import io.deephaven.tablelogger.Row;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * Holds data in memory simulating a single row of Deephaven data. Intended for use caching data that can be expensive
 * to process but can be multithreaded, and has to be held for a fixed ordering of writes.
 */
class InMemoryRowHolder {

    // TODO: generate row holders specific to the adapter's field setter types? or just use primitive arrs?
    private final boolean[] dataBooleans;
    private final char[] dataChars;
    private final byte[] dataBytes;
    private final short[] dataShorts;
    private final int[] dataInts;
    private final float[] dataFloats;
    private final long[] dataLongs;
    private final double[] dataDoubles;
    private final Object[] dataObjects;

    private long messageNumber;
    private boolean isEmpty = false;
    private String originalText = null;
    private Exception parseException = null;
    private Row.Flags flags = Row.Flags.SingleRow;

    public InMemoryRowHolder(
            final int nBooleans,
            final int nChars,
            final int nBytes,
            final int nShorts,
            final int nInts,
            final int nFloats,
            final int nLongs,
            final int nDoubles,
            final int nObjects) {
        dataBooleans = nBooleans == 0 ? null : new boolean[nBooleans];
        dataChars = nChars == 0 ? null : new char[nChars];
        dataBytes = nBytes == 0 ? null : new byte[nBytes];
        dataShorts = nShorts == 0 ? null : new short[nShorts];
        dataInts = nInts == 0 ? null : new int[nInts];
        dataFloats = nFloats == 0 ? null : new float[nFloats];
        dataLongs = nLongs == 0 ? null : new long[nLongs];
        dataDoubles = nDoubles == 0 ? null : new double[nDoubles];
        dataObjects = nObjects == 0 ? null : new Object[nObjects];
    }

    /**
     * Returns the {@link MessageMetadata#getMsgNo() monotonically-increasing sequence number} from the message that
     * corresponds to this row.
     *
     * @return The message number from the original message that corresponds to this row.
     */
    public long getMessageNumber() {
        return messageNumber;
    }

    public void setMessageNumber(final long messageNumber) {
        this.messageNumber = messageNumber;
    }

    /**
     * Indicate whether this holder is a placeholder to indicate a message with no elements. Empty holders are used to
     * ensure every {@link #getMessageNumber() sequence number} is processed.
     *
     * @return True if this holder should not cause a row to be created in the table, false otherwise.
     */
    public boolean getIsEmpty() {
        return isEmpty;
    }

    public void setIsEmpty(final boolean isEmpty) {
        this.isEmpty = isEmpty;
    }

    /**
     * Returns a caught exception that occurred during parsing this message.
     *
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
     *
     * @return the original message text
     */
    public String getOriginalText() {
        return originalText;
    }

    public void setOriginalText(String originalText) {
        this.originalText = originalText;
    }

    public boolean getBooleanPrimitive(final int position) {
        return dataBooleans[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public byte getByte(final int position) {
        return dataBytes[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public char getChar(final int position) {
        return dataChars[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public double getDouble(final int position) {
        return dataDoubles[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public float getFloat(final int position) {
        return dataFloats[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public int getInt(final int position) {
        return dataInts[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public long getLong(final int position) {
        return dataLongs[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The position within the data array for this data
     * @return The value that was previously stored.
     */
    public short getShort(final int position) {
        return dataShorts[position];
    }

    /**
     * Get a value that was previously stored
     *
     * @param position The name of the Deephaven column storing this datum.
     * @return The value that was previously stored.
     */
    public Object getObject(final int position) {
        return dataObjects[position];
    }

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

    public void copyDataFrom(
            final InMemoryRowHolder existingHolder,
            final int startingPosBooleans,
            final int startingPosChars,
            final int startingPosBytes,
            final int startingPosShorts,
            final int startingPosInts,
            final int startingPosFloats,
            final int startingPosLongs,
            final int startingPosDoubles,
            final int startingPosObjects) {
        if (startingPosBooleans > 0)
            System.arraycopy(existingHolder.dataBooleans, 0, dataBooleans, 0, startingPosBooleans);
        if (startingPosChars > 0)
            System.arraycopy(existingHolder.dataChars, 0, dataChars, 0, startingPosChars);
        if (startingPosBytes > 0)
            System.arraycopy(existingHolder.dataBytes, 0, dataBytes, 0, startingPosBytes);
        if (startingPosShorts > 0)
            System.arraycopy(existingHolder.dataShorts, 0, dataShorts, 0, startingPosShorts);
        if (startingPosInts > 0)
            System.arraycopy(existingHolder.dataInts, 0, dataInts, 0, startingPosInts);
        if (startingPosFloats > 0)
            System.arraycopy(existingHolder.dataFloats, 0, dataFloats, 0, startingPosFloats);
        if (startingPosLongs > 0)
            System.arraycopy(existingHolder.dataLongs, 0, dataLongs, 0, startingPosLongs);
        if (startingPosDoubles > 0)
            System.arraycopy(existingHolder.dataDoubles, 0, dataDoubles, 0, startingPosDoubles);
        if (startingPosObjects > 0)
            System.arraycopy(existingHolder.dataObjects, 0, dataObjects, 0, startingPosObjects);

        messageNumber = existingHolder.messageNumber;
        parseException = existingHolder.parseException;
    }

    /**
     * A RowSetter implementation that stores data for a single row into an in-memory store.
     */
    static class SingleRowSetter {
        private final int thisPosition;


        /**
         * Position (within the {@link InMemoryRowHolder} data array this function will be used with) at which this
         * setter will store inputs.
         * <p>
         * It is the caller's responsibility to only use each {@code SingleRowSetter} with the correct corresponding
         * type.
         *
         * @param thisPosition The index in the data array to which this will set values.
         */
        public SingleRowSetter(int thisPosition) {
            this.thisPosition = thisPosition;
        }

        int getThisPosition() {
            return thisPosition;
        }

        public void set(final InMemoryRowHolder holder, final Object value) {
            holder.dataObjects[thisPosition] = value;
        }

        public void setInstant(final InMemoryRowHolder holder, final Instant value) {
            holder.dataLongs[thisPosition] = DateTimeUtils.epochNanos(value);
        }

        public void setBooleanPrimitive(final InMemoryRowHolder holder, final boolean value) {
            holder.dataBooleans[thisPosition] = value;
        }

        public void setBooleanObj(final InMemoryRowHolder holder, final Boolean value) {
            holder.dataBytes[thisPosition] = BooleanUtils.booleanAsByte(value);
        }

        public void setByte(final InMemoryRowHolder holder, final byte value) {
            holder.dataBytes[thisPosition] = value;
        }

        public void setChar(final InMemoryRowHolder holder, final char value) {
            holder.dataChars[thisPosition] = value;
        }

        public void setDouble(final InMemoryRowHolder holder, final double value) {
            holder.dataDoubles[thisPosition] = value;
        }

        public void setFloat(final InMemoryRowHolder holder, final float value) {
            holder.dataFloats[thisPosition] = value;
        }

        public void setInt(final InMemoryRowHolder holder, final int value) {
            holder.dataInts[thisPosition] = value;
        }

        public void setLong(final InMemoryRowHolder holder, final long value) {
            holder.dataLongs[thisPosition] = value;
        }

        public void setShort(final InMemoryRowHolder holder, final short value) {
            holder.dataShorts[thisPosition] = value;
        }

    }

    static class SetterFactory {
        private int nextBooleanPos = 0;
        private int nextCharPos = 0;
        private int nextBytePos = 0;
        private int nextShortPos = 0;
        private int nextIntPos = 0;
        private int nextFloatPos = 0;
        private int nextLongPos = 0;
        private int nextDoublePos = 0;
        private int nextObjectPos = 0;

        /**
         * Whether this factory is "frozen" so that no new setters may be added.
         */
        private boolean frozen = false;

        SingleRowSetter createNextSetter(Class<?> type) {
            if (frozen) {
                throw new IllegalStateException("SetterFactory is frozen!");
            }

            int pos;
            if (type == boolean.class) {
                pos = nextBooleanPos++;
            } else if (type == Boolean.class) {
                // booleans are stored as bytes (io.deephaven.stream.StreamChunkUtils.replacementType)
                pos = nextBytePos++;
            } else if (type == char.class || type == Character.class) {
                pos = nextCharPos++;
            } else if (type == byte.class || type == Byte.class) {
                pos = nextBytePos++;
            } else if (type == short.class || type == Short.class) {
                pos = nextShortPos++;
            } else if (type == int.class || type == Integer.class) {
                pos = nextIntPos++;
            } else if (type == long.class || type == Long.class) {
                pos = nextLongPos++;
            } else if (type == float.class || type == Float.class) {
                pos = nextFloatPos++;
            } else if (type == double.class || type == Double.class) {
                pos = nextDoublePos++;
            } else if (type == Instant.class) {
                // Instants are stored as longs (io.deephaven.stream.StreamChunkUtils.replacementType)
                pos = nextLongPos++;
            } else {
                pos = nextObjectPos++;
            }

            return new SingleRowSetter(pos);
        }

        /**
         * Returns a Supplier that creates new InMemoryRowHolders with data arrays sized correctly for the number of
         * setters that have been prodcued by this {@code SetterFactory}.
         * 
         * @return A Supplier of {@link InMemoryRowHolder InMemoryRowHolders} appropriate for this factory's setters
         */
        public Supplier<InMemoryRowHolder> createRowHolderFactory() {
            if (frozen) {
                throw new IllegalStateException("Row holder factory already created!");
            }

            // Freeze the SetterFactory so no new setters can be added. (This is to prevent inadvertently adding
            // new setters that won't have corresponding slots.)
            frozen = true;

            final int nBooleans = nextBooleanPos;
            final int nChars = nextCharPos;
            final int nBytes = nextBytePos;
            final int nShorts = nextShortPos;
            final int nInts = nextIntPos;
            final int nFloats = nextFloatPos;
            final int nLongs = nextLongPos;
            final int nDoubles = nextDoublePos;
            final int nObjects = nextObjectPos;

            return () -> new InMemoryRowHolder(
                    nBooleans,
                    nChars,
                    nBytes,
                    nShorts,
                    nInts,
                    nFloats,
                    nLongs,
                    nDoubles,
                    nObjects);
        }

        public int getNextBooleanPos() {
            return nextBooleanPos;
        }

        public int getNextCharPos() {
            return nextCharPos;
        }

        public int getNextBytePos() {
            return nextBytePos;
        }

        public int getNextShortPos() {
            return nextShortPos;
        }

        public int getNextIntPos() {
            return nextIntPos;
        }

        public int getNextFloatPos() {
            return nextFloatPos;
        }

        public int getNextLongPos() {
            return nextLongPos;
        }

        public int getNextDoublePos() {
            return nextDoublePos;
        }

        public int getNextObjectPos() {
            return nextObjectPos;
        }
    }
}
