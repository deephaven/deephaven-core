//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

/**
 * Interface for ColumnSources of vectors that allow retrieving single elements by offset.
 */
public interface UngroupableColumnSource {

    /**
     * Does this particular ColumnSource support ungrouping?
     * 
     * @return {@code true} if you can call the getUngrouped family of methods and get a valid answer
     */
    boolean isUngroupable();

    /*
     * Get the size of the vector at {@code groupRowKey}.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * 
     * @return The size of the vector at {@code groupRowKey}
     */
    long getUngroupedSize(long groupRowKey);

    /*
     * Get the size of the vector at {@code groupRowKey} as of end of the previous update cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * 
     * @return The size of the vector at {@code groupRowKey} as of the end of the previous update cycle
     */
    long getUngroupedPrevSize(long groupRowKey);

    /**
     * Reach into a grouped column source and pull one Object element out of the vector.
     * 
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ObjectVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    Object getUngrouped(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one Object element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ObjectVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    Object getUngroupedPrev(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one Boolean element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ObjectVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    Boolean getUngroupedBoolean(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one Boolean element out of the vector as of end of the previous
     * update cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ObjectVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    Boolean getUngroupedPrevBoolean(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one double element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((DoubleVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    double getUngroupedDouble(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one double element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((DoubleVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    double getUngroupedPrevDouble(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one float element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((FloatVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    float getUngroupedFloat(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one float element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((FloatVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    float getUngroupedPrevFloat(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one byte element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ByteVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    byte getUngroupedByte(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one byte element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ByteVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    byte getUngroupedPrevByte(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one char element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((CharVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    char getUngroupedChar(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one char element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((CharVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    char getUngroupedPrevChar(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one short element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ShortVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    short getUngroupedShort(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one short element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((ShortVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    short getUngroupedPrevShort(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one int element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((IntVector)columnSource.get(groupRowKey)).get(offsetInGroup)
     */
    int getUngroupedInt(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one int element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((IntVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    int getUngroupedPrevInt(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one long element out of the vector.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((LongVector)columnSource.getLong(groupRowKey)).get(offsetInGroup)
     */
    long getUngroupedLong(long groupRowKey, int offsetInGroup);

    /**
     * Reach into a grouped column source and pull one long element out of the vector as of end of the previous update
     * cycle.
     *
     * @param groupRowKey The vector's row key in the grouped column source
     * @param offsetInGroup Positional offset within the vector at {@code groupRowKey}
     * @return Equivalent to ((LongVector)columnSource.getPrev(groupRowKey)).get(offsetInGroup)
     */
    long getUngroupedPrevLong(long groupRowKey, int offsetInGroup);
}
