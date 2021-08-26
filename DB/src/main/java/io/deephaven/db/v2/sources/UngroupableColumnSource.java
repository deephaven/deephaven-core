/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

/**
 * Column sources that provide an array.
 */
public interface UngroupableColumnSource {
    /**
     * Does this particular instance of the column source support ungrouping?
     * 
     * @return true if you can call the getUngrouped family of methods and get a valid answer.
     */
    boolean isUngroupable();

    /**
     * @param columnIndex the index within this column to interrogate
     * @return the size of the DbArray at columnIndex.
     */
    long getUngroupedSize(long columnIndex);

    long getUngroupedPrevSize(long columnIndex);

    /**
     * Reach into a grouped column source and pull one element out of the array.
     * 
     * @param columnIndex the index within the column of the cell to get
     * @param arrayIndex the index within the array at the specified cell
     * @return Equivalent to ((DbArray)columnSource.get(columnIndex)).get(arrayIndex)
     */
    Object getUngrouped(long columnIndex, int arrayIndex);

    Object getUngroupedPrev(long columnIndex, int arrayIndex);

    Boolean getUngroupedBoolean(long columnIndex, int arrayIndex);

    Boolean getUngroupedPrevBoolean(long columnIndex, int arrayIndex);

    double getUngroupedDouble(long columnIndex, int arrayIndex);

    double getUngroupedPrevDouble(long columnIndex, int arrayIndex);

    float getUngroupedFloat(long columnIndex, int arrayIndex);

    float getUngroupedPrevFloat(long columnIndex, int arrayIndex);

    byte getUngroupedByte(long columnIndex, int arrayIndex);

    byte getUngroupedPrevByte(long columnIndex, int arrayIndex);

    char getUngroupedChar(long columnIndex, int arrayIndex);

    char getUngroupedPrevChar(long columnIndex, int arrayIndex);

    short getUngroupedShort(long columnIndex, int arrayIndex);

    short getUngroupedPrevShort(long columnIndex, int arrayIndex);

    int getUngroupedInt(long columnIndex, int arrayIndex);

    int getUngroupedPrevInt(long columnIndex, int arrayIndex);

    long getUngroupedLong(long columnIndex, int arrayIndex);

    long getUngroupedPrevLong(long columnIndex, int arrayIndex);
}
