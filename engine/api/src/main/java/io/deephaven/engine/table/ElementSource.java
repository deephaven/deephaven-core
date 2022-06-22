/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

public interface ElementSource<T> {

    T get(long rowKey);

    Boolean getBoolean(long rowKey);

    byte getByte(long rowKey);

    char getChar(long rowKey);

    double getDouble(long rowKey);

    float getFloat(long rowKey);

    int getInt(long rowKey);

    long getLong(long rowKey);

    short getShort(long rowKey);

    T getPrev(long rowKey);

    Boolean getPrevBoolean(long rowKey);

    byte getPrevByte(long rowKey);

    char getPrevChar(long rowKey);

    double getPrevDouble(long rowKey);

    float getPrevFloat(long rowKey);

    int getPrevInt(long rowKey);

    long getPrevLong(long rowKey);

    short getPrevShort(long rowKey);
}
