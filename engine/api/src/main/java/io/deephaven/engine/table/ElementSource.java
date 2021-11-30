package io.deephaven.engine.table;

public interface ElementSource<T> {

    T get(long index);

    Boolean getBoolean(long index);

    byte getByte(long index);

    char getChar(long index);

    double getDouble(long index);

    float getFloat(long index);

    int getInt(long index);

    long getLong(long index);

    short getShort(long index);

    T getPrev(long index);

    Boolean getPrevBoolean(long index);

    byte getPrevByte(long index);

    char getPrevChar(long index);

    double getPrevDouble(long index);

    float getPrevFloat(long index);

    int getPrevInt(long index);

    long getPrevLong(long index);

    short getPrevShort(long index);
}
