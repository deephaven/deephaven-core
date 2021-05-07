/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

public interface DataObjectStreamConstants {

    public static final byte ADD_CLASS=-1;

    public static final byte OBJECT_TYPE=-2;

    public static final byte STRING_TYPE=-3;
    public static final byte INTEGER_TYPE=-4;
    public static final byte DOUBLE_TYPE=-5;
    public static final byte LONG_TYPE=-6;
    public static final byte BYTE_TYPE=-7;
    public static final byte DATE_TYPE=-8;

    public static final byte ARRAY_TYPE=-9;
    public static final byte STRING_ARRAY_TYPE=-10;

    public static final byte INT_ARRAY_TYPE=-11;
    public static final byte DOUBLE_ARRAY_TYPE=-12;
    public static final byte LONG_ARRAY_TYPE=-13;
    public static final byte BYTE_ARRAY_TYPE=-14;

    public static final byte NULL_TYPE=-15;

    public static final byte ADO_ARRAY_TYPE=-16;
    public static final byte MAP_TYPE=-17;
    public static final byte SET_TYPE=-18;
    public static final byte LINKED_MAP_TYPE=-19;
    public static final byte LINKED_SET_TYPE=-20;
    //for strings with more than 33,000 characters
    public static final byte LONG_STRING_TYPE=-21;
    public static final byte ADD_ENUM = -22;
    public static final int KNOWN_ENUM = -23;

    public static final byte BOOLEAN_TYPE=-24;
    public static final byte FLOAT_TYPE=-25;
    public static final byte FLOAT_ARRAY_TYPE=-26;
    public static final byte TREE_MAP_TYPE=-27;
    public static final byte TREE_SET_TYPE=-28;

    public static final byte ADD_ENUM_LONG = -29;
    public static final byte KNOWN_ENUM_LONG = -30;

    public static final byte ADD_COLUMN_SET_CLASS=-31;
}
