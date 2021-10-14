/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.dbarrays;

import java.io.ObjectStreamClass;

public class WriteSerializedDbArrays {
    public static void main(String[] args) throws ClassNotFoundException {
        for (String className : new String[] {
                "io.deephaven.engine.tables.dbarrays.DbArrayBase",
                "io.deephaven.engine.tables.dbarrays.DbArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbBooleanArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbCharArray",
                "io.deephaven.engine.tables.dbarrays.DbLongArray",
                "io.deephaven.engine.tables.dbarrays.DbIntArray",
                "io.deephaven.engine.tables.dbarrays.DbShortArray",
                "io.deephaven.engine.tables.dbarrays.DbByteArray",
                "io.deephaven.engine.tables.dbarrays.DbDoubleArray",
                "io.deephaven.engine.tables.dbarrays.DbFloatArray",
                "io.deephaven.engine.tables.dbarrays.DbCharArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbLongArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbIntArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbShortArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbByteArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbDoubleArrayDirect",
                "io.deephaven.engine.tables.dbarrays.DbFloatArrayDirect"
        }) {
            showSerialVersionUID(className);
        }
    }

    private static void showSerialVersionUID(String className) throws ClassNotFoundException {
        Class theClass = Class.forName(className);
        ObjectStreamClass osc = ObjectStreamClass.lookup(theClass);
        System.out.println("x.put(\"" + className + "\", " + osc.getSerialVersionUID() + "L);");
    }
}
