/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.dbarrays;

import java.io.ObjectStreamClass;

public class WriteSerializedDbArrays {
    public static void main(String[] args) throws ClassNotFoundException {
        for (String className : new String[] {
                "io.deephaven.db.tables.dbarrays.DbArrayBase",
                "io.deephaven.db.tables.dbarrays.DbArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbBooleanArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbCharArray",
                "io.deephaven.db.tables.dbarrays.DbLongArray",
                "io.deephaven.db.tables.dbarrays.DbIntArray",
                "io.deephaven.db.tables.dbarrays.DbShortArray",
                "io.deephaven.db.tables.dbarrays.DbByteArray",
                "io.deephaven.db.tables.dbarrays.DbDoubleArray",
                "io.deephaven.db.tables.dbarrays.DbFloatArray",
                "io.deephaven.db.tables.dbarrays.DbCharArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbLongArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbIntArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbShortArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbByteArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbDoubleArrayDirect",
                "io.deephaven.db.tables.dbarrays.DbFloatArrayDirect"
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
