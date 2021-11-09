/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.dbarrays;

import java.io.ObjectStreamClass;

public class WriteSerializedDbArrays {
    public static void main(String[] args) throws ClassNotFoundException {
        for (String className : new String[] {
                "io.deephaven.engine.tables.dbarrays.Vector",
                "io.deephaven.engine.tables.dbarrays.ObjectVectorDirect",
                "io.deephaven.engine.tables.dbarrays.BooleanVectorDirect",
                "io.deephaven.engine.tables.dbarrays.CharVector",
                "io.deephaven.engine.tables.dbarrays.LongVector",
                "io.deephaven.engine.tables.dbarrays.IntVector",
                "io.deephaven.engine.tables.dbarrays.ShortVector",
                "io.deephaven.engine.tables.dbarrays.ByteVector",
                "io.deephaven.engine.tables.dbarrays.DoubleVector",
                "io.deephaven.engine.tables.dbarrays.FloatVector",
                "io.deephaven.engine.tables.dbarrays.CharVectorDirect",
                "io.deephaven.engine.tables.dbarrays.LongVectorDirect",
                "io.deephaven.engine.tables.dbarrays.IntVectorDirect",
                "io.deephaven.engine.tables.dbarrays.ShortVectorDirect",
                "io.deephaven.engine.tables.dbarrays.ByteVectorDirect",
                "io.deephaven.engine.tables.dbarrays.DoubleVectorDirect",
                "io.deephaven.engine.tables.dbarrays.FloatVectorDirect"
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
