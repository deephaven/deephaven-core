/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import java.io.ObjectStreamClass;

public class WriteSerializedVectors {
    public static void main(String[] args) throws ClassNotFoundException {
        for (String className : new String[] {
                "Vector",
                "ObjectVectorDirect",
                "BooleanVectorDirect",
                "CharVector",
                "LongVector",
                "IntVector",
                "ShortVector",
                "ByteVector",
                "DoubleVector",
                "FloatVector",
                "CharVectorDirect",
                "LongVectorDirect",
                "IntVectorDirect",
                "ShortVectorDirect",
                "ByteVectorDirect",
                "DoubleVectorDirect",
                "FloatVectorDirect"
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
