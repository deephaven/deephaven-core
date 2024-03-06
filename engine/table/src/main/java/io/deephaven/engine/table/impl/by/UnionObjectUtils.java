//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.object.UnionObject;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

class UnionObjectUtils {
    // Elevate / refactor utilities as necessary

    public static char charValue(UnionObject obj) {
        return obj == null ? NULL_CHAR : obj.charValue();
    }

    public static byte byteValue(UnionObject obj) {
        return obj == null ? NULL_BYTE : obj.byteValue();
    }

    public static short shortValue(UnionObject obj) {
        return obj == null ? NULL_SHORT : obj.shortValue();
    }

    public static int intValue(UnionObject obj) {
        return obj == null ? NULL_INT : obj.intValue();
    }

    public static long longValue(UnionObject obj) {
        return obj == null ? NULL_LONG : obj.longValue();
    }

    public static double doubleValue(UnionObject obj) {
        return obj == null ? NULL_DOUBLE : obj.doubleValue();
    }

    public static float floatValue(UnionObject obj) {
        return obj == null ? NULL_FLOAT : obj.floatValue();
    }
}
