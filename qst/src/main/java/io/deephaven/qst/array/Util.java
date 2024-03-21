//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;

class Util {
    static final int DEFAULT_BUILDER_INITIAL_CAPACITY = 16;

    static byte adapt(Boolean x) {
        return BooleanUtils.booleanAsByte(x);
    }

    static byte adapt(Byte x) {
        return x == null ? QueryConstants.NULL_BYTE : x;
    }

    static char adapt(Character x) {
        return x == null ? QueryConstants.NULL_CHAR : x;
    }

    static double adapt(Double x) {
        return x == null ? QueryConstants.NULL_DOUBLE : x;
    }

    static float adapt(Float x) {
        return x == null ? QueryConstants.NULL_FLOAT : x;
    }

    static int adapt(Integer x) {
        return x == null ? QueryConstants.NULL_INT : x;
    }

    static long adapt(Long x) {
        return x == null ? QueryConstants.NULL_LONG : x;
    }

    static short adapt(Short x) {
        return x == null ? QueryConstants.NULL_SHORT : x;
    }
}
