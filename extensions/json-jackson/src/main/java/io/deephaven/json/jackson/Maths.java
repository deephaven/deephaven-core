//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.base.ArrayUtil;

final class Maths {

    static final int MAX_POWER_OF_2 = 1 << 30;

    static int nextPowerOf2(int newSize) {
        return Math.max(Integer.highestOneBit(newSize - 1) << 1, 1);
    }

    static int nextArrayCapacity(int newSize) {
        return newSize <= MAX_POWER_OF_2
                ? nextPowerOf2(newSize)
                : ArrayUtil.MAX_ARRAY_SIZE;
    }
}
