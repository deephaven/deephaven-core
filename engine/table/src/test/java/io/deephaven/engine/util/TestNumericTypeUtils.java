//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.util.type.NumericTypeUtils;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestNumericTypeUtils {

    @Test
    public void testIsType() {
        assertFalse(NumericTypeUtils.isPrimitiveNumeric(Instant.class));
        assertFalse(NumericTypeUtils.isPrimitiveNumeric(Date.class));
        assertTrue(NumericTypeUtils.isPrimitiveNumeric(int.class));
        assertFalse(NumericTypeUtils.isPrimitiveNumeric(Double.class));

        assertFalse(NumericTypeUtils.isBoxedNumeric(Instant.class));
        assertFalse(NumericTypeUtils.isBoxedNumeric(Date.class));
        assertFalse(NumericTypeUtils.isBoxedNumeric(int.class));
        assertTrue(NumericTypeUtils.isBoxedNumeric(Double.class));

        assertFalse(NumericTypeUtils.isNumeric(Instant.class));
        assertFalse(NumericTypeUtils.isNumeric(Date.class));
        assertTrue(NumericTypeUtils.isNumeric(int.class));
        assertTrue(NumericTypeUtils.isNumeric(Double.class));
    }
}
