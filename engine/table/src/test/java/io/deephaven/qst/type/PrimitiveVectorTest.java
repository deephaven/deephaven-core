/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.vector.Vector;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class PrimitiveVectorTest {

    @Test
    public void types()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        assertThat(PrimitiveVectorType.types()).containsExactlyInAnyOrder(
                ByteVector.type(),
                CharVector.type(),
                ShortVector.type(),
                IntVector.type(),
                LongVector.type(),
                FloatVector.type(),
                DoubleVector.type());
    }

    @Test
    public void noBooleanVector() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.booleanType());
            failBecauseExceptionWasNotThrown(IllegalAccessError.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void byteBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.byteType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void charBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.charType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void shortBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.shortType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void intBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.intType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void longBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.longType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void floatBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.floatType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void doubleBadClass() {
        try {
            PrimitiveVectorType.of(Vector.class, Type.doubleType());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // While we technically allow types that properly extend from

    @Test
    public void byteVectorAlt() {
        for (Class<? extends ByteVector> clazz : List.of(ByteVectorDirect.class)) {
            final PrimitiveVectorType<? extends ByteVector, Byte> type = PrimitiveVectorType.of(clazz, Type.byteType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.byteType());
        }
    }

    @Test
    public void charVectorAlt() {
        for (Class<? extends CharVector> clazz : List.of(CharVectorDirect.class)) {
            final PrimitiveVectorType<? extends CharVector, Character> type =
                    PrimitiveVectorType.of(clazz, Type.charType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.charType());
        }
    }

    @Test
    public void shortVectorAlt() {
        for (Class<? extends ShortVector> clazz : List.of(ShortVectorDirect.class)) {
            final PrimitiveVectorType<? extends ShortVector, Short> type =
                    PrimitiveVectorType.of(clazz, Type.shortType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.shortType());
        }
    }

    @Test
    public void intVectorAlt() {
        for (Class<? extends IntVector> clazz : List.of(IntVectorDirect.class)) {
            final PrimitiveVectorType<? extends IntVector, Integer> type =
                    PrimitiveVectorType.of(clazz, Type.intType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.intType());
        }
    }

    @Test
    public void longVectorAlt() {
        for (Class<? extends LongVector> clazz : List.of(LongVectorDirect.class)) {
            final PrimitiveVectorType<? extends LongVector, Long> type = PrimitiveVectorType.of(clazz, Type.longType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.longType());
        }
    }

    @Test
    public void floatVectorAlt() {
        for (Class<? extends FloatVector> clazz : List.of(FloatVectorDirect.class)) {
            final PrimitiveVectorType<? extends FloatVector, Float> type =
                    PrimitiveVectorType.of(clazz, Type.floatType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.floatType());
        }
    }

    @Test
    public void doubleVectorAlt() {
        for (Class<? extends DoubleVector> clazz : List.of(DoubleVectorDirect.class)) {
            final PrimitiveVectorType<? extends DoubleVector, Double> type =
                    PrimitiveVectorType.of(clazz, Type.doubleType());
            assertThat(type.clazz()).isEqualTo(clazz);
            assertThat(type.componentType()).isEqualTo(Type.doubleType());
        }
    }
}
