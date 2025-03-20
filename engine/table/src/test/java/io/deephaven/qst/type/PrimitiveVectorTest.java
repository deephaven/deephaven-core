//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ShortVector;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;

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
    public void byteVector() throws ClassNotFoundException {
        testConstruction(ByteVector.type());
    }

    @Test
    public void charVector() throws ClassNotFoundException {
        testConstruction(CharVector.type());
    }

    @Test
    public void shortVector() throws ClassNotFoundException {
        testConstruction(ShortVector.type());
    }

    @Test
    public void intVector() throws ClassNotFoundException {
        testConstruction(IntVector.type());
    }

    @Test
    public void longVector() throws ClassNotFoundException {
        testConstruction(LongVector.type());
    }

    @Test
    public void floatVector() throws ClassNotFoundException {
        testConstruction(FloatVector.type());
    }

    @Test
    public void doubleVector() throws ClassNotFoundException {
        testConstruction(DoubleVector.type());
    }

    private static <T, ComponentType> void testConstruction(PrimitiveVectorType<T, ComponentType> vectorType)
            throws ClassNotFoundException {
        assertThat(Type.find(vectorType.clazz())).isEqualTo(vectorType);
        assertThat(Type.find(vectorType.clazz(), vectorType.componentType().clazz())).isEqualTo(vectorType);
        assertThat(PrimitiveVectorType.of(vectorType.clazz(), vectorType.componentType())).isEqualTo(vectorType);
        // fail if component type is bad
        for (PrimitiveType<?> badComponent : PrimitiveType.instances().collect(Collectors.toList())) {
            if (badComponent.equals(vectorType.componentType())) {
                continue;
            }
            fail(vectorType.clazz(), badComponent);
        }
        // fail if data type is bad
        fail(Object.class, vectorType.componentType());
        for (PrimitiveVectorType<?, ?> primitiveVectorType : TypeHelper.primitiveVectorTypes()
                .collect(Collectors.toList())) {
            if (primitiveVectorType.equals(vectorType)) {
                continue;
            }
            fail(primitiveVectorType.clazz(), vectorType.componentType());
        }
    }

    public static void fail(Class<?> clazz, PrimitiveType<?> ct) {
        try {
            Type.find(clazz, ct.clazz());
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Invalid PrimitiveVectorType");
        }
        try {
            PrimitiveVectorType.of(clazz, ct);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Invalid PrimitiveVectorType");
        }
    }
}
