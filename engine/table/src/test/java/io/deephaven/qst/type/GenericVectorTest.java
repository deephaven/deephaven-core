//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.vector.ObjectVector;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GenericVectorTest {

    @Test
    public void stringType() throws ClassNotFoundException {
        testConstruction(GenericVectorType.of(ObjectVector.class, Type.stringType()));
    }

    @Test
    public void instantType() throws ClassNotFoundException {
        testConstruction(GenericVectorType.of(ObjectVector.class, Type.instantType()));
    }

    private static <ComponentType> void testConstruction(GenericVectorType<?, ComponentType> vectorType)
            throws ClassNotFoundException {
        assertThat(Type.find(vectorType.clazz(), vectorType.componentType().clazz())).isEqualTo(vectorType);
        assertThat(GenericVectorType.of(vectorType.clazz(), vectorType.componentType())).isEqualTo(vectorType);
    }
}
