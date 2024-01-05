/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.vector.Vector;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class GenericVectorTest {

    @Test
    public void stringType() {
        final GenericVectorType<ObjectVector<String>, String> type = ObjectVector.type(Type.stringType());
        assertThat(type.clazz()).isEqualTo(ObjectVector.class);
        assertThat(type.componentType()).isEqualTo(Type.stringType());
    }

    @Test
    public void instantType() {
        final GenericVectorType<ObjectVector<Instant>, Instant> type = ObjectVector.type(Type.instantType());
        assertThat(type.clazz()).isEqualTo(ObjectVector.class);
        assertThat(type.componentType()).isEqualTo(Type.instantType());
    }

    @Test
    public void boxedTypes() {
        BoxedType.instances().forEach(boxedType -> {
            final GenericVectorType<? extends ObjectVector<?>, ?> type = ObjectVector.type(boxedType);
            assertThat(type.clazz()).isEqualTo(ObjectVector.class);
            assertThat(type.componentType()).isEqualTo(boxedType);
        });
    }

    @Test
    public void badClass() {
        try {
            GenericVectorType.of(Vector.class, Type.stringType());
            failBecauseExceptionWasNotThrown(IllegalAccessError.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void alternativeTypes() {
        final GenericVectorType<ObjectVectorDirect, String> type =
                GenericVectorType.of(ObjectVectorDirect.class, Type.stringType());
        assertThat(type.clazz()).isEqualTo(ObjectVectorDirect.class);
        assertThat(type.componentType()).isEqualTo(Type.stringType());
    }
}
