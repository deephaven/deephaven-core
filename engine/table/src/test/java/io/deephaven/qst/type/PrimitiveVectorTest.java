package io.deephaven.qst.type;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.vector.*;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

public class PrimitiveVectorTest {

    @Test
    public void types()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        assertThat(PrimitiveVectorType.types()).containsExactlyInAnyOrder(
                BooleanVector.type(),
                ByteVector.type(),
                CharVector.type(),
                ShortVector.type(),
                IntVector.type(),
                LongVector.type(),
                FloatVector.type(),
                DoubleVector.type());
    }
}
