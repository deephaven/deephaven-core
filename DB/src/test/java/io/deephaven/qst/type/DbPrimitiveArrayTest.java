package io.deephaven.qst.type;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.engine.structures.vector.DbBooleanArray;
import io.deephaven.engine.structures.vector.DbByteArray;
import io.deephaven.engine.structures.vector.DbCharArray;
import io.deephaven.engine.structures.vector.DbDoubleArray;
import io.deephaven.engine.structures.vector.DbFloatArray;
import io.deephaven.engine.structures.vector.DbIntArray;
import io.deephaven.engine.structures.vector.DbLongArray;
import io.deephaven.engine.structures.vector.DbShortArray;
import java.lang.reflect.InvocationTargetException;
import org.junit.Test;

public class DbPrimitiveArrayTest {

    @Test
    public void types()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        assertThat(DbPrimitiveArrayType.types()).containsExactlyInAnyOrder(
                DbBooleanArray.type(),
                DbByteArray.type(),
                DbCharArray.type(),
                DbShortArray.type(),
                DbIntArray.type(),
                DbLongArray.type(),
                DbFloatArray.type(),
                DbDoubleArray.type());
    }
}
