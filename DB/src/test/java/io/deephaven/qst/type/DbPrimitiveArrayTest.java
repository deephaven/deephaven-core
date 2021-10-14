package io.deephaven.qst.type;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.engine.tables.dbarrays.DbBooleanArray;
import io.deephaven.engine.tables.dbarrays.DbByteArray;
import io.deephaven.engine.tables.dbarrays.DbCharArray;
import io.deephaven.engine.tables.dbarrays.DbDoubleArray;
import io.deephaven.engine.tables.dbarrays.DbFloatArray;
import io.deephaven.engine.tables.dbarrays.DbIntArray;
import io.deephaven.engine.tables.dbarrays.DbLongArray;
import io.deephaven.engine.tables.dbarrays.DbShortArray;
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
