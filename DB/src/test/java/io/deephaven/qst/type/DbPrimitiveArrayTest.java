package io.deephaven.qst.type;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.db.tables.dbarrays.DbBooleanArray;
import io.deephaven.db.tables.dbarrays.DbByteArray;
import io.deephaven.db.tables.dbarrays.DbCharArray;
import io.deephaven.db.tables.dbarrays.DbDoubleArray;
import io.deephaven.db.tables.dbarrays.DbFloatArray;
import io.deephaven.db.tables.dbarrays.DbIntArray;
import io.deephaven.db.tables.dbarrays.DbLongArray;
import io.deephaven.db.tables.dbarrays.DbShortArray;
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
