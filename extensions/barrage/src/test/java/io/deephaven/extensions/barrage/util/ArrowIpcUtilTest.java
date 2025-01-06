//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrowIpcUtilTest {

    public static final Field FOO = new Field("Foo", FieldType.nullable(Types.MinorType.INT.getType()), null);
    public static final Field BAR = new Field("Bar", FieldType.notNullable(Types.MinorType.INT.getType()), null);
    public static final Field BAZ = new Field("Baz",
            new FieldType(true, Types.MinorType.VARCHAR.getType(), null, Map.of("k1", "v1", "k2", "v2")), null);

    private static final Schema SCHEMA_1 = new Schema(List.of(FOO, BAR, BAZ));
    private static final Schema SCHEMA_2 =
            new Schema(List.of(FOO, BAR, BAZ), Map.of("key1", "value1", "key2", "value2"));

    @Test
    public void testSchemas() throws IOException {
        verifySerDeser(SCHEMA_1);
        verifySerDeser(SCHEMA_2);
    }

    // A bit circular, but better than nothing.
    public static void verifySerDeser(Schema schema) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final long length = ArrowIpcUtil.serialize(baos, schema);
        assertThat(length).isEqualTo(baos.size());
        Schema deserialized = ArrowIpcUtil.deserialize(baos.toByteArray(), 0, (int) length);
        assertThat(deserialized).isEqualTo(schema);
    }
}
