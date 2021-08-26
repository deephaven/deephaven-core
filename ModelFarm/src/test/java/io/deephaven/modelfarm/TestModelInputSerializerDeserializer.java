/*
 * Copyright (c) 2018. Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;

import java.io.IOException;

public class TestModelInputSerializerDeserializer extends BaseArrayTestCase {

    public void testSerializeDeserialize() throws IOException, ClassNotFoundException {
        final String filename =
                Configuration.getInstance().getTempPath("test") + "TestModelInputSerializerDeserializer.ser";

        final ModelInputSerializer<Integer> serializer = new ModelInputSerializer<>(filename);

        final int d1 = 1;
        final int d2 = 2;
        final int d3 = 3;

        serializer.exec(1);
        serializer.exec(2);
        serializer.exec(3);
        serializer.close();

        final ModelInputDeserializer<Integer> deserializer = new ModelInputDeserializer<>(Integer.class, filename);
        assertEquals(d1, deserializer.next().intValue());
        assertEquals(d2, deserializer.next().intValue());
        assertEquals(d3, deserializer.next().intValue());

        try {
            deserializer.next();
            fail("Should have thrown an exception");
        } catch (IOException e) {
            // pass
        }
    }

}
