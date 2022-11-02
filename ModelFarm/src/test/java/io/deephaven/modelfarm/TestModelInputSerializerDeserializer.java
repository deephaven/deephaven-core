/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.modelfarm;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestModelInputSerializerDeserializer extends BaseArrayTestCase {



    public void testSerializeDeserialize() throws IOException, ClassNotFoundException {
        final Path path = Files.createTempFile("test", "TestModelInputSerializerDeserializer.ser");
        try {
            final ModelInputSerializer<Integer> serializer = new ModelInputSerializer<>(path.toString());

            final int d1 = 1;
            final int d2 = 2;
            final int d3 = 3;

            serializer.exec(1);
            serializer.exec(2);
            serializer.exec(3);
            serializer.close();

            final ModelInputDeserializer<Integer> deserializer =
                    new ModelInputDeserializer<>(Integer.class, path.toString());
            assertEquals(d1, deserializer.next().intValue());
            assertEquals(d2, deserializer.next().intValue());
            assertEquals(d3, deserializer.next().intValue());

            try {
                deserializer.next();
                fail("Should have thrown an exception");
            } catch (IOException e) {
                // pass
            }
        } finally {
            Files.delete(path);
        }
    }

}
