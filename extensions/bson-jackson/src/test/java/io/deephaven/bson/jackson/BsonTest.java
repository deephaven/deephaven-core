//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.IntValue;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.deephaven.bson.jackson.TestHelper.parse;

public class BsonTest {

    private static final ObjectValue OBJECT_NAME_AGE_FIELD = ObjectValue.builder()
            .putFields("name", StringValue.standard())
            .putFields("age", IntValue.standard())
            .build();

    @Test
    void bson() throws IOException {
        final byte[] bsonExample = new ObjectMapper(new BsonFactory()).writeValueAsBytes(Map.of(
                "name", "foo",
                "age", 42));
        parse(
                JacksonBsonProvider.of(OBJECT_NAME_AGE_FIELD).bytesProcessor(),
                List.of(bsonExample),
                ObjectChunk.chunkWrap(new String[] {"foo"}),
                IntChunk.chunkWrap(new int[] {42}));
    }
}
