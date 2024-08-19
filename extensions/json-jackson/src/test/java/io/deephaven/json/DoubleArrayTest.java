//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.deephaven.json.TestHelper.parse;

public class DoubleArrayTest {

    @Test
    void standard() throws IOException {
        parse(DoubleValue.standard().array(), "[42.1, null, 43.2]",
                ObjectChunk.chunkWrap(new Object[] {new double[] {42.1, QueryConstants.NULL_DOUBLE, 43.2}}));
    }

    @Test
    void standardMissing() throws IOException {
        parse(DoubleValue.standard().array(), "", ObjectChunk.chunkWrap(new Object[] {null}));
    }

    @Test
    void standardNull() throws IOException {
        parse(DoubleValue.standard().array(), "null", ObjectChunk.chunkWrap(new Object[] {null}));
    }
}
