//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateVectorChunkFilters {
    private static final String TASK = "replicateVectorChunkFilters";

    private static final String CHUNK_FILTER_PATH =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/select/vectorchunkFilter/";
    private static final String CHAR_VECTOR_CHUNK_FILTER = CHUNK_FILTER_PATH + "CharVectorChunkFilter.java";
    private static final String CHAR_ARRAY_CHUNK_FILTER = CHUNK_FILTER_PATH + "CharArrayChunkFilter.java";

    public static void main(final String[] args) throws IOException {
        charToAllButBoolean(TASK, CHAR_VECTOR_CHUNK_FILTER);
        fixupObject(charToObject(TASK, CHAR_VECTOR_CHUNK_FILTER));
        charToAllButBoolean(TASK, CHAR_ARRAY_CHUNK_FILTER);
        fixupObject(charToObject(TASK, CHAR_ARRAY_CHUNK_FILTER));
    }

    private static void fixupObject(final String objectPath) throws IOException {
        List<String> lines = FileUtils.readLines(new File(objectPath), Charset.defaultCharset());
        lines = ReplicationUtils.fixupChunkAttributes(lines);
        lines = ReplicationUtils.globalReplacements(lines, "ValueIteratorOfObject", "ValueIterator");
        FileUtils.writeLines(new File(objectPath), lines);
    }
}
