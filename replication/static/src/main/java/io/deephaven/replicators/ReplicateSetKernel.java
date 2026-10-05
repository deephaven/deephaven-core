//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBooleanAndFloats;
import static io.deephaven.replication.ReplicatePrimitiveCode.floatToAllFloatingPoints;

public class ReplicateSetKernel {
    private static final String TASK = "replicateSetKernel";
    private static final String DIRECTORY = "engine/table/src/main/java/io/deephaven/engine/table/impl/select/";

    public static void main(String[] args) throws IOException {
        charToAllButBooleanAndFloats(TASK, DIRECTORY + "CharSetKernel.java");
        floatToAllFloatingPoints(TASK, DIRECTORY + "FloatSetKernel.java");
        // A double's bits are a long
        final File doubleFile = new File(DIRECTORY + "DoubleSetKernel.java");
        List<String> lines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "Int2IntOpenHashMap", "Long2IntOpenHashMap",
                "IntIterator", "LongIterator",
                "nextInt\\(", "nextLong(",
                "intBitsToDouble", "longBitsToDouble",
                "it\\.unimi\\.dsi\\.fastutil\\.ints", "it.unimi.dsi.fastutil.longs");
        FileUtils.writeLines(doubleFile, lines);
    }
}
