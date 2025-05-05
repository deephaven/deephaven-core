//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReplicateVectors {
    private static final String TASK = "replicateVectors";

    @SuppressWarnings("AutoBoxing")
    public static void main(String[] args) throws IOException {
        Map<String, Long> serialVersionUIDs = Collections.emptyMap();

        final String charVectorJavaPath = "engine/vector/src/main/java/io/deephaven/vector/CharVector.java";

        ReplicatePrimitiveCode.charToByte(TASK, charVectorJavaPath, serialVersionUIDs);
        ReplicatePrimitiveCode.charToShort(TASK, charVectorJavaPath, serialVersionUIDs);

        ReplicatePrimitiveCode.charToInteger(TASK, charVectorJavaPath, serialVersionUIDs);
        ReplicatePrimitiveCode.charToLong(TASK, charVectorJavaPath, serialVersionUIDs);
        fixupCharToFloat(ReplicatePrimitiveCode.charToFloat(TASK, charVectorJavaPath, serialVersionUIDs));
        fixupCharToDouble(ReplicatePrimitiveCode.charToDouble(TASK, charVectorJavaPath, serialVersionUIDs));

        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVectorDirect.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharVectorSlice.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(TASK,
                "engine/vector/src/main/java/io/deephaven/vector/CharSubVector.java",
                serialVersionUIDs);
    }

    public static void fixupCharToDouble(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.simpleFixup(lines, "ElementEquals",
                "aIterator\\.nextDouble\\(\\) != bIterator\\.nextDouble\\(\\)",
                "Double.doubleToLongBits(aIterator.nextDouble()) != Double.doubleToLongBits(bIterator.nextDouble())");
        FileUtils.writeLines(file, lines);
    }

    public static void fixupCharToFloat(@NotNull final String path) throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = ReplicationUtils.simpleFixup(lines, "ElementEquals",
                "aIterator\\.nextFloat\\(\\) != bIterator\\.nextFloat\\(\\)",
                "Float.floatToIntBits(aIterator.nextFloat()) != Float.floatToIntBits(bIterator.nextFloat())");
        FileUtils.writeLines(file, lines);
    }
}
