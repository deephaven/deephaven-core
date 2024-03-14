//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

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

        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(TASK, charVectorJavaPath, serialVersionUIDs);

        final String floatPath = ReplicatePrimitiveCode.charToFloat(TASK, charVectorJavaPath, serialVersionUIDs);
        final File floatFile = new File(floatPath);
        List<String> floatLines = FileUtils.readLines(floatFile, Charset.defaultCharset());
        floatLines = ReplicationUtils.simpleFixup(floatLines, "ElementEquals",
                "aIterator\\.nextFloat\\(\\) != bIterator\\.nextFloat\\(\\)",
                "Float.floatToIntBits(aIterator.nextFloat()) != Float.floatToIntBits(bIterator.nextFloat())");
        FileUtils.writeLines(floatFile, floatLines);

        final String doublePath = ReplicatePrimitiveCode.charToDouble(TASK, charVectorJavaPath, serialVersionUIDs);
        final File doubleFile = new File(doublePath);
        List<String> doubleLines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        doubleLines = ReplicationUtils.simpleFixup(doubleLines, "ElementEquals",
                "aIterator\\.nextDouble\\(\\) != bIterator\\.nextDouble\\(\\)",
                "Double.doubleToLongBits(aIterator.nextDouble()) != Double.doubleToLongBits(bIterator.nextDouble())");
        FileUtils.writeLines(doubleFile, doubleLines);

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
}
