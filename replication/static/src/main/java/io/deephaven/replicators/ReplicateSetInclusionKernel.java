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

import static io.deephaven.replication.ReplicatePrimitiveCode.charToAllButBooleanAndFloats;
import static io.deephaven.replication.ReplicatePrimitiveCode.floatToAllFloatingPoints;

public class ReplicateSetInclusionKernel {
    public static void main(String[] args) throws IOException {
        charToAllButBooleanAndFloats("replicateSetInclusionKernel",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/setinclusion/CharSetInclusionKernel.java");
        floatToAllFloatingPoints("replicateSetInclusionKernel",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/setinclusion/FloatSetInclusionKernel.java");
        // Fixup double because it needs to use long bits
        final File objectFile = new File(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/select/setinclusion/DoubleSetInclusionKernel.java");
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "int valueBits", "long valueBits",
                "int checkValue", "long checkValue",
                "doubleToIntBits", "doubleToLongBits",
                "intBitsToDouble", "longBitsToDouble",
                "TIntHashSet", "TLongHashSet",
                "TIntIterator", "TLongIterator");
        FileUtils.writeLines(objectFile, lines);
    }
}
