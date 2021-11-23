package io.deephaven.replicators;

import io.deephaven.compilertools.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToAllButBoolean;
import static io.deephaven.compilertools.ReplicatePrimitiveCode.charToObject;

public class ReplicateSegmentedSortedArrayTests {
    public static void main(String[] args) throws IOException {
        ReplicateSegmentedSortedArray.main(args);

        charToAllButBoolean("engine/table/src/test/java/io/deephaven/engine/table/impl/ssa/TestCharSegmentedSortedArray.java");
        final String objectSsaTest =
                charToObject("engine/table/src/test/java/io/deephaven/engine/table/impl/ssa/TestCharSegmentedSortedArray.java");
        fixupObjectSsaTest(objectSsaTest);
    }

    private static void fixupObjectSsaTest(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile, ReplicateUtilities.fixupChunkAttributes(lines));
    }
}
