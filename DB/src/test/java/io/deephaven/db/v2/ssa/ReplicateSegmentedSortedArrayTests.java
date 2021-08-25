package io.deephaven.db.v2.ssa;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class ReplicateSegmentedSortedArrayTests {
    public static void main(String[] args) throws IOException {
        ReplicateSegmentedSortedArray.main(args);

        ReplicatePrimitiveCode.charToAllButBoolean(TestCharSegmentedSortedArray.class,
            ReplicatePrimitiveCode.TEST_SRC);
        final String objectSsaTest = ReplicatePrimitiveCode
            .charToObject(TestCharSegmentedSortedArray.class, ReplicatePrimitiveCode.TEST_SRC);
        fixupObjectSsaTest(objectSsaTest);
    }

    private static void fixupObjectSsaTest(String objectPath) throws IOException {
        final File objectFile = new File(objectPath);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        FileUtils.writeLines(objectFile, ReplicateUtilities.fixupChunkAttributes(lines));
    }

}
