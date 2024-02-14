/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateBarrageUtils {
    private static final String CHUNK_PACKAGE =
            "extensions/barrage/src/main/java/io/deephaven/extensions/barrage/chunk";

    public static void main(final String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CHUNK_PACKAGE + "/CharChunkInputStreamGenerator.java");
        ReplicatePrimitiveCode.charToAllButBoolean(CHUNK_PACKAGE + "/array/CharArrayExpansionKernel.java");

        ReplicatePrimitiveCode.charToAllButBoolean(CHUNK_PACKAGE + "/vector/CharVectorExpansionKernel.java");
        fixupVectorExpansionKernel(CHUNK_PACKAGE + "/vector/IntVectorExpansionKernel.java", "Int");
        fixupVectorExpansionKernel(CHUNK_PACKAGE + "/vector/LongVectorExpansionKernel.java", "Long");
        fixupVectorExpansionKernel(CHUNK_PACKAGE + "/vector/DoubleVectorExpansionKernel.java", "Double");
    }

    private static void fixupVectorExpansionKernel(final @NotNull String path, final @NotNull String type)
            throws IOException {
        final File file = new File(path);
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        lines = removeImport(lines, "import io.deephaven.engine.primitive.function." + type + "Consumer;");
        lines = addImport(lines, "import java.util.function." + type + "Consumer;");
        FileUtils.writeLines(file, lines);
    }
}
