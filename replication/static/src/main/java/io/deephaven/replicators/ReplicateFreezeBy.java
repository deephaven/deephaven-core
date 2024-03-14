//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicateFreezeBy {
    public static void main(String[] args) throws IOException {
        charToAllButBoolean("replicateFreezeBy",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/freezeby/CharFreezeByHelper.java");

        final String objectResult = charToObject("replicateFreezeBy",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/freezeby/CharFreezeByHelper.java");
        fixupObject(objectResult);

        final String booleanResult = charToBoolean("replicateFreezeBy",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/util/freezeby/CharFreezeByHelper.java");
        fixupBoolean(booleanResult);
    }

    private static void fixupObject(String objectResult) throws IOException {
        final File objectFile = new File(objectResult);
        final List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        final List<String> newLines = ReplicationUtils.replaceRegion(lines, "clearIndex",
                Collections.singletonList("        removed.forAllRowKeys(idx -> resultSource.set(idx, null));"));
        FileUtils.writeLines(objectFile, newLines);
    }

    private static void fixupBoolean(String booleanResult) throws IOException {
        final File booleanFile = new File(booleanResult);
        final List<String> lines = FileUtils.readLines(booleanFile, Charset.defaultCharset());
        final List<String> newLines =
                ReplicationUtils.globalReplacements(lines, "final BooleanChunk asBoolean = values.asBooleanChunk",
                        "final ObjectChunk<Boolean, ?> asBoolean = values.asObjectChunk");
        FileUtils.writeLines(booleanFile, newLines);
    }
}
