/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;

public class ReplicatePrimitiveInterfaces {

    private static final String CHAR_CONSUMER_PATH =
            "engine/primitive/src/main/java/io/deephaven/engine/primitive/function/CharConsumer.java";

    public static void main(String[] args) throws IOException {
        charToShortAndByte(CHAR_CONSUMER_PATH);
        fixupIntToDouble(charToFloat(CHAR_CONSUMER_PATH, null));
    }

    private static void fixupIntToDouble(@NotNull final String path) throws IOException {
        final File file = new File(path);
        final List<String> lines = ReplicationUtils.globalReplacements(
                FileUtils.readLines(file, Charset.defaultCharset()), "IntConsumer", "DoubleConsumer", "accept\\(int\\)",
                "accept(double)");
        FileUtils.writeLines(file, lines);
    }
}
