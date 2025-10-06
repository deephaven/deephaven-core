//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicationUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;

public class ReplicateArrayComparators {
    private static final String TASK = "replicateArrayComparators";

    public static void main(String[] args) throws IOException {
        final String charFile =
                "engine/table/src/main/java/io/deephaven/engine/table/impl/comparators/CharArrayComparator.java";
        charToFloat(TASK, charFile, null);
        charToDouble(TASK, charFile, null);
    }
}
