//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;


import java.io.IOException;

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
