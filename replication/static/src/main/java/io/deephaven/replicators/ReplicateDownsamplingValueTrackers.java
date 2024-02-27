//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;

import java.io.IOException;

/**
 * Generates primitive value trackers from the char variant, so that only char and Object need to be manually
 * maintained. When these are changed, please run `./gradlew replicateDownsampleValueTrackers` to regenerate the other
 * types.
 */
public class ReplicateDownsamplingValueTrackers extends ReplicatePrimitiveCode {
    public static void main(String[] args) throws IOException {
        charToAllButBoolean("replicateDownsamplingValueTrackers",
                "ClientSupport/src/main/java/io/deephaven/clientsupport/plotdownsampling/CharValueTracker.java");
    }
}
