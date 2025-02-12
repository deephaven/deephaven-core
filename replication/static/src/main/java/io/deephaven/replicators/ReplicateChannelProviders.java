//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

public class ReplicateChannelProviders {
    private static final String TASK = "replicateChannelProviders";
    private static final String[] NO_EXCEPTIONS = new String[0];

    private static final String BASE_CHANNEL_PROVIDER_DIR =
            "extensions/s3/src/main/java/io/deephaven/extensions/s3/";
    private static final String S3A_CHANNEL_PROVIDER_PATH =
            BASE_CHANNEL_PROVIDER_DIR + "S3ASeekableChannelProvider.java";
    private static final String S3A_CHANNEL_PROVIDER_PLUGIN_PATH =
            BASE_CHANNEL_PROVIDER_DIR + "S3ASeekableChannelProviderPlugin.java";

    public static void main(String... args) throws IOException {
        // S3A -> GCS
        String[][] pairs = new String[][] {
                {"S3A", "GCS"},
                {"s3a", "gcs"},
        };
        replaceAll(TASK, S3A_CHANNEL_PROVIDER_PATH, null, NO_EXCEPTIONS, pairs);

        // S3A -> S3N
        pairs = new String[][] {
                {"S3A", "S3N"},
                {"s3a", "s3n"},
        };
        replaceAll(TASK, S3A_CHANNEL_PROVIDER_PATH, null, NO_EXCEPTIONS, pairs);
        replaceAll(TASK, S3A_CHANNEL_PROVIDER_PLUGIN_PATH, null, NO_EXCEPTIONS, pairs);
    }
}
