//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

public class ReplicateTableLocationKey {
    private static final String TABLE_LOCATION_KEY_DIR =
            "engine/table/src/main/java/io/deephaven/engine/table/impl/locations/local/";
    private static final String FILE_TABLE_LOCATION_KEY_PATH = TABLE_LOCATION_KEY_DIR + "FileTableLocationKey.java";
    private static final String URI_TABLE_LOCATION_KEY_PATH = TABLE_LOCATION_KEY_DIR + "URITableLocationKey.java";

    private static final String[] NO_EXCEPTIONS = new String[0];

    public static void main(final String[] args) throws IOException {
        final String[][] pairs = new String[][] {
                {"file\\.getAbsoluteFile\\(\\)", "uri"},
                {"java.io.File", "java.net.URI"},
                {"file", "uri"},
                {"File", "URI"},
        };
        replaceAll("replicateTableLocationKey", FILE_TABLE_LOCATION_KEY_PATH, URI_TABLE_LOCATION_KEY_PATH, null,
                NO_EXCEPTIONS, pairs);
    }
}
