//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

public class ReplicateCachingSupplier {
    private static final String SOFT_CACHING_SUPPLIER_DIR = "Util/src/main/java/io/deephaven/util/datastructures/";
    private static final String SOFT_CACHING_SUPPLIER_PATH = SOFT_CACHING_SUPPLIER_DIR + "SoftCachingSupplier.java";
    private static final String SOFT_CACHING_FUNCTION_PATH = SOFT_CACHING_SUPPLIER_DIR + "SoftCachingFunction.java";

    private static final String[] NO_EXCEPTIONS = new String[0];

    public static void main(final String[] args) throws IOException {
        final String[][] pairs = new String[][] {
                {"Supplier<OUTPUT_TYPE>", "Function<INPUT_TYPE, OUTPUT_TYPE>"},
                {"internalSupplier\\.get\\(\\)", "internalFunction\\.apply\\(arg\\)"},
                {"OUTPUT_TYPE get\\(\\)", "OUTPUT_TYPE apply\\(final INPUT_TYPE arg\\)"},
                {"Supplier", "Function"},
                {"supplier", "function"},
        };
        replaceAll("replicateCachingSupplier", SOFT_CACHING_SUPPLIER_PATH, SOFT_CACHING_FUNCTION_PATH, null,
                NO_EXCEPTIONS, pairs);
    }
}
