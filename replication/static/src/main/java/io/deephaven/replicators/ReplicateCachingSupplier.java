/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import java.io.IOException;

import static io.deephaven.replication.ReplicatePrimitiveCode.replaceAll;

public class ReplicateCachingSupplier {
    private static final String LAZY_CACHING_SUPPLIER_DIR = "util/src/main/java/io/deephaven/util/datastructures/";
    private static final String LAZY_CACHING_SUPPLIER_PATH = LAZY_CACHING_SUPPLIER_DIR + "LazyCachingSupplier.java";
    private static final String LAZY_CACHING_FUNCTION_PATH = LAZY_CACHING_SUPPLIER_DIR + "LazyCachingFunction.java";

    private static final String[] NO_EXCEPTIONS = new String[0];

    public static void main(final String[] args) throws IOException {
        final String[][] pairs = new String[][] {
                {"Supplier<T>", "Function<INPUT_TYPE, OUTPUT_TYPE>"},
                {"internalSupplier\\.get\\(\\)", "internalFunction\\.apply\\(arg\\)"},
                {"T get\\(\\)", "OUTPUT_TYPE apply\\(final INPUT_TYPE arg\\)"},
                {"SoftReference<T>", "SoftReference<OUTPUT_TYPE>"},
                {"T current", "OUTPUT_TYPE current"},
                {"Supplier", "Function"},
                {"supplier", "function"},
        };
        replaceAll(LAZY_CACHING_SUPPLIER_PATH, LAZY_CACHING_FUNCTION_PATH, null, NO_EXCEPTIONS, pairs);
    }
}
