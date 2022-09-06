package io.deephaven.engine.bench;

public class IncrementalSortRedirectionDefaultCase2 extends IncrementalSortRedirectionBase {
    static {
        System.setProperty("io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree.hashBucketWidth", "2");
    }
}
