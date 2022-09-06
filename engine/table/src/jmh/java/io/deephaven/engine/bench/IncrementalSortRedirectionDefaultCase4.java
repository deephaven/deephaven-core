package io.deephaven.engine.bench;

public class IncrementalSortRedirectionDefaultCase4 extends IncrementalSortRedirectionBase {
    static {
        System.setProperty("io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree.hashBucketWidth", "4");
    }
}
