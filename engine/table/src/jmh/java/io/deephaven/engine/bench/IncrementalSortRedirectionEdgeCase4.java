package io.deephaven.engine.bench;

public class IncrementalSortRedirectionEdgeCase4 extends IncrementalSortRedirectionBase {

    static {
        // A value that exhibits on O(n^2) case with bad hash functions, ie (int)(value ^ (value >>> 32)) from
        // gnu.trove.impl.HashFunctions.hash(long).
        //
        // When used with a bad hash function, this benchmark will not complete in a reasonable amount of time.
        System.setProperty("UnionRedirection.allocationUnit", "2199023255552");
        System.setProperty("io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree.hashBucketWidth", "4");
    }
}
