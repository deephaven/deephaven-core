package io.deephaven.engine.bench;

public class IncrementalSortRedirectionEdgeCase extends IncrementalSortRedirectionBase {

    static {
        // A value that exhibits on O(n^2) case with poorly distributed hash functions, ie (int)(value ^ (value >>> 32))
        // from gnu.trove.impl.HashFunctions.hash(long).
        //
        // When used with a bad hash function, this benchmark will not complete in a reasonable amount of time.
        System.setProperty("UnionRedirection.allocationUnit", "2199023255552");
    }
}
