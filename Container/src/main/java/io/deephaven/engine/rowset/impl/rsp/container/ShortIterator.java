/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 *
 * The code in this file is a heavily modified version of the original in the RoaringBitmap library; please see
 * https://roaringbitmap.org/
 *
 */

package io.deephaven.engine.rowset.impl.rsp.container;

/**
 * Iterator over short values.
 */
public interface ShortIterator {
    /**
     * @return whether there is another value
     */
    boolean hasNext();

    /**
     * @return next short value
     */
    short next();

    /**
     * @return next short value as int value (using the least significant 16 bits)
     */
    int nextAsInt();
}
