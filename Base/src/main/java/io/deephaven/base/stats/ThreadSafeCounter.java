//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import java.util.function.LongFunction;

// --------------------------------------------------------------------

/**
 * A statistic where each value represents an additive quantity, and thus the sum of the values <U>does</U> have
 * meaning. Examples include event counts and processing duration. If the sum of the values <I>does not</I> have a
 * useful interpretation, use {@link State} instead.
 * <UL>
 * <LI>{@link #increment} updates the counter, recording a single value. This is the most common usage. ({@link #sample}
 * does exactly the same thing but is a poor verb to use with a Counter.)
 * </UL>
 */
public class ThreadSafeCounter extends ThreadSafeValue {

    public ThreadSafeCounter(final long now) {
        super(now);
    }

    public char getTypeTag() {
        return Counter.TYPE_TAG;
    }

    public static final LongFunction<ThreadSafeCounter> FACTORY = ThreadSafeCounter::new;
}
