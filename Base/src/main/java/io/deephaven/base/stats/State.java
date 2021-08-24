/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import io.deephaven.base.Function;

// --------------------------------------------------------------------
/**
 * A statistic where each value represents a snapshot of the state of the system, and thus the sum
 * of the values <U>does not</U> have any meaning. Examples include queue size and latency. If the
 * sum of the values <I>does</I> have a useful interpretation, use {@link Counter} instead.
 * <UL>
 * <LI>{@link #sample} takes a sample, recording a single value. This is the most common usage.
 * ({@link #increment} does exactly the same thing but is a poor verb to use with a State.)
 * <LI>{@link #sampleFromIncrement} takes a sample, recording a value that is the last sample plus
 * the given increment. (If no samples have yet been taken, the "last" sample is assumed to be 0).
 * For example, this can be used to track a queue's size by calling it every time an item is added
 * or removed.
 * </UL>
 */
public class State extends Value {

    public static char TYPE_TAG = 'S';

    public State(long now) {
        super(now);
    }

    public void sampleFromIncrement(long n) {
        sample(last + n);
    }

    public char getTypeTag() {
        return TYPE_TAG;
    }

    public static final Function.Unary<State, Long> FACTORY = new Function.Unary<State, Long>() {
        public State call(Long now) {
            return new State(now);
        }
    };
}
