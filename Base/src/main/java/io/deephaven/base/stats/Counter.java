/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import io.deephaven.base.Function;

// --------------------------------------------------------------------
/**
 * A statistic where each value represents a additive quantity, and thus the sum of the values <U>does</U> have meaning.
 * Examples include event counts and processing duration. If the sum of the values <I>does not</I> have a useful
 * interpretation, use {@link State} instead.
 * <UL>
 * <LI>{@link #increment} updates the counter, recording a single value. This is the most common usage. ({@link #sample}
 * does exactly the same thing but is a poor verb to use with a Counter.)
 * <LI>{@link #incrementFromSample} updates the counter, recording a value that is the difference between this sample
 * and the last sample. (The first call just sets the "last" sample and does not record a value.) For example, this can
 * be used to CPU usage rate when only a running total is available by periodically sampling the running total.
 * </UL>
 */
public class Counter extends Value {

    public static char TYPE_TAG = 'C';

    public Counter(long now) {
        super(now);
    }

    long previousSample = Long.MIN_VALUE;

    public void incrementFromSample(long n) {
        if (Long.MIN_VALUE != previousSample) {
            sample(n - previousSample);
        }
        previousSample = n;
    }

    public char getTypeTag() {
        return TYPE_TAG;
    }

    public static final Function.Unary<Counter, Long> FACTORY = new Function.Unary<Counter, Long>() {
        public Counter call(Long now) {
            return new Counter(now);
        }
    };
}
