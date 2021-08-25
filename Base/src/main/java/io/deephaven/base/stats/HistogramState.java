/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import io.deephaven.base.Function;

public class HistogramState extends State {

    public static char TYPE_TAG = 'H';

    private long rangeMin, rangeMax;
    private double rangeBucket;
    private State[] buckets;

    private int getBucket(long sample) {
        if (sample < rangeMin)
            return 0;
        if (sample >= rangeMax)
            return buckets.length - 1;
        return 1 + (int) ((sample - rangeMin) / rangeBucket);
    }

    public static class Spec { // For packaging as a single argument to makeItem through the FACTORY
                               // (see below)
        String groupName;
        String itemName;
        long rangeMin;
        long rangeMax;
        int numBuckets;

        public Spec(String groupName, String itemName, long rangeMin, long rangeMax,
            int numBuckets) {
            this.groupName = groupName;
            this.itemName = itemName;
            this.rangeMin = rangeMin;
            this.rangeMax = rangeMax;
            this.numBuckets = numBuckets;
        }
    }

    public HistogramState(long now, Spec spec) {
        super(now);
        this.rangeMin = spec.rangeMin;
        this.rangeMax = spec.rangeMax;
        this.rangeBucket = (double) (rangeMax - rangeMin) / spec.numBuckets;
        this.buckets = new State[spec.numBuckets + 2];
        this.buckets[0] = Stats.makeItem(spec.groupName, spec.itemName + "[0]", State.FACTORY,
            "Values of " + spec.itemName + " less than " + rangeMin, now).getValue();
        for (int i = 1; i <= spec.numBuckets; ++i) {
            this.buckets[i] =
                Stats.makeItem(spec.groupName, spec.itemName + '[' + i + ']', State.FACTORY,
                    "Values of " + spec.itemName + " between "
                        + (long) (rangeMin + (i - 1) * rangeBucket) + " (incl.) and "
                        + (long) (rangeMin + i * rangeBucket) + " (excl.)",
                    now).getValue();
        }
        this.buckets[spec.numBuckets + 1] = Stats
            .makeItem(spec.groupName, spec.itemName + '[' + (spec.numBuckets + 1) + ']',
                State.FACTORY, "Values of " + spec.itemName + " at least " + rangeMax, now)
            .getValue();
    }

    public char getTypeTag() {
        return TYPE_TAG;
    }

    @Override
    public Value alwaysUpdated(boolean b) {
        for (int i = 0; i < buckets.length; ++i) {
            buckets[i].alwaysUpdated(b);
        }
        return super.alwaysUpdated(b);
    }

    public void sample(long x) {
        super.sample(x);
        buckets[getBucket(x)].sample(x);
    }

    public void reset() {
        super.reset();
        for (int i = 0; i < buckets.length; ++i) {
            buckets[i].reset();
        }
    }

    public static final Function.Binary<HistogramState, Long, Spec> FACTORY =
        new Function.Binary<HistogramState, Long, Spec>() {
            public HistogramState call(Long now, Spec spec) {
                return new HistogramState(now, spec);
            }
        };
}
