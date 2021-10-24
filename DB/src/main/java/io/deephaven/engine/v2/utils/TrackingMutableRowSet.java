/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.v2.utils.singlerange.SingleRange;

/**
 * Tracking, mutable {@link RowSet}.
 */
public interface TrackingMutableRowSet extends MutableRowSet, TrackingRowSet {
    // TODO-RWC: Should these move?
    boolean USE_PRIORITY_QUEUE_RANDOM_BUILDER =
            Configuration.getInstance().getBooleanWithDefault("TrackingMutableRowSet.usePriorityQueueRandomBuilder", true);

    boolean VALIDATE_COALESCED_UPDATES =
            Configuration.getInstance().getBooleanWithDefault("TrackingMutableRowSet.validateCoalescedUpdates", true);

    boolean BAD_RANGES_AS_ERROR =
            Configuration.getInstance().getBooleanForClassWithDefault(TrackingMutableRowSet.class, "badRangeAsError", true);

    /**
     * Initializes our previous value from the current value.
     *
     * This call is used by operations that manipulate a RowSet while constructing it, but need to set the state at the
     * end of the initial operation to the current state.
     *
     * Calling this in other circumstances will yield undefined results.
     */
    void initializePreviousValue(); // TODO-RWC: I think this can be eliminated

    abstract class AbstractRandomBuilder implements RowSetBuilder.RandomBuilder {
        protected AdaptiveBuilder builder = new AdaptiveBuilder();

        @Override
        public void addKey(final long key) {
            builder.addKey(key);
        }

        @Override
        public void addRange(final long start, final long endInclusive) {
            builder.addRange(start, endInclusive);
        }
    }

    class AdaptiveRowSetBuilder extends AbstractRandomBuilder {
        @Override
        public MutableRowSet build() {
            return new TrackingMutableRowSetImpl(builder.getTreeIndexImpl());
        }
    }

    RowSetFactory FACTORY = new AbstractFactory() {
        @Override
        public RowSetBuilder getRandomBuilder() {
            if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
                return new AdaptiveRowSetBuilder() {};
            } else {
                return TrackingMutableRowSetImpl.makeRandomBuilder();
            }
        }

        @Override
        public SequentialRowSetBuilder getSequentialBuilder() {
            return TrackingMutableRowSetImpl.makeSequentialBuilder();
        }

        @Override
        public MutableRowSet getEmptyRowSet() {
            return TrackingMutableRowSetImpl.getEmptyIndex();
        }

        // For backwards compatibility.
        /**
         * @return {@link #getRandomBuilder()}
         */
        public RowSetBuilder getBuilder() {
            return getRandomBuilder();
        }
    };

    RowSetFactory CURRENT_FACTORY = new AbstractFactory() {
        @Override
        public RowSetBuilder getRandomBuilder() {
            if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
                return new AbstractRandomBuilder() {
                    @Override
                    public MutableRowSet build() {
                        return new MutableRowSetImpl(builder.getTreeIndexImpl());
                    }
                };
            } else {
                return TrackingMutableRowSetImpl.makeCurrentRandomBuilder();
            }
        }

        @Override
        public SequentialRowSetBuilder getSequentialBuilder() {
            return TrackingMutableRowSetImpl.makeCurrentSequentialBuilder();
        }

        @Override
        public MutableRowSet getEmptyRowSet() {
            return new MutableRowSetImpl();
        }

        @Override
        public MutableRowSet getRowSetByRange(final long firstRowKey, final long lastRowKey) {
            return new MutableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
        }

        // For backwards compatibility.
        /**
         * @return {@link #getRandomBuilder()}
         */
        public RowSetBuilder getBuilder() {
            return getRandomBuilder();
        }
    };

}
