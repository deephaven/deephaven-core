//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A set of JoinControl objects useful for unit tests.
 */
public class TestJoinControl {
    static final JoinControl DEFAULT_JOIN_CONTROL = new JoinControl();

    static final JoinControl BUILD_LEFT_CONTROL = new JoinControl() {
        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.LeftInput, initialBuildSize());
        }
    };

    static final JoinControl BUILD_RIGHT_CONTROL = new JoinControl() {
        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.RightInput, initialBuildSize());
        }
    };

    /**
     * A small hash table that is nearly full before it grows, so that builds rehash and probes search long runs of
     * occupied slots.
     */
    static final JoinControl SMALL_TABLE_JOIN_CONTROL = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 16;
        }

        @Override
        public double getTargetLoadFactor() {
            return 0.9;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 0.95;
        }
    };

    public static final JoinControl SMALL_TABLE_BUILD_LEFT = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 16;
        }

        @Override
        public double getTargetLoadFactor() {
            return 0.9;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 0.95;
        }

        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.LeftInput, initialBuildSize());
        }
    };

    public static final JoinControl SMALL_TABLE_BUILD_RIGHT = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 16;
        }

        @Override
        public double getTargetLoadFactor() {
            return 0.9;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 0.95;
        }

        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.RightInput, initialBuildSize());
        }
    };
}
