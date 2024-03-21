//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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

    static final JoinControl OVERFLOW_JOIN_CONTROL = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 16;
        }

        @Override
        public double getTargetLoadFactor() {
            return 19;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 20;
        }
    };

    public static final JoinControl OVERFLOW_BUILD_LEFT = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 16;
        }

        @Override
        public double getTargetLoadFactor() {
            return 19;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 20;
        }

        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.LeftInput, initialBuildSize());
        }
    };

    public static final JoinControl OVERFLOW_BUILD_RIGHT = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 16;
        }

        @Override
        public double getTargetLoadFactor() {
            return 19;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 20;
        }

        @Override
        BuildParameters buildParameters(
                @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
                @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
            return new BuildParameters(BuildParameters.From.RightInput, initialBuildSize());
        }
    };
}
