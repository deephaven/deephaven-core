/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.formatters;

import java.util.BitSet;

import io.deephaven.base.log.LogOutputAppendable;
import org.jetbrains.annotations.NotNull;

public class FormatBitSet {
    public static LogOutputAppendable formatBitSet(final BitSet bitSet) {
        return logOutput -> {
            logOutput.append("{");
            boolean first = true;
            for (int runStart = 0; runStart < bitSet.size();) {
                if (bitSet.get(runStart)) {
                    if (first) {
                        first = false;
                    } else {
                        logOutput.append(", ");
                    }


                    logOutput.append(runStart);
                    int runEnd;
                    // noinspection StatementWithEmptyBody
                    for (runEnd = runStart + 1; runEnd < bitSet.size() && bitSet.get(runEnd); runEnd++);
                    if (runEnd > runStart + 1) {
                        logOutput.append("-").append(runEnd - 1);
                    }
                    runStart = runEnd;
                } else {
                    ++runStart;
                }
            }
            logOutput.append("}");
            return logOutput;
        };
    }

    public static String formatBitSetAsString(final BitSet bitSet) {
        StringBuilder builder = new StringBuilder();

        builder.append("{");
        boolean first = true;
        for (int runStart = 0; runStart < bitSet.size();) {
            if (bitSet.get(runStart)) {
                if (first) {
                    first = false;
                } else {
                    builder.append(", ");
                }


                builder.append(runStart);
                int runEnd;
                // noinspection StatementWithEmptyBody
                for (runEnd = runStart + 1; runEnd < bitSet.size() && bitSet.get(runEnd); runEnd++);
                if (runEnd > runStart + 1) {
                    builder.append("-").append(runEnd - 1);
                }
                runStart = runEnd;
            } else {
                ++runStart;
            }
        }
        builder.append("}");

        return builder.toString();
    }

    @NotNull
    public static BitSet arrayToBitSet(Object[] array) {
        if (array == null) {
            return new BitSet();
        }

        BitSet populated = new BitSet(array.length);
        for (int ii = 0; ii < array.length; ++ii) {
            if (array[ii] != null) {
                populated.set(ii);
            }
        }
        return populated;
    }

    @NotNull
    public static LogOutputAppendable arrayToLog(Object[] array) {
        if (array == null) {
            return formatBitSet(new BitSet());
        }

        BitSet populated = new BitSet(array.length);
        for (int ii = 0; ii < array.length; ++ii) {
            if (array[ii] != null) {
                populated.set(ii);
            }
        }
        return formatBitSet(populated);
    }
}
