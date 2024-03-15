//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class RightIncrementalAsOfJoinStateManager {

    protected final ColumnSource<?>[] keySourcesForErrorMessages;

    protected RightIncrementalAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        this.keySourcesForErrorMessages = keySourcesForErrorMessages;
    }

    // produce a pretty key for error messages
    protected String extractKeyStringFromSourceTable(long leftKey) {
        if (keySourcesForErrorMessages.length == 1) {
            return Objects.toString(keySourcesForErrorMessages[0].get(leftKey));
        }
        return "[" + Arrays.stream(keySourcesForErrorMessages).map(ls -> Objects.toString(ls.get(leftKey)))
                .collect(Collectors.joining(", ")) + "]";
    }
}
