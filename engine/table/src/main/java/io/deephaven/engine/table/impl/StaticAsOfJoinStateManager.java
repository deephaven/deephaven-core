package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public abstract class StaticAsOfJoinStateManager {
    public static final long NO_RIGHT_ENTRY_VALUE = RowSequence.NULL_ROW_KEY;

    protected final ColumnSource<?>[] keySourcesForErrorMessages;

    protected StaticAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        this.keySourcesForErrorMessages = keySourcesForErrorMessages;
    }

    @SuppressWarnings("WeakerAccess")
    public void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide) {
        if (exactMatch && rightSide == NO_RIGHT_ENTRY_VALUE) {
            throw new RuntimeException("Tables don't have one-to-one mapping - no mappings for key " + extractKeyStringFromSourceTable(leftKeyIndex) + ".");
        }
    }

    // produce a pretty key for error messages
    protected String extractKeyStringFromSourceTable(long leftKey) {
        if (keySourcesForErrorMessages.length == 1) {
            return Objects.toString(keySourcesForErrorMessages[0].get(leftKey));
        }
        return "[" + Arrays.stream(keySourcesForErrorMessages).map(ls -> Objects.toString(ls.get(leftKey))).collect(Collectors.joining(", ")) + "]";
    }
}
