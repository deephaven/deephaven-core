package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.engine.table.ColumnSource;

public abstract class StaticNaturalJoinStateManagerTypedBase extends StaticHashedNaturalJoinStateManager {
    StaticNaturalJoinStateManagerTypedBase(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }
}
