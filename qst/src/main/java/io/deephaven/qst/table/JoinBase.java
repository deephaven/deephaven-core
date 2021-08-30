package io.deephaven.qst.table;

import io.deephaven.api.JoinAddition;
import org.immutables.value.Value.Check;

public abstract class JoinBase extends TableBase implements Join {

    @Check
    final void checkAdditions() {
        if (additions().stream().map(JoinAddition::newColumn).distinct().count() != additions()
                .size()) {
            throw new IllegalArgumentException(
                    "Invalid join additions, must not use the same output column multiple times.");
        }
    }
}
