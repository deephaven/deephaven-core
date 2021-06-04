package io.deephaven.qst.table;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class TailTable extends TableBase {

    @Parameter
    public abstract Table parent();

    @Parameter
    public abstract long size();

    @Check
    final void checkSize() {
        if (size() <= 0) {
            throw new IllegalArgumentException("Must have positive size");
        }
    }
}
