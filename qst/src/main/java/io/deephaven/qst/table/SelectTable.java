package io.deephaven.qst.table;

import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SelectTable extends TableBase {

    public abstract Table parent();

    public abstract List<String> columns();
}
