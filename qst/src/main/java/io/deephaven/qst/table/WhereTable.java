package io.deephaven.qst.table;

import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
public abstract class WhereTable extends TableBase {

    public abstract Table parent();

    public abstract List<String> filters();
}
