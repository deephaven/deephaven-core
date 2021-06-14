package io.deephaven.qst.table;

import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class NaturalJoinTable extends TableBase {

    public abstract Table left();

    public abstract Table right();

    public abstract List<String> matches();

    public abstract List<String> additions();
}
