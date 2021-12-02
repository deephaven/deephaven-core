package io.deephaven.qst.table;

import io.deephaven.api.Selectable;

import java.util.List;

public abstract class ByTableBase extends TableBase implements SingleParentTable {

    public abstract List<Selectable> columns();
}
