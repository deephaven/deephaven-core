package io.deephaven.client;

import io.deephaven.qst.table.Table;
import java.util.Collection;
import java.util.List;

public interface ExportManager {

    ExportedTable export(Table table);

    List<ExportedTable> export(Collection<Table> tables);

    // TODO: support batch release in the future
    // void release(Collection<ExportedTable> exports);
}
