package io.deephaven.qst.manager;

import io.deephaven.qst.table.Table;
import java.util.Collection;
import java.util.List;

/**
 * An export manager manages the protocol and caching around executing exports with respect to the
 * server.
 */
public interface ExportManager {

    /**
     * Equivalent to {@code ExportedTableChain.of(manager, root)}.
     *
     * @param manager the manager
     * @param root the table
     * @return the exported table chain
     * @see ExportedTableChain#of(ExportManager, Table)
     */
    static ExportedTableChain exportChain(ExportManager manager, Table root) {
        return ExportedTableChain.of(manager, root);
    }

    /**
     * Equivalent to {@code ExportedTableChain.of(manager, root, maxDepth)}.
     *
     * @param manager the manager
     * @param root the table
     * @param maxDepth the max depth
     * @return the exported table chain
     * @see ExportedTableChain#of(ExportManager, Table, int)
     */
    static ExportedTableChain exportChain(ExportManager manager, Table root, int maxDepth) {
        return ExportedTableChain.of(manager, root, maxDepth);
    }

    /**
     * Export a single table.
     *
     * @param table the table
     * @return the exported table
     */
    ExportedTable export(Table table);

    /**
     * Export a collection of tables. Note: the same table may be exported multiple times.
     *
     * @param tables the tables
     * @return the exported table
     */
    List<ExportedTable> export(Collection<Table> tables);

    // TODO: support batch release in the future
    // void release(Collection<ExportedTable> exports);
}
