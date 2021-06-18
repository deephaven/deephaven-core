package io.deephaven.qst.manager;

import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.Table;
import java.io.Closeable;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Keeps exported references to a chain of ancestors for a given {@link Table}.
 */
public class ExportedTableChain implements Closeable {

    /**
     * Creates an exported chain of all ancestors for {@code root}.
     *
     * @param manager the export manager
     * @param root the root table
     * @return the exported chain
     *
     * @see ParentsVisitor#getAncestorsAndSelf(Table)
     */
    public static ExportedTableChain of(ExportManager manager, Table root) {
        final Set<Table> chain = ParentsVisitor.getAncestorsAndSelf(root)
            .collect(Collectors.toCollection(LinkedHashSet::new));
        return new ExportedTableChain(root, ExportedTables.of(manager, chain), Integer.MAX_VALUE);
    }

    /**
     * Creates an exported chain of all ancestors for {@code root}, up to {@code maxDepth}.
     *
     * @param manager the export manager
     * @param root the root table
     * @param maxDepth the maximum depth
     * @return the exported chain
     *
     * @see ParentsVisitor#getAncestorsAndSelf(Table, int)
     */
    public static ExportedTableChain of(ExportManager manager, Table root, int maxDepth) {
        final Set<Table> chain = ParentsVisitor.getAncestorsAndSelf(root, maxDepth)
            .collect(Collectors.toCollection(LinkedHashSet::new));
        return new ExportedTableChain(root, ExportedTables.of(manager, chain), maxDepth);
    }

    private final Table root;
    private final ExportedTables exportedTables;
    private final int maxDepth;

    private ExportedTableChain(Table root, ExportedTables exportedTables, int maxDepth) {
        this.root = Objects.requireNonNull(root);
        this.exportedTables = Objects.requireNonNull(exportedTables);
        this.maxDepth = maxDepth;
    }

    public final Table table() {
        return root;
    }

    public final int getMaxDepth() {
        return maxDepth;
    }

    public final boolean isFullDepth() {
        return maxDepth == Integer.MAX_VALUE;
    }

    public final ExportedTable newRef() {
        return exportedTables.newRef(root);
    }

    public final ExportedTableChain newChainRef() {
        return new ExportedTableChain(root, exportedTables.newRefs(), maxDepth);
    }

    public final boolean isReleased() {
        return exportedTables.isReleased();
    }

    public final void release() {
        exportedTables.release();
    }

    @Override
    public final void close() {
        release();
    }
}
