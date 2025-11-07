//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.Node;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import static guru.nidi.graphviz.model.Factory.*;

/**
 * <p>
 * Generate visualizations of update graph listeners.
 * </p>
 *
 * <p>
 * The UpdateAncestorViz class generates visualizations for dependency relationships among UpdateGraph Listener
 * {@link io.deephaven.engine.table.impl.perf.PerformanceEntry performance entries} using data from provided by the
 * {@link io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker update performance tables}. It creates
 * representations in the Graphviz DOT format or SVG.
 * </p>
 */
public class UpdateAncestorViz {
    final Map<Long, String> entryToId = new HashMap<>();
    final Map<Long, LongVector> entryToAncestors = new HashMap<>();
    final Set<Long> visited = new HashSet<>();

    private UpdateAncestorViz(Table updatePerformanceLog, Table updateAncestors) {
        try (CloseablePrimitiveIteratorOfLong id = updatePerformanceLog.longColumnIterator("EntryId");
                CloseableIterator<String> description = updatePerformanceLog.columnIterator("EntryDescription")) {
            while (id.hasNext()) {
                entryToId.put(id.nextLong(), description.next());
            }
        }

        try (CloseablePrimitiveIteratorOfLong id = updateAncestors.longColumnIterator("EntryId");
                CloseableIterator<String> description = updateAncestors.columnIterator("EntryDescription");
                CloseableIterator<LongVector> ancestors = updateAncestors.columnIterator("Ancestors")) {
            while (id.hasNext()) {
                final long idValue = id.nextLong();
                entryToId.put(idValue, description.next());
                entryToAncestors.compute(idValue, (ignoredKey, oldValue) -> {
                    final LongVector next = ancestors.next();
                    if (oldValue == null) {
                        return next;
                    } else {
                        final long[] newArray = Arrays.copyOf(oldValue.toArray(), oldValue.intSize() + next.intSize());
                        System.arraycopy(next.toArray(), 0, newArray, oldValue.intSize(), next.intSize());
                        return new LongVectorDirect(newArray);
                    }
                });
            }
        }
    }

    /**
     * Generates an SVG representation of ancestors based on the {@link TableLoggers#updatePerformanceLog() update
     * performance log} and the {@link TableLoggers#updatePerformanceAncestorsLog() update performance ancestors log}.
     * 
     * @param entryIds an array of performance entry IDs to traverse and generate a graph for
     * @param updatePerformanceLog a table containing update performance log data
     * @param updateAncestors a table containing update ancestor data
     * @param filename an optional file to which the SVG will be written; if null, the SVG is not written to a file
     * @return a byte array representing the SVG output
     * @throws IOException if an I/O error occurs while writing to the file
     */
    public static byte[] svg(long[] entryIds, Table updatePerformanceLog, Table updateAncestors, File filename)
            throws IOException {
        UpdateAncestorViz updateAncestorViz = new UpdateAncestorViz(updatePerformanceLog, updateAncestors);
        byte[] bytes = updateAncestorViz.makeSvg(entryIds);
        if (filename != null) {
            try (FileOutputStream fos = new FileOutputStream(filename, false)) {
                fos.write(bytes);
            }
        }
        return bytes;
    }

    /**
     * Generates a Graphviz DOT representation of ancestors based on the {@link TableLoggers#updatePerformanceLog()
     * update performance log} and the {@link TableLoggers#updatePerformanceAncestorsLog() update performance ancestors
     * log}.
     * 
     * @param entryIds an array of performance entry IDs to traverse and generate a graph for
     * @param updatePerformanceLog a table containing update performance log data
     * @param updateAncestors a table containing update ancestor data
     * @return a byte array representing the SVG output
     */
    public static String dot(long[] entryIds, Table updatePerformanceLog, Table updateAncestors) {
        UpdateAncestorViz updateAncestorViz = new UpdateAncestorViz(updatePerformanceLog, updateAncestors);
        return updateAncestorViz.makeDot(entryIds);
    }

    @TestUseOnly
    static Graph graph(long[] entryIds, Table updatePerformanceLog, Table updateAncestors) {
        UpdateAncestorViz updateAncestorViz = new UpdateAncestorViz(updatePerformanceLog, updateAncestors);
        return updateAncestorViz.makeGraph(entryIds);
    }

    private byte[] makeSvg(long[] entryIds) throws IOException {
        Graphviz graphviz = Graphviz.fromGraph(makeGraph(entryIds));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        graphviz.render(Format.SVG).toOutputStream(baos);

        return baos.toByteArray();
    }

    private String makeDot(long[] entryIds) {
        Graphviz graphviz = Graphviz.fromGraph(makeGraph(entryIds));
        return graphviz.render(Format.DOT).toString();
    }

    @NotNull
    private Graph makeGraph(long[] entryIds) {
        Graph g = guru.nidi.graphviz.model.Factory.graph("deephaven_update_graph").directed()
                .graphAttr().with(Rank.dir(Rank.RankDir.TOP_TO_BOTTOM));

        for (long id : entryIds) {
            g = addEntry(g, id);
        }
        return g;
    }

    private Graph addEntry(Graph g, long id) {
        if (!visited.add(id)) {
            return g;
        }

        final String description = entryToId.get(id);
        if (description == null) {
            return g;
        }

        Node node = node("n" + id).with("label", id + " " + description);

        LongVector ancestors = entryToAncestors.get(id);
        if (ancestors != null) {
            for (long ancestor : ancestors) {
                g = addEntry(g, ancestor);
                node = node.link("n" + ancestor);
            }
        }

        return g.with(node);
    }
}
