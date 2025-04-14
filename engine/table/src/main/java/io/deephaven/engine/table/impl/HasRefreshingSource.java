//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.util.TableLoggers;
import org.jetbrains.annotations.NotNull;

import java.util.stream.Stream;

/**
 * An interface to indicate that a source has refreshing behavior and can provide {@link PerformanceEntry performance
 * entries}.
 *
 * <p>
 * The provided performance entries are used to generate entries in the
 * {@link TableLoggers#updatePerformanceAncestorsLog() ancestors log}, so that refreshing roots that typically do not
 * have a {@link BaseTable.ListenerImpl} or {@link MergedListener} can participate in ancestor discovery.
 * </p>
 */
public interface HasRefreshingSource {
    /**
     * Return a stream of performance entries that represent the {@link InstrumentedTableUpdateSource} which drive this
     * table.
     *
     * <p>
     * If no such sources, exist, then an empty stream is returned.
     * </p>
     * 
     * @return a stream of performance entries for this node in the graph
     */
    @NotNull
    Stream<PerformanceEntry> sourceEntries();
}
