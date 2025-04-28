//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.util.TableLoggers;

import java.util.stream.LongStream;

/**
 * An interface to indicate that an object has refreshing behavior and can provide {@link PerformanceEntry performance
 * entry} identifiers.
 *
 * <p>
 * The provided performance entries are used to generate entries in the
 * {@link TableLoggers#updatePerformanceAncestorsLog() ancestors log}, for objects that are not Listeners themselves to
 * participate in parent discovery.
 * </p>
 */
public interface HasParentPerformanceIds {
    LongStream parentPerformanceEntryIds();
}
