//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import java.util.function.Supplier;

/**
 * Interface for a supplier that continues to "live" while retained by a {@link LivenessManager}.
 */
public interface LiveSupplier<TYPE> extends LivenessReferent, Supplier<TYPE> {
}
