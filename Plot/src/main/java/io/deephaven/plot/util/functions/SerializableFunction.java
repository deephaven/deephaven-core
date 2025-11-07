//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.functions;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A serializable function.
 */
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
