/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.db.plot.util.functions;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * A serializable binary function. <br/>
 */
public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {
}
