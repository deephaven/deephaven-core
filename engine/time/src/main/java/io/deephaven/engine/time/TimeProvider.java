/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.time;

import io.deephaven.engine.time.DateTime;

/**
 * Interface for providing the current time.
 */
public interface TimeProvider {
    DateTime currentTime();
}
