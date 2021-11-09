/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.tables.utils.DateTime;

/**
 * Interface for providing the current time.
 */
public interface TimeProvider {
    DateTime currentTime();
}
