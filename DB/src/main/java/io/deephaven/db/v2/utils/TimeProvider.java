/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.utils.DBDateTime;

/**
 * Interface for providing the current time.
 */
public interface TimeProvider {
    DBDateTime currentTime();
}
