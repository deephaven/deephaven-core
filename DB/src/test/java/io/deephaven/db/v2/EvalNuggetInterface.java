/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import java.io.IOException;

import static io.deephaven.db.tables.utils.TableTools.diff;

public interface EvalNuggetInterface {
    void validate(final String msg);
    void show() throws IOException;
}
