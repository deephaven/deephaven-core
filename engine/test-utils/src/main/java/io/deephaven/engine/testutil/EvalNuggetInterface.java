/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.testutil;

import java.io.IOException;

public interface EvalNuggetInterface {
    void validate(final String msg);

    void show() throws IOException;
}
