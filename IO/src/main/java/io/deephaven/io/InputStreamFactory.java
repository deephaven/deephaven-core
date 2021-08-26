/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io;

import java.io.IOException;
import java.io.InputStream;

// --------------------------------------------------------------------
public interface InputStreamFactory {
    InputStream createInputStream() throws IOException;

    String getDescription();
}
