/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import java.nio.file.Path;

enum PathsAll implements PathsInternal {
    INSTANCE;

    @Override
    public boolean matches(Path path) {
        return true;
    }
}
