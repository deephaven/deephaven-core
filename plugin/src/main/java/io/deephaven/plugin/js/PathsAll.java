//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.js;

import java.nio.file.Path;

enum PathsAll implements PathsInternal {
    ALL;

    @Override
    public boolean matches(Path path) {
        return true;
    }
}
