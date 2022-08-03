package io.deephaven.web;

import java.nio.file.Path;
import java.util.Objects;

public final class WebLayout {
    private final Path path;

    public WebLayout(Path path) {
        this.path = Objects.requireNonNull(path);
    }

    Path path() {
        return path;
    }
}
