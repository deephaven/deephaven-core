/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.uri;

import java.net.URI;

public abstract class DeephavenUriBase extends StructuredUriBase implements DeephavenUri {

    @Override
    public final URI toURI() {
        return URI.create(toString());
    }

    @Override
    public abstract String toString();
}
