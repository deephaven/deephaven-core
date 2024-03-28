//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.engine.table.Table;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface JsonPublishingProvider {

    static JsonPublishingProvider serviceLoader() {
        final Iterator<JsonPublishingProvider> it = ServiceLoader.load(JsonPublishingProvider.class).iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final JsonPublishingProvider provider = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return provider;
    }

    JsonStreamPublisher of(JsonStreamPublisherOptions options);

    Table of(JsonTableOptions options);
}
