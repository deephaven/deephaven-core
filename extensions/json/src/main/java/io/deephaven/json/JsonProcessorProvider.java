//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface JsonProcessorProvider {
    static JsonProcessorProvider serviceLoader() {
        final Iterator<JsonProcessorProvider> it = ServiceLoader.load(JsonProcessorProvider.class).iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final JsonProcessorProvider provider = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return provider;
    }

    ObjectProcessor.Provider provider(ValueOptions options);

    NamedObjectProcessor.Provider namedProvider(ValueOptions options);
}
