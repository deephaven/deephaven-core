//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.JsonPublishingProvider;
import io.deephaven.json.JsonStreamPublisher;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;

/**
 * The Jackson {@link JsonPublishingProvider} and {@link JsonPublishingProvider}.
 *
 * @see JacksonProcessors
 * @see JacksonStreamPublisher
 * @see JacksonTable
 */
@AutoService({JsonProcessorProvider.class, JsonPublishingProvider.class})
public final class JacksonJsonProvider implements JsonProcessorProvider, JsonPublishingProvider {

    @Override
    public JacksonProcessors provider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonConfiguration.defaultFactory());
    }

    @Override
    public JacksonProcessors namedProvider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonConfiguration.defaultFactory());
    }

    @Override
    public JacksonStreamPublisher of(JsonStreamPublisherOptions options) {
        return JacksonStreamPublisher.of(options, JacksonConfiguration.defaultFactory());
    }

    @Override
    public Table of(JsonTableOptions options) {
        return JacksonTable.of(options, JacksonConfiguration.defaultFactory());
    }
}
