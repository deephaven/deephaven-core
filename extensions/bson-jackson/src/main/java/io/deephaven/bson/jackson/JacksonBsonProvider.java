//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import io.deephaven.engine.table.Table;
import io.deephaven.json.JsonProcessorProvider;
import io.deephaven.json.JsonPublishingProvider;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.JacksonProcessors;
import io.deephaven.json.jackson.JacksonStreamPublisher;
import io.deephaven.json.jackson.JacksonTable;

// Not hooking up auto-service, is not the default
public final class JacksonBsonProvider implements JsonProcessorProvider, JsonPublishingProvider {

    @Override
    public JacksonProcessors provider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonBsonConfiguration.defaultFactory());
    }

    @Override
    public JacksonProcessors namedProvider(ValueOptions options) {
        return JacksonProcessors.of(options, JacksonBsonConfiguration.defaultFactory());
    }

    @Override
    public JacksonStreamPublisher of(JsonStreamPublisherOptions options) {
        return JacksonStreamPublisher.of(options, JacksonBsonConfiguration.defaultFactory());
    }

    @Override
    public Table of(JsonTableOptions options) {
        return JacksonTable.of(options, JacksonBsonConfiguration.defaultFactory());
    }
}
