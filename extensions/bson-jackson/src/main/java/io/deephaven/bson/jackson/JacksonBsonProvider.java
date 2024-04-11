//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.jackson.JacksonProvider;

public final class JacksonBsonProvider {

    /**
     * Creates a jackson BSON provider using a default factory.
     *
     * @param options the object options
     * @return the jackson BSON provider
     * @see #of(ValueOptions, BsonFactory)
     */
    public static JacksonProvider of(ValueOptions options) {
        return of(options, JacksonBsonConfiguration.defaultFactory());
    }

    /**
     * Creates a jackson BSON provider using the provided {@code factory}.
     *
     * @param options the object options
     * @param factory the jackson BSON factory
     * @return the jackson BSON provider
     */
    public static JacksonProvider of(ValueOptions options, BsonFactory factory) {
        return JacksonProvider.of(options, factory);
    }
}
