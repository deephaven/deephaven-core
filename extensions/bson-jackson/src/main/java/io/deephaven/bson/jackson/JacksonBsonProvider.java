//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import de.undercouch.bson4jackson.BsonFactory;
import io.deephaven.json.Value;
import io.deephaven.json.jackson.JacksonProvider;

public final class JacksonBsonProvider {

    /**
     * Creates a jackson BSON provider using a default factory.
     *
     * @param options the object options
     * @return the jackson BSON provider
     * @see #of(Value, BsonFactory)
     */
    public static JacksonProvider of(Value options) {
        return of(options, JacksonBsonConfiguration.defaultFactory());
    }

    /**
     * Creates a jackson BSON provider using the provided {@code factory}.
     *
     * @param options the object options
     * @param factory the jackson BSON factory
     * @return the jackson BSON provider
     */
    public static JacksonProvider of(Value options, BsonFactory factory) {
        return JacksonProvider.of(options, factory);
    }
}
