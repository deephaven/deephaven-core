//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.StreamReadFeature;

import java.lang.reflect.InvocationTargetException;

public final class JacksonConfiguration {

    private static final JsonFactory DEFAULT_FACTORY;

    static {
        // We'll attach an ObjectMapper if it's on the classpath, this allows parsing of AnyOptions
        ObjectCodec objectCodec = null;
        try {
            final Class<?> clazz = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
            objectCodec = (ObjectCodec) clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            // ignore
        }
        DEFAULT_FACTORY = defaultFactoryBuilder().build().setCodec(objectCodec);
    }

    /**
     * Constructs a Deephaven-configured json factory builder. This currently includes
     * {@link StreamReadFeature#USE_FAST_DOUBLE_PARSER}, {@link StreamReadFeature#USE_FAST_BIG_NUMBER_PARSER}, and
     * {@link StreamReadFeature#INCLUDE_SOURCE_IN_LOCATION}. The specific configuration may change in the future.
     *
     * @return the Deephaven-configured json factory builder
     */
    public static JsonFactoryBuilder defaultFactoryBuilder() {
        return new JsonFactoryBuilder()
                .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                .enable(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION);
    }

    // Not currently public, but javadoc still useful to ensure internal callers don't modify.
    /**
     * Returns a Deephaven-configured json factory singleton. Callers should not modify the returned factory in any way.
     * This has been constructed as the singleton-equivalent of {@code defaultFactoryBuilder().build()}, with an
     * ObjectMapper set as the codec if it is on the classpath.
     *
     * @return the Deephaven-configured json factory singleton
     * @see #defaultFactoryBuilder()
     */
    static JsonFactory defaultFactory() {
        return DEFAULT_FACTORY;
    }
}
