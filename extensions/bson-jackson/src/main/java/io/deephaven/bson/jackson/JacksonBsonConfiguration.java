//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.bson.jackson;

import com.fasterxml.jackson.core.ObjectCodec;
import de.undercouch.bson4jackson.BsonFactory;

import java.lang.reflect.InvocationTargetException;

final class JacksonBsonConfiguration {
    private static final BsonFactory DEFAULT_FACTORY;

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
        DEFAULT_FACTORY = new BsonFactory(objectCodec);
    }

    static BsonFactory defaultFactory() {
        return DEFAULT_FACTORY;
    }
}
