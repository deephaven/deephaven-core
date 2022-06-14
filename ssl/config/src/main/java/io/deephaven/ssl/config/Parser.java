/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class Parser {
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.findAndRegisterModules();
    }

    public static <T> T parseJson(File file, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(file, clazz);
    }

    public static <T> T parseJson(String value, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(value, clazz);
    }

    public static <T> T parseJson(URL url, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(url, clazz);
    }
}
