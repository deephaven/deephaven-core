//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ToObject<T> {

    T parseValue(JsonParser parser) throws IOException;

    T parseMissing(JsonParser parser) throws IOException;
}
