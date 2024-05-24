//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.deephaven.json.Value;

import java.io.IOException;
import java.util.Objects;

final class Exceptions {

    static IOException notAllowed(JsonParser parser) {
        return new IOException(String.format("Token '%s' not allowed", parser.currentToken()));
    }

    static IOException missingNotAllowed(JsonParser parser) {
        return new IOException("Missing token not allowed");
    }

    static IOException notAllowed(JsonParser parser, Mixin<?> mixin) {
        final JsonLocation location = parser.currentLocation();
        return new ValueAwareException(String.format("Token '%s' not allowed", parser.currentToken()), location,
                mixin.options);
    }

    static IOException missingNotAllowed(JsonParser parser, Mixin<?> mixin) {
        final JsonLocation location = parser.currentLocation();
        return new ValueAwareException("Missing token not allowed", location, mixin.options);
    }

    public static class ValueAwareException extends JsonProcessingException {

        private final Value value;

        public ValueAwareException(String msg, JsonLocation loc, Value valueContext) {
            super(msg, loc);
            this.value = Objects.requireNonNull(valueContext);
        }

        public ValueAwareException(String msg, JsonLocation loc, Throwable cause, Value valueContext) {
            super(msg, loc, cause);
            this.value = Objects.requireNonNull(valueContext);
        }

        @Override
        protected String getMessageSuffix() {
            return " for " + value;
        }
    }
}
