//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.deephaven.json.Value;

import java.util.Objects;

public class ValueAwareException extends JsonProcessingException {

    private final Value value;

    public ValueAwareException(String msg, JsonLocation loc, Value valueContext) {
        super(msg, loc);
        this.value = Objects.requireNonNull(valueContext);
    }

    public ValueAwareException(String msg, JsonLocation loc, Throwable cause, Value valueContext) {
        super(msg, loc, cause);
        this.value = Objects.requireNonNull(valueContext);
    }

    public Value value() {
        return value;
    }

    @Override
    protected String getMessageSuffix() {
        return " for " + value;
    }
}
