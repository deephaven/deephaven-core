//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.junit.jupiter.api.Test;

public class JsonValueTypesTest {

    @Test
    public void all() {
        JsonValueTypes.checkInvariants(JsonValueTypes.ALL);
    }

    @Test
    public void numberLike() {
        JsonValueTypes.checkInvariants(JsonValueTypes.NUMBER_LIKE);
    }

    @Test
    public void stringLike() {
        JsonValueTypes.checkInvariants(JsonValueTypes.STRING_LIKE);
    }

    @Test
    public void stringOrNull() {
        JsonValueTypes.checkInvariants(JsonValueTypes.STRING_OR_NULL);
    }

    @Test
    public void objectOrNull() {
        JsonValueTypes.checkInvariants(JsonValueTypes.OBJECT_OR_NULL);
    }

    @Test
    public void arrayOrNull() {
        JsonValueTypes.checkInvariants(JsonValueTypes.ARRAY_OR_NULL);
    }

    @Test
    public void numberIntOrNull() {
        JsonValueTypes.checkInvariants(JsonValueTypes.INT_OR_NULL);
    }

}
