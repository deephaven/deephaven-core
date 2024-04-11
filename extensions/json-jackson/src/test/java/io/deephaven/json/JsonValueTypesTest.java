//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.junit.jupiter.api.Test;

public class JsonValueTypesTest {

    @Test
    public void all() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.ALL);
    }

    @Test
    public void numberLike() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.NUMBER_LIKE);
    }

    @Test
    public void stringLike() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.STRING_LIKE);
    }

    @Test
    public void stringOrNull() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.STRING_OR_NULL);
    }

    @Test
    public void objectOrNull() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.OBJECT_OR_NULL);
    }

    @Test
    public void arrayOrNull() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.ARRAY_OR_NULL);
    }

    @Test
    public void numberIntOrNull() {
        JsonValueTypes.checkAllowedTypeInvariants(JsonValueTypes.INT_OR_NULL);
    }

}
