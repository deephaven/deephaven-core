//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TypedObjectOptionsTest {


    public static final ObjectOptions TRADE = ObjectOptions.builder()
            .putFields("symbol", StringOptions.strict())
            .putFields("price", DoubleOptions.strict())
            .putFields("size", LongOptions.strict())
            .build();

    public static final ObjectOptions QUOTE = ObjectOptions.builder()
            .putFields("symbol", StringOptions.strict())
            .putFields("bid", DoubleOptions.strict())
            .putFields("ask", DoubleOptions.strict())
            .build();

    public static final TypedObjectOptions COMBINED = TypedObjectOptions.builder()
            .typeFieldName("type")
            .addSharedFields(ObjectFieldOptions.of("symbol", StringOptions.strict()))
            .putObjects("trade", ObjectOptions.builder()
                    .putFields("price", DoubleOptions.strict())
                    .putFields("size", LongOptions.strict())
                    .build())
            .putObjects("quote", ObjectOptions.builder()
                    .putFields("bid", DoubleOptions.strict())
                    .putFields("ask", DoubleOptions.strict())
                    .build())
            .build();

    @Test
    void builderHelper() {
        final TypedObjectOptions combined =
                TypedObjectOptions.builder("type", Map.of("quote", QUOTE, "trade", TRADE)).build();
        assertThat(combined).isEqualTo(COMBINED);
    }
}
