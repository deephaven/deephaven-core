//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TypedObjectValueBuilderTest {

    public static final ObjectValue TRADE = ObjectValue.builder()
            .putFields("symbol", StringValue.strict())
            .putFields("price", DoubleValue.strict())
            .putFields("size", LongValue.strict())
            .build();

    public static final ObjectValue QUOTE = ObjectValue.builder()
            .putFields("symbol", StringValue.strict())
            .putFields("bid", DoubleValue.strict())
            .putFields("ask", DoubleValue.strict())
            .build();

    public static final TypedObjectValue COMBINED = TypedObjectValue.builder()
            .typeFieldName("type")
            .addSharedFields(ObjectField.of("symbol", StringValue.strict()))
            .putObjects("trade", ObjectValue.builder()
                    .putFields("price", DoubleValue.strict())
                    .putFields("size", LongValue.strict())
                    .build())
            .putObjects("quote", ObjectValue.builder()
                    .putFields("bid", DoubleValue.strict())
                    .putFields("ask", DoubleValue.strict())
                    .build())
            .build();

    @Test
    void builderHelper() {
        final TypedObjectValue combined = TypedObjectValue
                .builder(Map.of("quote", QUOTE, "trade", TRADE))
                .typeFieldName("type")
                .build();
        assertThat(combined).isEqualTo(COMBINED);
    }
}
