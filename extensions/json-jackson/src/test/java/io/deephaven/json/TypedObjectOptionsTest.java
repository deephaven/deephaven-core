//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;

public class TypedObjectOptionsTest {
    private static final ObjectOptions QUOTE_OBJECT = ObjectOptions.builder()
            .putFields("symbol", StringOptions.strict())
            .putFields("bid", DoubleOptions.standard())
            .putFields("ask", DoubleOptions.standard())
            .build();

    private static final ObjectOptions TRADE_OBJECT = ObjectOptions.builder()
            .putFields("symbol", StringOptions.strict())
            .putFields("price", DoubleOptions.standard())
            .putFields("size", DoubleOptions.standard())
            .build();

    private static final TypedObjectOptions QUOTE_OR_TRADE_OBJECT =
            TypedObjectOptions.strict("type", new LinkedHashMap<>() {
                {
                    put("quote", QUOTE_OBJECT);
                    put("trade", TRADE_OBJECT);
                }
            });

    @Test
    void typeDiscriminationQuoteTrade() throws IOException {
        parse(QUOTE_OR_TRADE_OBJECT, List.of(
                // "",
                // "null",
                // "{}",
                // "{\"type\": null}",
                // "{\"type\": \"other\"}",
                "{\"type\": \"quote\", \"symbol\": \"foo\", \"bid\": 1.01, \"ask\": 1.05}",
                "{\"type\": \"trade\", \"symbol\": \"bar\", \"price\": 42.42, \"size\": 123}"),
                ObjectChunk.chunkWrap(new String[] {"quote", "trade"}), // type
                ObjectChunk.chunkWrap(new String[] {"foo", "bar"}), // symbol
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE}), // quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE}), // quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42}), // trade/price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123})); // trade/size
    }
}
