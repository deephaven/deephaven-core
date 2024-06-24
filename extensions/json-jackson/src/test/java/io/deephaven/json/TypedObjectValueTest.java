//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import static io.deephaven.json.TestHelper.parse;
import static io.deephaven.json.TestHelper.process;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class TypedObjectValueTest {

    private static final ObjectValue SYMBOL_INFO = ObjectValue.builder()
            .putFields("name", StringValue.strict())
            .putFields("id", LongValue.strict())
            .build();

    private static final ObjectValue QUOTE_OBJECT = ObjectValue.builder()
            .putFields("symbol", SYMBOL_INFO)
            .putFields("quote", ObjectValue.builder()
                    .putFields("bid", DoubleValue.standard())
                    .putFields("ask", DoubleValue.standard())
                    .build())
            .build();

    private static final ObjectValue TRADE_OBJECT = ObjectValue.builder()
            .putFields("symbol", SYMBOL_INFO)
            .putFields("price", DoubleValue.standard())
            .putFields("size", DoubleValue.standard())
            .build();

    private static final TypedObjectValue QUOTE_OR_TRADE_OBJECT =
            TypedObjectValue.builder(new LinkedHashMap<>() {
                {
                    put("quote", QUOTE_OBJECT);
                    put("trade", TRADE_OBJECT);
                }
            })
                    .typeFieldName("type")
                    .onNull("<null>")
                    .onMissing("<missing>")
                    .build();


    @Test
    void provider() {
        final JacksonProvider provider = JacksonProvider.of(QUOTE_OR_TRADE_OBJECT);
        assertThat(provider.outputTypes()).containsExactly(Type.stringType(), Type.stringType(), Type.longType(),
                Type.doubleType(), Type.doubleType(), Type.doubleType(), Type.doubleType());
        assertThat(provider.stringProcessor().outputTypes()).containsExactly(Type.stringType(), Type.stringType(),
                Type.longType(), Type.doubleType(), Type.doubleType(), Type.doubleType(), Type.doubleType());
    }

    @Test
    void typeDiscriminationQuoteTrade() throws IOException {
        parse(QUOTE_OR_TRADE_OBJECT, List.of(
                "{\"type\": \"quote\", \"symbol\": {\"name\": \"foo\", \"id\": 42}, \"quote\":{\"bid\": 1.01, \"ask\": 1.05}}",
                "{\"type\": \"trade\", \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}",
                "{\"type\": \"other\"}",
                "{\"type\": \"other_mimic_trade\", \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}"),
                ObjectChunk.chunkWrap(new String[] {"quote", "trade", "other", "other_mimic_trade"}), // type
                ObjectChunk.chunkWrap(new String[] {"foo", "bar", null, null}), // symbol/symbol
                LongChunk.chunkWrap(new long[] {42, 43, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG}), // symbol/symbol_id
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // trade: price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE})); // trade: size
    }

    @Test
    void standardNull() throws IOException {
        parse(QUOTE_OR_TRADE_OBJECT, "null",
                ObjectChunk.chunkWrap(new String[] {"<null>"}), // type
                ObjectChunk.chunkWrap(new String[] {null}), // symbol/symbol
                LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}), // symbol/symbol_id
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}), // quote: quote/bid
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}), // quote: quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}), // trade: price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE})); // trade: size
    }

    @Test
    void standardMissing() throws IOException {
        parse(QUOTE_OR_TRADE_OBJECT, "",
                ObjectChunk.chunkWrap(new String[] {"<missing>"}), // type
                ObjectChunk.chunkWrap(new String[] {null}), // symbol/symbol
                LongChunk.chunkWrap(new long[] {QueryConstants.NULL_LONG}), // symbol/symbol_id
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}), // quote: quote/bid
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}), // quote: quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE}), // trade: price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE})); // trade: size
    }

    @Test
    void unexpectedFirstField() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "{\"foo\": 42}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Expected the first field to be 'type', is 'foo'");
        }
    }

    @Test
    void standardEmptyObject() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "{}");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Expected a non-empty object");
        }
    }

    @Test
    void standardInt() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Number int not expected");
        }
    }

    @Test
    void standardDecimal() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "42.42");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Decimal not expected");
        }
    }

    @Test
    void standardString() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "\"hello\"");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("String not expected");
        }
    }

    @Test
    void standardTrue() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "true");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardFalse() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "false");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Bool not expected");
        }
    }

    @Test
    void standardArray() {
        try {
            process(QUOTE_OR_TRADE_OBJECT, "[]");
            failBecauseExceptionWasNotThrown(IOException.class);
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("Array not expected");
        }
    }

    // Disabled; this may be a feature we want to spec out in the future
    @Test
    @Disabled
    void intAsDiscriminator() throws IOException {
        // Note: need to pass
        final TypedObjectValue tov = TypedObjectValue.builder(new LinkedHashMap<>() {
            {
                put(1, QUOTE_OBJECT);
                put(2, TRADE_OBJECT);
            }
        }).typeField(ObjectField.of("id", IntValue.standard())).build();

        parse(tov, List.of(
                "{\"id\": 1, \"symbol\": {\"name\": \"foo\", \"id\": 42}, \"quote\":{\"bid\": 1.01, \"ask\": 1.05}}",
                "{\"id\": 2, \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}",
                "{\"id\": 3}",
                "{\"id\": 4, \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}"),
                IntChunk.chunkWrap(new int[] {1, 2, 3, 4}), // id
                ObjectChunk.chunkWrap(new String[] {"foo", "bar", null, null}), // symbol/symbol
                LongChunk.chunkWrap(new long[] {42, 43, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG}), // symbol/symbol_id
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // trade: price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE})); // trade: size

    }

    @Test
    void intStringAsDiscriminator() throws IOException {
        final TypedObjectValue tov = TypedObjectValue.builder(new LinkedHashMap<>() {
            {
                put("1", QUOTE_OBJECT);
                put("2", TRADE_OBJECT);
            }
        }).typeFieldName("id").build();

        parse(tov, List.of(
                "{\"id\": 1, \"symbol\": {\"name\": \"foo\", \"id\": 42}, \"quote\":{\"bid\": 1.01, \"ask\": 1.05}}",
                "{\"id\": 2, \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}",
                "{\"id\": 3}",
                "{\"id\": 4, \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}"),
                ObjectChunk.chunkWrap(new String[] {"1", "2", "3", "4"}), // id
                ObjectChunk.chunkWrap(new String[] {"foo", "bar", null, null}), // symbol/symbol
                LongChunk.chunkWrap(new long[] {42, 43, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG}), // symbol/symbol_id
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // trade: price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE})); // trade: size
    }

    @Test
    void caseSensitiveDiscriminator() throws IOException {
        final TypedObjectValue tov = TypedObjectValue.builder(new LinkedHashMap<>() {
            {
                put("q", QUOTE_OBJECT);
                put("Q", TRADE_OBJECT);
            }
        }).typeFieldName("type").build();
        parse(tov, List.of(
                "{\"type\": \"q\", \"symbol\": {\"name\": \"foo\", \"id\": 42}, \"quote\":{\"bid\": 1.01, \"ask\": 1.05}}",
                "{\"type\": \"Q\", \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}",
                "{\"type\": \"other\"}",
                "{\"type\": \"other_mimic_trade\", \"symbol\": {\"name\": \"bar\", \"id\": 43}, \"price\": 42.42, \"size\": 123}"),
                ObjectChunk.chunkWrap(new String[] {"q", "Q", "other", "other_mimic_trade"}), // type
                ObjectChunk.chunkWrap(new String[] {"foo", "bar", null, null}), // symbol/symbol
                LongChunk.chunkWrap(new long[] {42, 43, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG}), // symbol/symbol_id
                DoubleChunk.chunkWrap(new double[] {1.01, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/bid
                DoubleChunk.chunkWrap(new double[] {1.05, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // quote: quote/ask
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 42.42, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE}), // trade: price
                DoubleChunk.chunkWrap(new double[] {QueryConstants.NULL_DOUBLE, 123, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE})); // trade: size
    }

    @Test
    void columnNames() {
        assertThat(JacksonProvider.of(QUOTE_OR_TRADE_OBJECT).named(Type.stringType()).names()).containsExactly(
                "type",
                "symbol_name",
                "symbol_id",
                "quote_quote_bid",
                "quote_quote_ask",
                "trade_price",
                "trade_size");
    }
}
