//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

import static io.deephaven.json.jackson.Parsing.assertNextToken;
import static io.deephaven.json.jackson.Parsing.assertNoCurrentToken;

interface ValueProcessor extends ContextAware {

    static void processFullJson(JsonParser parser, ValueProcessor processor) throws IOException {
        assertNoCurrentToken(parser);
        final JsonToken startToken = parser.nextToken();
        if (startToken == null) {
            processor.processMissing(parser);
            return;
        }
        processor.processCurrentValue(parser);
        // note: AnyOptions impl which is based on com.fasterxml.jackson.core.JsonParser.readValueAsTree
        // clears out the token, so we can't necessarily check it.
        // parser.getLastClearedToken()
        // assertCurrentToken(parser, endToken(startToken));
        assertNextToken(parser, null);
    }

    // semantically _similar_ to
    // com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
    // com.fasterxml.jackson.databind.DeserializationContext),
    // but not functional (want to destructure efficiently)

    /**
     * Called when the JSON value is present; the current token should be one of {@link JsonToken#START_OBJECT},
     * {@link JsonToken#START_ARRAY}, {@link JsonToken#VALUE_STRING}, {@link JsonToken#VALUE_NUMBER_INT},
     * {@link JsonToken#VALUE_NUMBER_FLOAT}, {@link JsonToken#VALUE_TRUE}, {@link JsonToken#VALUE_FALSE}, or
     * {@link JsonToken#VALUE_NULL}. When the current token is {@link JsonToken#START_OBJECT}, the implementation must
     * end with the corresponding {@link JsonToken#END_OBJECT} as the current token; when the current token is
     * {@link JsonToken#START_ARRAY}, the implementation must end with the corresponding {@link JsonToken#END_ARRAY} as
     * the current token; otherwise, the implementation must not change the current token.
     *
     * @param parser the parser
     * @throws IOException if an IOException occurs
     */
    void processCurrentValue(JsonParser parser) throws IOException;

    /**
     * Called when the JSON value is missing; the current token may or may <b>not</b> be {@code null}. For example, if a
     * field is missing from a JSON object, it's likely that missing values will be notified when the current token is
     * {@link JsonToken#END_OBJECT}. Implementations must not modify the state of {@code parser}.
     *
     * @param parser the parser
     * @throws IOException if an IOException occurs
     */
    void processMissing(JsonParser parser) throws IOException;
}
