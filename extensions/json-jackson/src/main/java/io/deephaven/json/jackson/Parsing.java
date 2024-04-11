//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import ch.randelshofer.fastdoubleparser.JavaBigDecimalParser;
import ch.randelshofer.fastdoubleparser.JavaBigIntegerParser;
import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import ch.randelshofer.fastdoubleparser.JavaFloatParser;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
import io.deephaven.util.BooleanUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;

final class Parsing {

    static void assertNoCurrentToken(JsonParser parser) {
        if (parser.hasCurrentToken()) {
            throw new IllegalStateException(
                    String.format("Expected no current token. actual=%s", parser.currentToken()));
        }
    }

    static void assertNextToken(JsonParser parser, JsonToken expected) throws IOException {
        final JsonToken actual = parser.nextToken();
        if (actual != expected) {
            throw new IllegalStateException(
                    String.format("Unexpected next token. expected=%s, actual=%s", expected, actual));
        }
    }

    static void assertCurrentToken(JsonParser parser, JsonToken expected) {
        if (!parser.hasToken(expected)) {
            throw new IllegalStateException(
                    String.format("Unexpected current token. expected=%s, actual=%s", expected, parser.currentToken()));
        }
    }

    static CharSequence textAsCharSequence(JsonParser parser) throws IOException {
        return parser.hasTextCharacters()
                ? CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength())
                : parser.getText();
    }

    static class UnexpectedToken extends JsonProcessingException {
        public UnexpectedToken(String msg, JsonLocation loc) {
            super(msg, loc);
        }
    }

    static IOException mismatch(JsonParser parser, Class<?> clazz) {
        final JsonLocation location = parser.currentLocation();
        final String msg = String.format("Unexpected token '%s'", parser.currentToken());
        return new UnexpectedToken(msg, location);
    }

    static IOException mismatchMissing(JsonParser parser, Class<?> clazz) {
        final JsonLocation location = parser.currentLocation();
        return new UnexpectedToken("Unexpected missing token", location);
    }

    static byte parseStringAsByteBool(JsonParser parser, byte onNull) throws IOException {
        final String text = parser.getText().trim();
        if ("true".equalsIgnoreCase(text)) {
            return BooleanUtils.TRUE_BOOLEAN_AS_BYTE;
        }
        if ("false".equalsIgnoreCase(text)) {
            return BooleanUtils.FALSE_BOOLEAN_AS_BYTE;
        }
        if ("null".equalsIgnoreCase(text)) {
            return onNull;
        }
        throw new IOException(String.format("Unexpected string as boolean '%s'", text));
    }

    static Boolean parseStringAsBoolean(JsonParser parser, Boolean onNull) throws IOException {
        final String text = parser.getText().trim();
        if ("true".equalsIgnoreCase(text)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(text)) {
            return Boolean.FALSE;
        }
        if ("null".equalsIgnoreCase(text)) {
            return onNull;
        }
        throw new IOException(String.format("Unexpected string as boolean '%s'", text));
    }

    static char parseStringAsChar(JsonParser parser) throws IOException {
        if (parser.hasTextCharacters()) {
            final int textLength = parser.getTextLength();
            if (textLength != 1) {
                throw new IOException(
                        String.format("Expected char to be string of length 1, is instead %d", textLength));
            }
            return parser.getTextCharacters()[parser.getTextOffset()];
        }
        final String text = parser.getText();
        if (text.length() != 1) {
            throw new IOException(
                    String.format("Expected char to be string of length 1, is instead %d", text.length()));
        }
        return text.charAt(0);
    }

    static byte parseIntAsByte(JsonParser parser) throws IOException {
        return parser.getByteValue();
    }

    static byte parseDecimalAsTruncatedByte(JsonParser parser) throws IOException {
        return parser.getByteValue();
    }

    static byte parseDecimalStringAsTruncatedByte(JsonParser parser) throws IOException {
        // parse as float then cast to byte; no loss of whole number part (32 bit -> 8 bit) if in range
        return (byte) Parsing.parseStringAsFloat(parser);
    }

    static byte parseStringAsByte(JsonParser parser) throws IOException {
        return (byte) parseStringAsInt(parser);
    }

    static short parseIntAsShort(JsonParser parser) throws IOException {
        return parser.getShortValue();
    }

    static short parseDecimalAsTruncatedShort(JsonParser parser) throws IOException {
        return parser.getShortValue();
    }

    static short parseDecimalStringAsTruncatedShort(JsonParser parser) throws IOException {
        // parse as float then cast to short; no loss of whole number part (32 bit -> 16 bit) if in range
        return (short) Parsing.parseStringAsFloat(parser);
    }

    static short parseStringAsShort(JsonParser parser) throws IOException {
        return (short) parseStringAsInt(parser);
    }

    static int parseIntAsInt(JsonParser parser) throws IOException {
        return parser.getIntValue();
    }

    static int parseDecimalAsTruncatedInt(JsonParser parser) throws IOException {
        // parser as double then cast to int; no loss of whole number part (64 bit -> 32 bit)
        return parser.getIntValue();
    }

    static long parseIntAsLong(JsonParser parser) throws IOException {
        return parser.getLongValue();
    }

    static long parseDecimalAsLossyLong(JsonParser parser) throws IOException {
        // parser as double then cast to long; loses info (64 bit -> 64 bit)
        return parser.getLongValue();
    }

    static long parseDecimalAsTruncatedLong(JsonParser parser) throws IOException {
        // io.deephaven.json.InstantNumberOptionsTest.epochNanosDecimal fails
        // return parser.getLongValue();
        return parser.getDecimalValue().longValue();
    }

    static long parseDecimalAsScaledTruncatedLong(JsonParser parser, int n) throws IOException {
        return parser.getDecimalValue().scaleByPowerOfTen(n).longValue();
    }

    static String parseStringAsString(JsonParser parser) throws IOException {
        return parser.getText();
    }

    static String parseIntAsString(JsonParser parser) throws IOException {
        return parser.getText();
    }

    static String parseDecimalAsString(JsonParser parser) throws IOException {
        return parser.getText();
    }

    static String parseBoolAsString(JsonParser parser) throws IOException {
        return parser.getText();
    }

    static BigDecimal parseDecimalAsBigDecimal(JsonParser parser) throws IOException {
        return parser.getDecimalValue();
    }

    static float parseNumberAsFloat(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.getFloatValue();
    }

    static double parseNumberAsDouble(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.getDoubleValue();
    }

    static int parseStringAsInt(JsonParser parser) throws IOException {
        if (parser.hasTextCharacters()) {
            // TODO: potential to write parseInt optimized for char[]
            final int len = parser.getTextLength();
            final CharSequence cs = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), len);
            return Integer.parseInt(cs, 0, len, 10);
        } else {
            return Integer.parseInt(parser.getText());
        }
    }

    static int parseDecimalStringAsTruncatedInt(JsonParser parser) throws IOException {
        // parse as double then cast to int; no loss of whole number part (64 bit -> 32 bit)
        return (int) Parsing.parseStringAsDouble(parser);
    }

    static long parseStringAsLong(JsonParser parser) throws IOException {
        if (parser.hasTextCharacters()) {
            // TODO: potential to write parseInt optimized for char[]
            final int len = parser.getTextLength();
            final CharSequence cs = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), len);
            return Long.parseLong(cs, 0, len, 10);
        } else {
            return Long.parseLong(parser.getText());
        }
    }

    static long parseDecimalStringAsLossyLong(JsonParser parser) throws IOException {
        // parser as double then cast to long; loses info (64 bit -> 64 bit)
        return (long) parseStringAsDouble(parser);
    }

    static long parseDecimalStringAsTruncatedLong(JsonParser parser) throws IOException {
        // To ensure 64-bit in cases where the string is a decimal, we need BigDecimal
        return parseStringAsBigDecimal(parser).longValue();
    }

    static long parseDecimalStringAsScaledTruncatedLong(JsonParser parser, int n) throws IOException {
        // To ensure 64-bit in cases where the string is a decimal, we need BigDecimal
        return parseStringAsBigDecimal(parser).scaleByPowerOfTen(n).longValue();
    }

    static float parseStringAsFloat(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                ? parseStringAsFloatFast(parser)
                : Float.parseFloat(parser.getText());
    }

    static double parseStringAsDouble(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                ? parseStringAsDoubleFast(parser)
                : Double.parseDouble(parser.getText());
    }

    static BigInteger parseStringAsBigInteger(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                ? parseStringAsBigIntegerFast(parser)
                : new BigInteger(parser.getText());
    }

    static BigInteger parseStringAsTruncatedBigInteger(JsonParser parser) throws IOException {
        return parseStringAsBigDecimal(parser).toBigInteger();
    }

    static BigDecimal parseStringAsBigDecimal(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                ? parseStringAsBigDecimalFast(parser)
                : new BigDecimal(parser.getText());
    }

    private static float parseStringAsFloatFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaFloatParser.parseFloat(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaFloatParser.parseFloat(p.getText());
    }

    private static double parseStringAsDoubleFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaDoubleParser.parseDouble(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaDoubleParser.parseDouble(p.getText());
    }

    private static BigInteger parseStringAsBigIntegerFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaBigIntegerParser.parseBigInteger(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaBigIntegerParser.parseBigInteger(p.getText());
    }

    private static BigDecimal parseStringAsBigDecimalFast(JsonParser p) throws IOException {
        return p.hasTextCharacters()
                ? JavaBigDecimalParser.parseBigDecimal(p.getTextCharacters(), p.getTextOffset(), p.getTextLength())
                : JavaBigDecimalParser.parseBigDecimal(p.getText());
    }
}
