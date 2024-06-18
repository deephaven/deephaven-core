//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import ch.randelshofer.fastdoubleparser.JavaBigDecimalParser;
import ch.randelshofer.fastdoubleparser.JavaBigIntegerParser;
import ch.randelshofer.fastdoubleparser.JavaDoubleParser;
import ch.randelshofer.fastdoubleparser.JavaFloatParser;
import com.fasterxml.jackson.core.JsonParser;
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

    // ---------------------------------------------------------------------------

    static byte parseIntAsByte(JsonParser parser) throws IOException {
        return parser.getByteValue();
    }

    static byte parseDecimalAsByte(JsonParser parser) throws IOException {
        return parser.getByteValue();
    }

    static byte parseDecimalStringAsByte(JsonParser parser) throws IOException {
        // parse as float then cast to byte; no loss of whole number part (32 bit -> 8 bit) if in range
        return (byte) parseStringAsFloat(parser);
    }

    static byte parseStringAsByte(JsonParser parser) throws IOException {
        return (byte) parseStringAsInt(parser);
    }

    // ---------------------------------------------------------------------------


    static short parseIntAsShort(JsonParser parser) throws IOException {
        return parser.getShortValue();
    }

    static short parseDecimalAsShort(JsonParser parser) throws IOException {
        // parse as double then cast to short; no loss of whole number part (64 bit -> 16 bit)
        return parser.getShortValue();
    }

    static short parseDecimalStringAsShort(JsonParser parser) throws IOException {
        // parse as float then cast to short; no loss of whole number part (32 bit -> 16 bit) if in range
        return (short) parseStringAsFloat(parser);
    }

    static short parseStringAsShort(JsonParser parser) throws IOException {
        return (short) parseStringAsInt(parser);
    }

    // ---------------------------------------------------------------------------

    static int parseIntAsInt(JsonParser parser) throws IOException {
        return parser.getIntValue();
    }

    static int parseDecimalAsInt(JsonParser parser) throws IOException {
        // parse as double then cast to int; no loss of whole number part (64 bit -> 32 bit)
        return parser.getIntValue();
    }

    // ---------------------------------------------------------------------------

    static long parseIntAsLong(JsonParser parser) throws IOException {
        return parser.getLongValue();
    }

    static long parseDecimalAsLong(JsonParser parser) throws IOException {
        // Parsing as double then casting to long will lose precision.
        // (long)9223372036854775806.0 == 9223372036854775807
        return parser.getDecimalValue().longValue();
    }

    static long parseDecimalAsScaledLong(JsonParser parser, int n) throws IOException {
        return parser.getDecimalValue().scaleByPowerOfTen(n).longValue();
    }

    // ---------------------------------------------------------------------------

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

    // ---------------------------------------------------------------------------

    static float parseNumberAsFloat(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.getFloatValue();
    }

    static double parseNumberAsDouble(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.getDoubleValue();
    }

    // ---------------------------------------------------------------------------

    static int parseStringAsInt(JsonParser parser) throws IOException {
        if (parser.hasTextCharacters()) {
            final int len = parser.getTextLength();
            final CharSequence cs = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), len);
            return Integer.parseInt(cs, 0, len, 10);
        } else {
            return Integer.parseInt(parser.getText());
        }
    }

    static int parseDecimalStringAsInt(JsonParser parser) throws IOException {
        // parse as double then cast to int; no loss of whole number part (64 bit -> 32 bit)
        return (int) parseStringAsDouble(parser);
    }

    static long parseStringAsLong(JsonParser parser) throws IOException {
        if (parser.hasTextCharacters()) {
            final int len = parser.getTextLength();
            final CharSequence cs = CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), len);
            return Long.parseLong(cs, 0, len, 10);
        } else {
            return Long.parseLong(parser.getText());
        }
    }

    static long parseDecimalStringAsLong(JsonParser parser) throws IOException {
        // To ensure 64-bit in cases where the string is a decimal, we need BigDecimal
        return parseStringAsBigDecimal(parser).longValue();
    }

    static long parseDecimalStringAsScaledLong(JsonParser parser, int n) throws IOException {
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

    // ---------------------------------------------------------------------------

    static BigInteger parseIntAsBigInteger(JsonParser parser) throws IOException {
        return parser.getBigIntegerValue();
    }

    static BigInteger parseDecimalAsBigInteger(JsonParser parser) throws IOException {
        return parser.getBigIntegerValue();
    }

    static BigInteger parseStringAsBigInteger(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                ? parseStringAsBigIntegerFast(parser)
                : new BigInteger(parser.getText());
    }

    static BigInteger parseDecimalStringAsBigInteger(JsonParser parser) throws IOException {
        return parseStringAsBigDecimal(parser).toBigInteger();
    }

    // ---------------------------------------------------------------------------

    static BigDecimal parseDecimalAsBigDecimal(JsonParser parser) throws IOException {
        return parser.getDecimalValue();
    }

    static BigDecimal parseStringAsBigDecimal(JsonParser parser) throws IOException {
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return parser.isEnabled(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                ? parseStringAsBigDecimalFast(parser)
                : new BigDecimal(parser.getText());
    }

    // ---------------------------------------------------------------------------

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
