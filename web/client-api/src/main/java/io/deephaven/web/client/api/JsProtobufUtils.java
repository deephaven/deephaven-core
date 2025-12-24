//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.Uint8Array;
import elemental2.dom.TextEncoder;

/**
 * Utility methods for working with protobuf messages in the client-side JavaScript environment.
 */
public class JsProtobufUtils {

    private JsProtobufUtils() {
        // Utility class, no instantiation
    }

    /**
     * Wraps a protobuf message in a google.protobuf.Any message.
     * <p>
     * The google.protobuf.Any message has two fields:
     * <ul>
     *   <li>Field 1: type_url (string) - identifies the type of message contained</li>
     *   <li>Field 2: value (bytes) - the actual serialized message</li>
     * </ul>
     * <p>
     * This method manually encodes the Any message in protobuf binary format since the client-side
     * JavaScript protobuf library doesn't provide Any.pack() like the server-side Java library does.
     *
     * @param typeUrl the type URL for the message (e.g., "type.googleapis.com/package.MessageName")
     * @param messageBytes the serialized protobuf message bytes
     * @return the serialized Any message containing the wrapped message
     */
    public static Uint8Array wrapInAny(String typeUrl, Uint8Array messageBytes) {
        // Protobuf tag constants for google.protobuf.Any message fields
        // Tag format: (field_number << 3) | wire_type
        // wire_type=2 means length-delimited (for strings/bytes)
        final int TYPE_URL_TAG = 10;  // (1 << 3) | 2 = field 1, wire type 2
        final int VALUE_TAG = 18;     // (2 << 3) | 2 = field 2, wire type 2

        // Encode the type_url string to UTF-8 bytes
        TextEncoder textEncoder = new TextEncoder();
        Uint8Array typeUrlBytes = textEncoder.encode(typeUrl);

        // Calculate sizes for protobuf binary encoding
        int typeUrlFieldSize = calculateFieldSize(TYPE_URL_TAG, typeUrlBytes.length);
        int valueFieldSize = calculateFieldSize(VALUE_TAG, messageBytes.length);

        // Allocate buffer for the complete Any message
        int totalSize = typeUrlFieldSize + valueFieldSize;
        Uint8Array result = new Uint8Array(totalSize);
        int pos = 0;

        // Write field 1 (type_url) in protobuf binary format
        pos = writeField(result, pos, TYPE_URL_TAG, typeUrlBytes);

        // Write field 2 (value) in protobuf binary format
        writeField(result, pos, VALUE_TAG, messageBytes);

        return result;
    }

    /**
     * Calculates the total size needed for a protobuf length-delimited field.
     * <p>
     * A length-delimited field consists of:
     * <ul>
     *   <li>Tag (field number + wire type) encoded as a varint</li>
     *   <li>Length of the data encoded as a varint</li>
     *   <li>The actual data bytes</li>
     * </ul>
     *
     * @param tag the protobuf field tag (field number << 3 | wire type)
     * @param dataLength the length of the data in bytes
     * @return the total number of bytes needed for this field
     */
    private static int calculateFieldSize(int tag, int dataLength) {
        return sizeOfVarint(tag) + sizeOfVarint(dataLength) + dataLength;
    }

    /**
     * Calculates how many bytes a varint encoding will require for the given value.
     * <p>
     * Protobuf uses varint encoding where each byte stores 7 bits of data (the 8th bit is
     * a continuation flag). This means:
     * <ul>
     *   <li>1 byte: 0 to 127 (2^7 - 1)</li>
     *   <li>2 bytes: 128 to 16,383 (2^14 - 1)</li>
     *   <li>3 bytes: 16,384 to 2,097,151 (2^21 - 1)</li>
     *   <li>4 bytes: 2,097,152 to 268,435,455 (2^28 - 1)</li>
     *   <li>5 bytes: 268,435,456 to 4,294,967,295 (2^35 - 1, max unsigned 32-bit)</li>
     *   <li>10 bytes: negative numbers (due to sign extension)</li>
     * </ul>
     *
     * @param value the integer value to encode
     * @return the number of bytes required to encode the value as a varint
     */
    private static int sizeOfVarint(int value) {
        if (value < 0)
            return 10;          // Negative numbers use sign extension, always 10 bytes
        if (value < 128)        // 2^7
            return 1;
        if (value < 16384)      // 2^14
            return 2;
        if (value < 2097152)    // 2^21
            return 3;
        if (value < 268435456)  // 2^28
            return 4;
        return 5;               // 2^35 (max for positive 32-bit int)
    }

    /**
     * Writes a complete protobuf length-delimited field to the buffer.
     * <p>
     * A length-delimited field consists of:
     * <ul>
     *   <li>Tag (field number + wire type) encoded as a varint</li>
     *   <li>Length of the data encoded as a varint</li>
     *   <li>The actual data bytes</li>
     * </ul>
     *
     * @param buffer the buffer to write to
     * @param pos the starting position in the buffer
     * @param tag the protobuf field tag
     * @param data the data bytes to write
     * @return the new position after writing the complete field
     */
    private static int writeField(Uint8Array buffer, int pos, int tag, Uint8Array data) {
        // Write tag and length
        pos = writeVarint(buffer, pos, tag);
        pos = writeVarint(buffer, pos, data.length);
        // Write data bytes
        for (int i = 0; i < data.length; i++) {
            buffer.setAt(pos++, data.getAt(i));
        }
        return pos;
    }

    /**
     * Writes a value to the buffer as a protobuf varint (variable-length integer).
     * <p>
     * Varint encoding works by:
     * <ol>
     *   <li>Taking the lowest 7 bits of the value</li>
     *   <li>Setting the 8th bit to 1 if more bytes follow (continuation flag)</li>
     *   <li>Writing the byte to the buffer</li>
     *   <li>Shifting the value right by 7 bits</li>
     *   <li>Repeating until the value is less than 128</li>
     *   <li>Writing the final byte without the continuation flag (8th bit = 0)</li>
     * </ol>
     * <p>
     * Example: encoding 300
     * <ul>
     *   <li>300 in binary: 100101100</li>
     *   <li>First byte: (300 & 0x7F) | 0x80 = 0b00101100 | 0b10000000 = 172 (0xAC)</li>
     *   <li>Shift: 300 >>> 7 = 2</li>
     *   <li>Second byte: 2 (no continuation flag)</li>
     *   <li>Result: [172, 2]</li>
     * </ul>
     *
     * @param buffer the buffer to write to
     * @param pos the starting position in the buffer
     * @param value the value to encode
     * @return the new position after writing
     */
    private static int writeVarint(Uint8Array buffer, int pos, int value) {
        while (value >= 128) {
            // Extract lowest 7 bits and set continuation flag (8th bit = 1)
            buffer.setAt(pos++, (double) ((value & 0x7F) | 0x80));
            // Shift right by 7 to process next chunk
            value >>>= 7;  // Unsigned right shift to handle large positive values
        }
        // Write final byte (no continuation flag, 8th bit = 0)
        buffer.setAt(pos++, (double) value);
        return pos;
    }
}

