package io.deephaven.parquet.base.util;


import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 */
public class RunLengthBitPackingHybridBufferDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(RunLengthBitPackingHybridBufferDecoder.class);
    private int rangeCount;
    private final int maxLevel;
    private int rleCandidateValue;

    private enum MODE {
        RLE, PACKED
    }

    private final int bitWidth;
    private final BytePacker packer;
    private final ByteBuffer in;

    private MODE mode;
    private int currentCount;
    private int currentValue;
    private int[] currentBuffer;

    public RunLengthBitPackingHybridBufferDecoder(int maxLevel, ByteBuffer in) {
        this.bitWidth = BytesUtils.getWidthFromMaxInt(maxLevel);
        this.maxLevel = maxLevel;
        LOG.debug("decoding bitWidth {}", bitWidth);
        Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
        this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        this.in = in;
    }

    public int readInt() throws IOException {
        if (currentCount == 0) {
            readNext();
        }
        --currentCount;
        int result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case PACKED:
                result = currentBuffer[currentBuffer.length - 1 - currentCount];
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
        return result;
    }

    public void readNextRange() throws IOException {

        if (currentCount == 0) {
            readNext();
        }
        rangeCount = 1;
        currentCount--;
        switch (mode) {
            case RLE:
                currentValue = rleCandidateValue;
                break;
            case PACKED:
                currentValue = currentBuffer[currentBuffer.length - 1 - currentCount];
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }

        if (currentCount == 0) {
            if (!hasNext()) {
                return;
            }
            readNext();
        }
        while (true) {
            switch (mode) {
                case RLE:
                    if (rleCandidateValue != currentValue) {
                        return;
                    }
                    rangeCount += currentCount;
                    currentCount = 0;
                    break;
                case PACKED:
                    while (currentCount > 0 && (currentBuffer[currentBuffer.length - currentCount] == currentValue)) {
                        currentCount--;
                        rangeCount++;
                    }
                    if (currentCount > 0) {
                        return;
                    }
                    break;
                default:
                    throw new ParquetDecodingException("not a valid mode " + mode);
            }
            if (!hasNext()) {
                return;
            }
            readNext();
        }
    }

    public boolean hasNext() {
        return in.hasRemaining() || currentCount != 0;
    }

    public int currentRangeCount() {
        return rangeCount;
    }


    public boolean isNullRange() {
        return currentValue < maxLevel;
    }

    public int currentValue() {
        return currentValue;
    }

    private void readNext() throws IOException {
        final int header = Helpers.readUnsignedVarInt(in);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                currentCount = header >>> 1;
                LOG.debug("reading {} values RLE", currentCount);
                rleCandidateValue = Helpers.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                break;
            case PACKED:
                int numGroups = header >>> 1;
                currentCount = numGroups * 8;
                LOG.debug("reading {} values BIT PACKED", currentCount);
                currentBuffer = new int[currentCount]; // TODO: reuse a buffer
                // At the end of the file RLE data though, there might not be that many bytes left.
                int bytesToRead = (int) Math.ceil(currentCount * bitWidth / 8.0);
                bytesToRead = Math.min(bytesToRead, in.remaining());
                int newPos = in.position() + bytesToRead;
                for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex +=
                        bitWidth) {
                    packer.unpack8Values(in, byteIndex + in.position(), currentBuffer, valueIndex);
                }
                in.position(newPos);
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
    }
}
