/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.string.EncodingInfo;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.codec.ObjectCodec;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * Tests for {@link RegionedColumnSourceObject} with fixed length codec.
 */
@SuppressWarnings("Convert2Diamond")
public class TestRegionedColumnSourceObjectFixed extends TstRegionedColumnSourceObject<String> {

    private static final ObjectCodec<String> FIXED = new ObjectCodec<String>() {
        @NotNull
        @Override
        public byte[] encode(@Nullable String input) {
            assert input != null;
            Assert.eq(input.length(), "input.length()", 4);
            return EncodingInfo.US_ASCII.encode(input);
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public int getPrecision() {
            return 0;
        }

        @Override
        public int getScale() {
            return 0;
        }

        @Override
        public String decode(@NotNull byte[] input, int offset, int length) {
            return EncodingInfo.US_ASCII.decode(input, offset, length);
        }

        @Override
        public int expectedObjectWidth() {
            return 4;
        }
    };


    private static final Value<String>[] REUSABLE_VALUES;
    static {
        final MutableLong length = new MutableLong(0);
        //noinspection unchecked
        REUSABLE_VALUES = Stream.of("1234", "0000", "abcd", "ABCD", "love", "hate", "nine", "nein", "wxyz", "WXYZ").map(
                s -> { length.add(s.length()); return new Value<>(s, objectToBytes(s), length.longValue()); }
        ).toArray(Value[]::new);
    }

    public TestRegionedColumnSourceObjectFixed() {
        super(REUSABLE_VALUES);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SUT = new RegionedColumnSourceObject.AsValues<String>(String.class);
        TestCase.assertEquals(String.class, SUT.getType());
    }

    private static byte[] objectToBytes(String inObject) {
        try {
            ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
            for(int ci = 0; ci < inObject.length(); ++ci) {
                byteOutStream.write((byte)inObject.charAt(ci));
            }
            byteOutStream.flush();
            return byteOutStream.toByteArray();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
