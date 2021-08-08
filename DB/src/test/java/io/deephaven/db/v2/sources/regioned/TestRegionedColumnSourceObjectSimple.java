/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for {@link RegionedColumnSourceObject} with simple strings.
 */
public class TestRegionedColumnSourceObjectSimple extends TstRegionedColumnSourceObject<String> {

    private static final String REALLY_LONG = IntStream.range(0, 1000).mapToObj(Integer::toString).collect(Collectors.joining());

    private static final Value<String>[] REUSABLE_VALUES;
    static {
        long length = 0L;
        //noinspection unchecked,UnusedAssignment
        REUSABLE_VALUES = new Value[]{
                new Value("one", objectToBytes("one"), length += "one".length()),
                new Value("", new byte[]{ '\0' }, length += 1),
                new Value("three", objectToBytes("three"), length += "three".length()),
                new Value("scooby doo", objectToBytes("scooby doo"), length += "scooby doo".length()),
                new Value("five", objectToBytes("five"), length += "five".length()),
                new Value("hello", objectToBytes("hello"), length += "hello".length()),
                new Value("world", objectToBytes("world"), length += "world".length()),
                new Value("nineteen", objectToBytes("nineteen"), length += "nineteen".length()),
                new Value("one million dollars", objectToBytes("one million dollars"), length += "one million dollars".length()),
                new Value(REALLY_LONG, objectToBytes(REALLY_LONG), length += REALLY_LONG.length())
        };
    }

    public TestRegionedColumnSourceObjectSimple() {
        super(REUSABLE_VALUES);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //noinspection unchecked
        SUT = new RegionedColumnSourceObject.AsValues<>(String.class);
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
