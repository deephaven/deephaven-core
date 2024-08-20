//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.base.verify.Assert;
import io.deephaven.io.util.NullOutputStream;
import org.junit.Test;

import java.io.IOException;

public class BarrageStreamGeneratorTest {

    @Test
    public void testDrainableStreamIsEmptied() throws IOException {
        final int length = 512;
        final DrainableByteArrayInputStream inputStream =
                new DrainableByteArrayInputStream(new byte[length * 2], length / 2, length);

        int bytesRead = inputStream.drainTo(new NullOutputStream());

        Assert.eq(bytesRead, "bytesRead", length, "length");
        Assert.eq(inputStream.available(), "inputStream.available()", 0);
    }

    @Test
    public void testConsecutiveDrainableStreamIsEmptied() throws IOException {
        final int length = 512;
        final DrainableByteArrayInputStream in1 =
                new DrainableByteArrayInputStream(new byte[length * 2], length / 2, length);
        final DrainableByteArrayInputStream in2 =
                new DrainableByteArrayInputStream(new byte[length * 2], length / 2, length);
        final ConsecutiveDrainableStreams inputStream = new ConsecutiveDrainableStreams(in1, in2);

        int bytesRead = inputStream.drainTo(new NullOutputStream());

        Assert.eq(bytesRead, "bytesRead", length * 2, "length * 2");
        Assert.eq(inputStream.available(), "inputStream.available()", 0);
    }
}
