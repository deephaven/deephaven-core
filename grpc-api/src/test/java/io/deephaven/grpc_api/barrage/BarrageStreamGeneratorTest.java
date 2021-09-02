package io.deephaven.grpc_api.barrage;

import io.deephaven.base.verify.Assert;
import io.deephaven.io.util.NullOutputStream;
import org.junit.Test;

import java.io.IOException;

public class BarrageStreamGeneratorTest {

    @Test
    public void testDrainableStreamIsEmptied() throws IOException {
        final int length = 512;
        final BarrageStreamGenerator.DrainableByteArrayInputStream inputStream =
                new BarrageStreamGenerator.DrainableByteArrayInputStream(new byte[length * 2], length / 2, length);

        int bytesRead = inputStream.drainTo(new NullOutputStream());

        Assert.eq(bytesRead, "bytesRead", length, "length");
        Assert.eq(inputStream.available(), "inputStream.available()", 0);
    }

    @Test
    public void testConsecutiveDrainableStreamIsEmptied() throws IOException {
        final int length = 512;
        final BarrageStreamGenerator.DrainableByteArrayInputStream in1 =
                new BarrageStreamGenerator.DrainableByteArrayInputStream(new byte[length * 2], length / 2, length);
        final BarrageStreamGenerator.DrainableByteArrayInputStream in2 =
                new BarrageStreamGenerator.DrainableByteArrayInputStream(new byte[length * 2], length / 2, length);
        final BarrageStreamGenerator.ConsecutiveDrainableStreams inputStream =
                new BarrageStreamGenerator.ConsecutiveDrainableStreams(in1, in2);

        int bytesRead = inputStream.drainTo(new NullOutputStream());

        Assert.eq(bytesRead, "bytesRead", length * 2, "length * 2");
        Assert.eq(inputStream.available(), "inputStream.available()", 0);
    }
}
