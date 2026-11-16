//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// The test-side encoder has to produce what the client's decoder expects, or every BarrageProcessor test is
/// testing the wrong thing.
/// </summary>
public class RowSequenceEncoderTest {
  [Test]
  public async Task RoundTripsThroughTheDecoder() {
    var builder = new RowSequenceBuilder();
    builder.AddInterval(Interval.Of(1, 4));
    builder.Add(10);
    builder.AddInterval(Interval.Of(99, 1000));
    builder.Add(1UL << 40);
    builder.AddInterval(Interval.Of(1UL << 41, (1UL << 41) + 3));

    var cases = new[] {
      RowSequence.CreateEmpty(),
      RowSequence.CreateSequential(Interval.OfSingleton(0)),
      RowSequence.CreateSequential(Interval.OfSingleton(7)),
      RowSequence.CreateSequential(Interval.Of(0, 2)),
      RowSequence.CreateSequential(Interval.Of(0, 400)),
      builder.Build(),
    };

    foreach (var rows in cases) {
      var encoded = RowSequenceEncoder.Encode(rows);
      var decoded = RowSequenceDecoder.ReadExternalCompressedDelta(new DataInput(encoded));
      await Assert.That(BarrageProcessorDriver.Render(decoded)).IsEqualTo(BarrageProcessorDriver.Render(rows));
    }
  }
}
