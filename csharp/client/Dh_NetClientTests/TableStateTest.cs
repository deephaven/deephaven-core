//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Apache.Arrow;
using Apache.Arrow.Types;
using Deephaven.Dh_NetClient;
using static Deephaven.Dh_NetClientTests.BarrageProcessorDriver;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// TableState holds only the rows the subscription has received data for. Erase is handed the server's removed set,
/// which can name rows it does not hold, and must erase just the ones it does, reporting their positions.
/// </summary>
public class TableStateTest {
  private static readonly Schema OneLongColumn = new Schema.Builder()
    .Field(f => f.Name("Key").DataType(Int64Type.Default))
    .Build();

  private static RowSequence Range(UInt64 begin, UInt64 end) => RowSequence.CreateSequential(Interval.Of(begin, end));

  private static RowSequence Keys(params UInt64[] keys) {
    var builder = new RowSequenceBuilder();
    foreach (var key in keys) {
      builder.Add(key);
    }
    return builder.Build();
  }

  /// <summary>
  /// A TableState holding exactly 'keys', each row's Key column being its key.
  /// </summary>
  private static TableState Holding(RowSequence keys) {
    var state = new TableState(OneLongColumn);
    var indexSpace = state.AddKeys(keys);
    var column = BarrageMessages.LongColumn(keys.Elements.Select(k => (Int64)k).ToArray());
    state.AddData([new SourceAndRange(column, Interval.OfStartAndSize(0, keys.Count))], indexSpace);
    return state;
  }

  private static async Task AssertHoldsKeys(TableState state, params Int64[] expected) {
    var actual = Column(state.Snapshot(), 0);
    await Assert.That(string.Join(",", actual)).IsEqualTo(string.Join(",", expected));
  }

  [Test]
  public async Task EraseSkipsKeysItDoesNotHold() {
    var state = Holding(Keys(1, 2, 3, 10, 11, 12, 100));

    // Of 0..4, only 1, 2 and 3 are held; they are the first three rows.
    var erased = state.Erase(Range(0, 5));
    await Assert.That(Render(erased)).IsEqualTo("[0,3)");
    await AssertHoldsKeys(state, 10, 11, 12, 100);

    // Of 5..199, only 10, 11, 12 and 100 are held, and they are now rows 0..3.
    erased = state.Erase(Range(5, 200));
    await Assert.That(Render(erased)).IsEqualTo("[0,4)");
    await AssertHoldsKeys(state);
  }

  [Test]
  public async Task EraseOfKeysItDoesNotHoldChangesNothing() {
    var state = Holding(Keys(1, 2, 3, 10, 11, 12, 100));
    var erased = state.Erase(Range(50, 60));
    await Assert.That(erased.IsEmpty).IsTrue();
    await AssertHoldsKeys(state, 1, 2, 3, 10, 11, 12, 100);
  }

  [Test]
  public async Task EraseMapsEachIntervalToTheRanksOfItsHeldKeys() {
    var state = Holding(Keys(1, 2, 3, 10, 11, 12, 100));
    var builder = new RowSequenceBuilder();
    builder.AddInterval(Interval.Of(0, 5));     // holds 1, 2, 3: rows 0..2
    builder.AddInterval(Interval.Of(11, 101));  // holds 11, 12, 100: rows 4..6
    var erased = state.Erase(builder.Build());
    await Assert.That(Render(erased)).IsEqualTo("[0,3) [4,7)");
    await AssertHoldsKeys(state, 10);
  }

  [Test]
  public async Task PresentKeysReturnsTheHeldSubset() {
    var state = Holding(Keys(1, 2, 3, 10, 11, 12, 100));
    await Assert.That(Render(state.PresentKeys(Range(0, 5)))).IsEqualTo("[1,4)");
    await Assert.That(Render(state.PresentKeys(Range(50, 60)))).IsEqualTo("");
    var builder = new RowSequenceBuilder();
    builder.AddInterval(Interval.Of(0, 5));
    builder.AddInterval(Interval.Of(11, 101));
    await Assert.That(Render(state.PresentKeys(builder.Build()))).IsEqualTo("[1,4) [11,13) [100,101)");
  }

  /// <summary>
  /// The second erasure's positions count only the rows that survived the first, so each has to move past the
  /// rows the first one took out ahead of it.
  /// </summary>
  [Test]
  public async Task CombineErasuresLiftsTheSecondPastTheFirst() {
    // Rows 0 and 1 go first; the next two rows of what remains were rows 2 and 3 originally.
    await Assert.That(Render(TableState.CombineErasures(Range(0, 2), Range(0, 2)))).IsEqualTo("[0,4)");
    // Rows 5..7 go first; row 396 of what remains was row 399.
    await Assert.That(Render(TableState.CombineErasures(Range(5, 8), Range(396, 397)))).IsEqualTo("[5,8) [399,400)");
    // Rows 3 and 5 go first; rows 3 and 4 of what remains were rows 4 and 6, and the lift grows between them.
    await Assert.That(Render(TableState.CombineErasures(Keys(3, 5), Range(3, 5)))).IsEqualTo("[3,7)");
    // Either erasure may be empty.
    await Assert.That(Render(TableState.CombineErasures(RowSequence.CreateEmpty(), Range(2, 4)))).IsEqualTo("[2,4)");
    await Assert.That(Render(TableState.CombineErasures(Range(2, 4), RowSequence.CreateEmpty()))).IsEqualTo("[2,4)");
  }
}
