//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Deephaven.Dh_NetClient;
using static Deephaven.Dh_NetClientTests.BarrageProcessorDriver;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// Feeds BarrageProcessor the messages a server sends while it delivers the initial snapshot of a large table in
/// rounds, with no server involved. The shapes come straight from the server's writer: a snapshot round announces
/// the table's whole row set in added_rows and carries data only for its own slice, named in added_rows_included;
/// a delta between rounds can carry data for rows added_rows does not mention and no data for rows it does.
/// </summary>
public class BarrageProcessorTest {
  private static readonly RowSequence None = RowSequence.CreateEmpty();

  private static RowSequence Range(UInt64 begin, UInt64 end) => RowSequence.CreateSequential(Interval.Of(begin, end));

  private static RowSequence Keys(params UInt64[] keys) {
    var builder = new RowSequenceBuilder();
    foreach (var key in keys) {
      builder.Add(key);
    }
    return builder.Build();
  }

  /// <summary>
  /// Subscribes to a 1000-row table and consumes the first snapshot round, which delivers keys 0..399.
  /// </summary>
  private static BarrageProcessorDriver AfterFirstRound() {
    var driver = new BarrageProcessorDriver();
    var update = driver.FeedRows(AddsOnly(Range(0, 1000), Range(0, 400), isSnapshot: true), Range(0, 400));
    if (update == null) {
      throw new Exception("First round did not produce an update");
    }
    return driver;
  }

  [Test]
  public async Task SnapshotRoundIsSizedFromIncludedRows() {
    var driver = new BarrageProcessorDriver();
    var update = driver.FeedRows(AddsOnly(Range(0, 1000), Range(0, 400), isSnapshot: true), Range(0, 400));
    // Sized from added_rows, the add phase would still be waiting for 600 more rows here.
    await Assert.That(update).IsNotNull();
    await Assert.That(Render(update!.AddedRowsIndexSpace)).IsEqualTo("[0,400)");
    await AssertHolds(update.Current, Range(0, 400));
  }

  [Test]
  public async Task IncludedRowsSpanRecordBatches() {
    var driver = new BarrageProcessorDriver();
    var metadata = AddsOnly(Range(0, 1000), Range(0, 400), isSnapshot: true);
    await Assert.That(driver.FeedRows(metadata, Range(0, 250))).IsNull();
    var update = driver.FeedRows(null, Range(250, 400));
    await Assert.That(update).IsNotNull();
    await AssertHolds(update!.Current, Range(0, 400));
  }

  [Test]
  public async Task LaterRoundsAnnounceTheWholeRowSetAgain() {
    var driver = AfterFirstRound();
    var all = Range(0, 1000);
    var second = driver.FeedRows(AddsOnly(all, Range(400, 800), isSnapshot: true), Range(400, 800));
    var third = driver.FeedRows(AddsOnly(all, Range(800, 1000), isSnapshot: true), Range(800, 1000));
    await Assert.That(Render(second!.AddedRowsIndexSpace)).IsEqualTo("[400,800)");
    await Assert.That(Render(third!.AddedRowsIndexSpace)).IsEqualTo("[800,1000)");
    await AssertHolds(third.Current, all);
  }

  /// <summary>
  /// A delta announcing a row beyond the delivered region carries no data for it: the included set is present but
  /// empty. The batch holds only modify data, here one row for the Value column and none for Key, and the add
  /// phase must not consume it. This is the shape behind "Chunks have inconsistent sizes".
  /// </summary>
  [Test]
  public async Task DeltaWithEmptyIncludedRowsSkipsTheAddPhase() {
    var driver = AfterFirstRound();
    var metadata = BarrageMessages.Metadata(None, Keys(1000), None, [None, Keys(5)]);
    var update = driver.Feed(metadata, [], [999]);
    await Assert.That(update).IsNotNull();
    await Assert.That(update!.AddedRowsIndexSpace.IsEmpty).IsTrue();
    await Assert.That(Render(update.ModifiedRowsIndexSpace[1])).IsEqualTo("[5,6)");
    await Assert.That(update.Current.NumRows).IsEqualTo(400L);
    await Assert.That(Column(update.Current, 1)[5]).IsEqualTo(999L);
  }

  /// <summary>
  /// Removing a row inside the delivered region moves the next undelivered row into it, and the server sends that
  /// row's data without announcing it in added_rows. It has to be inserted all the same.
  /// </summary>
  [Test]
  public async Task ScopedInRowArrivesWithEmptyAddedRows() {
    var driver = AfterFirstRound();
    var metadata = BarrageMessages.Metadata(Keys(10), None, Keys(400), [None, None]);
    var update = driver.FeedRows(metadata, Keys(400));
    await Assert.That(update).IsNotNull();
    await Assert.That(Render(update!.RemovedRowsIndexSpace)).IsEqualTo("[10,11)");
    await Assert.That(Render(update.AddedRowsIndexSpace)).IsEqualTo("[399,400)");
    await AssertHolds(update.Current, Range(0, 10), Range(11, 401));
  }

  [Test]
  public async Task AbsentIncludedRowsMeansSameAsAddedRows() {
    var driver = new BarrageProcessorDriver();
    driver.FeedRows(AddsOnly(Range(0, 1000), Range(0, 1000), isSnapshot: true), Range(0, 1000));
    var update = driver.FeedRows(AddsOnly(Range(1000, 1005), null), Range(1000, 1005));
    await Assert.That(update).IsNotNull();
    await Assert.That(Render(update!.AddedRowsIndexSpace)).IsEqualTo("[1000,1005)");
    await AssertHolds(update.Current, Range(0, 1005));
  }

  /// <summary>
  /// While the initial snapshot is still arriving the server can re-send a row the client already holds. Treating
  /// that as an overwrite is a separate change; until it lands, the row must be rejected rather than inserted a
  /// second time.
  /// </summary>
  [Test]
  public async Task ReSentRowIsRejected() {
    var driver = AfterFirstRound();
    var metadata = AddsOnly(Range(0, 1000), Range(399, 800), isSnapshot: true);
    await Assert.That(() => { driver.FeedRows(metadata, Range(399, 800)); })
      .Throws<Exception>()
      .WithMessageContaining("duplicates");
  }

  /// <summary>
  /// A full subscription is sent the server's entire removed set, which while the snapshot is still arriving can
  /// name rows this client has not been given yet. Skipping those is a separate change; until it lands, such a
  /// remove must be rejected rather than silently applied.
  /// </summary>
  [Test]
  public async Task RemoveOfUndeliveredRowsIsRejected() {
    var driver = AfterFirstRound();
    var metadata = BarrageMessages.Metadata(Range(800, 820), None, null, [None, None]);
    await Assert.That(() => { driver.Feed(metadata, [], []); })
      .Throws<Exception>()
      .WithMessageContaining("the set only contains");
  }
}
