//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using System.Diagnostics;
using Apache.Arrow;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// Subscribes to tables whose initial snapshot the server delivers in several rounds, with a delta arriving in
/// between, and checks that the whole table arrives intact. These run against a real server the message shapes
/// that BarrageProcessorTest feeds in by hand. Each table comes from the bug report that led to the fix.
///
/// Whether the delta lands between rounds is not left to timing. The part of a table that changes is an input
/// table, empty when the subscription opens, and the test waits for the first round to arrive before adding rows
/// to it with a second script, so that the change is applied while the rest of the snapshot is still being
/// delivered. The helper hands back what the subscription reported for every update from that point on, and each
/// test asserts that its delta was observed, so that it cannot pass without the scenario it describes. The one
/// table that ticks on its own does so on every update graph cycle for as long as the subscription lives.
///
/// Two things about the tables are deliberate. Each has a column computed by a Python function that sleeps for a
/// few microseconds per row, so that every snapshot round costs the server real time. The server sizes a round to
/// about a quarter of an update graph cycle, so the rounds come out small and the snapshot as a whole spans
/// several cycles, which leaves the delta room to land between rounds. Without that, a table of this size arrives
/// in two back-to-back rounds. Secondly, a column whose value is the same in every row at the moment the table is
/// fetched is advertised by the server as run-end encoded, which this client cannot decode on the subscription
/// path. So the tables avoid constant columns, and in particular the joined table below is seeded with a row so
/// that the joined column is not null everywhere until the first tick.
/// </summary>
public class GrowingSubscriptionTest {
  private const int BaseRows = 20_000;

  /// <summary>
  /// The Python preamble shared by the tables: the function that makes a row expensive to snapshot.
  /// </summary>
  private const string Expensive = """
    from deephaven import empty_table, time_table, merge, input_table
    import time

    def expensive(i):
        time.sleep(0.00002)
        return i * 3
    """;

  /// <summary>
  /// A table that gains rows at its end while its snapshot is being delivered. The add phase used to be sized from
  /// added_rows of the first round, so the rounds that also carried the rows appended meanwhile had more data than
  /// it was willing to take: "There is excess data in the chunk that I won't be able to process".
  /// </summary>
  [Test]
  public async Task LargeAppendingTableIsFullyDelivered() {
    const int appendedRows = 2_000;
    var run = RunId();
    var script = Expensive + "\n\n" + $"""
      _base = empty_table({BaseRows}).update(["K = ii", "Price = ii * 1.5"]).update_view(["Slow = (long) expensive(K)"])
      # The rows appended once the snapshot is under way; empty until then, with the columns of the rows to come.
      appended_{run} = input_table(init_table=empty_table(0).update(["K = ii", "Price = ii * 2.5"]))
      growing_appends_{run} = merge([_base, appended_{run}.update_view(["Slow = (long) expensive(K)"])])
      """;
    var delta = $"""
      appended_{run}.add(empty_table({appendedRows}).update(["K = 9_000_000 + ii", "Price = ii * 2.5"]))
      """;
    var delivery = await AssertFullyDelivered(script, $"growing_appends_{run}", delta, settlesAt: BaseRows + appendedRows);

    // The appended rows lie beyond the region the snapshot has delivered, so their delta reaches the client with
    // no data for them: an update that adds nothing. The rows themselves follow in a later round.
    await Assert.That(delivery.SinceDelta.Any(r => r.Added == 0 && r.Removed == 0)).IsTrue()
      .Because("the delta appending rows beyond the delivered region should arrive as an update that adds nothing");

    // The base rows come first, with keys 0, 1, 2, ...; the appended rows follow, with keys from 9,000,000 up.
    // Every row's other columns are functions of its key, so a misaligned row shows up as a wrong value.
    var table = delivery.Table;
    var keys = Longs(table, 0);
    var prices = Doubles(table, 1);
    var slow = Longs(table, 2);
    await Assert.That(FirstBadRow(keys.Length, i => {
      var expectedKey = i < BaseRows ? i : 9_000_000 + (i - BaseRows);
      if (keys[i] != expectedKey) {
        return $"K is {keys[i]}, expected {expectedKey}";
      }
      var expectedPrice = i < BaseRows ? keys[i] * 1.5 : (keys[i] - 9_000_000) * 2.5;
      if (prices[i] != expectedPrice) {
        return $"Price is {prices[i]}, expected {expectedPrice}";
      }
      return slow[i] == keys[i] * 3 ? null : $"Slow is {slow[i]}, expected {keys[i] * 3}";
    })).IsEqualTo("");
  }

  /// <summary>
  /// A wide table whose every row is modified on every cycle. The add phase, still waiting for rows the first round
  /// had announced but not delivered, used to consume the next message's modify data, which legitimately has data
  /// for one column and none for the others: "Chunks have inconsistent sizes".
  /// </summary>
  [Test]
  public async Task WideModifiedTableIsFullyDelivered() {
    var run = RunId();
    var script = Expensive + "\n\n" + $"""
      # A one-row table that ticks, joined to every row of the big table so that every row is modified each cycle.
      _seed = empty_table(1).update(["Beat = (long) 0"])
      _beat = merge([_seed, time_table("PT0.002S").update(["Beat = ii"]).drop_columns("Timestamp")]).last_by()
      _cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
      growing_modifies_{run} = (empty_table({BaseRows}).update(["K = ii"] + _cols)
          .update_view(["Slow = (long) expensive(K)"])
          .natural_join(_beat, on=[], joins=["Beat"])
          .update_view(["BeatK = Beat * 1_000_000 + K"])
          .drop_columns("Beat"))
      """;
    var delivery = await AssertFullyDelivered(script, $"growing_modifies_{run}");

    // The beat ticks on every cycle, so the deltas between rounds modify the rows delivered so far.
    await Assert.That(delivery.SinceDelta.Any(r => r.Modified != 0)).IsTrue()
      .Because("the rows delivered so far should have been modified between rounds");

    // Columns are K, C0..C29, Slow, BeatK. BeatK combines the beat the row was last modified with and the row's
    // own key; every row has seen the same beat, because each round carries the beat as of that round and the
    // deltas between rounds bring the earlier rows up to date.
    var table = delivery.Table;
    await Assert.That(table.NumRows).IsEqualTo((long)BaseRows);
    var columns = Enumerable.Range(0, 33).Select(c => Longs(table, c)).ToArray();
    var keys = columns[0];
    var slow = columns[31];
    var beatK = columns[32];
    var beat = beatK[0] / 1_000_000;
    await Assert.That(FirstBadRow(keys.Length, i => {
      if (keys[i] != i) {
        return $"K is {keys[i]}, expected {i}";
      }
      for (var c = 0; c != 30; ++c) {
        if (columns[c + 1][i] != keys[i] * (c + 1)) {
          return $"C{c} is {columns[c + 1][i]}, expected {keys[i] * (c + 1)}";
        }
      }
      if (slow[i] != keys[i] * 3) {
        return $"Slow is {slow[i]}, expected {keys[i] * 3}";
      }
      var expectedBeatK = beat * 1_000_000 + keys[i];
      return beatK[i] == expectedBeatK ? null : $"BeatK is {beatK[i]}, expected {expectedBeatK}";
    })).IsEqualTo("");
  }

  /// <summary>
  /// A table whose rows are being removed from the top of its key space while the snapshot is delivering it from
  /// the bottom. A full subscription receives the server's entire removed set, so a delta between rounds names
  /// rows this client has not been given yet. Those used to be converted strictly and throw out of SpaceMapper:
  /// "Of the 25 keys specified in Interval { Begin = 299960, ... }, the set only contains 0 keys".
  /// </summary>
  [Test]
  public async Task TableLosingUndeliveredRowsIsFullyDelivered() {
    // The delta removes a few rows from the bottom of the key space, which the first round has delivered, along
    // with many from the top, which the snapshot has not reached. The former make the delta visible to the client.
    const int removedFromBottom = 10;
    const int removedFromTop = 1_000;
    var run = RunId();
    var script = Expensive + "\n\n" + $"""
      # The keys of the rows removed once the snapshot is under way; empty until then.
      blacklist_{run} = input_table(init_table=empty_table(0).update(["K = ii"]))
      _cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
      growing_removes_{run} = (empty_table({BaseRows}).update(["K = ii"] + _cols)
          .update_view(["Slow = (long) expensive(K)"])
          .where_not_in(blacklist_{run}, "K"))
      """;
    var delta = $"""
      blacklist_{run}.add(merge([empty_table({removedFromBottom}).update(["K = ii"]),
          empty_table({removedFromTop}).update(["K = {BaseRows - removedFromTop} + ii"])]))
      """;
    var delivery = await AssertFullyDelivered(script, $"growing_removes_{run}", delta,
      settlesAt: BaseRows - removedFromBottom - removedFromTop);

    // The delta is the one update that removes rows. It must have erased only the rows from the bottom, and it must
    // have arrived while the client held fewer rows than the first key removed from the top, so that none of the
    // rows removed from the top was held and the removes naming them had to be skipped.
    var removes = delivery.SinceDelta.Where(r => r.Removed != 0).ToList();
    await Assert.That(removes.Count).IsEqualTo(1).Because("exactly one update should have removed rows");
    await Assert.That(removes[0].Removed).IsEqualTo((UInt64)removedFromBottom);
    await Assert.That(removes[0].RowsBefore).IsLessThanOrEqualTo((Int64)(BaseRows - removedFromTop))
      .Because("the delta should have arrived before the snapshot reached the rows removed from the top");

    // Columns are K, C0..C29, Slow. What survives is the keys between the two removed ranges, in order.
    var table = delivery.Table;
    var columns = Enumerable.Range(0, 32).Select(c => Longs(table, c)).ToArray();
    var keys = columns[0];
    var slow = columns[31];
    await Assert.That(FirstBadRow(keys.Length, i => {
      var expectedKey = removedFromBottom + i;
      if (keys[i] != expectedKey) {
        return $"K is {keys[i]}, expected {expectedKey}";
      }
      for (var c = 0; c != 30; ++c) {
        if (columns[c + 1][i] != keys[i] * (c + 1)) {
          return $"C{c} is {columns[c + 1][i]}, expected {keys[i] * (c + 1)}";
        }
      }
      return slow[i] == keys[i] * 3 ? null : $"Slow is {slow[i]}, expected {keys[i] * 3}";
    })).IsEqualTo("");
  }

  /// <summary>
  /// What the subscription reported for one update: the row count before it, and the sizes of the removed, added
  /// and modified sets it carried, the last being the largest over the columns.
  /// </summary>
  private readonly record struct Reported(Int64 RowsBefore, UInt64 Removed, UInt64 Added, UInt64 Modified);

  /// <summary>
  /// The table as of the completing update, and what the subscription reported for every update from the delta on.
  /// </summary>
  private sealed record Delivery(IClientTable Table, IReadOnlyList<Reported> SinceDelta);

  /// <summary>
  /// Defines the table, subscribes, and once the first snapshot round has arrived runs 'delta', the script that
  /// changes the table while the rest of the snapshot is still being delivered. Then waits until the subscription
  /// has held at least as many rows as the table had when it was opened, which for a table of fixed size means the
  /// initial snapshot has been delivered in full. A table whose row count the delta changes is complete at some
  /// other count: a growing one reaches its opening count while the snapshot still owes it the rows added since,
  /// and a shrinking one never reaches it. For such a table 'settlesAt' gives the row count at which it is
  /// complete, which the completing update must hit exactly. Returns the table as of the completing update, along
  /// with what the subscription reported for each update from the delta on (for a table that ticks on its own,
  /// each update after the first), so that the caller can check that the delta was observed.
  /// </summary>
  private static async Task<Delivery> AssertFullyDelivered(string script, string tableName, string? delta = null,
      Int64? settlesAt = null) {
    using var ctx = CommonContextForTests.Create(new ClientOptions().SetSessionType(SessionTypes.Python));
    var manager = ctx.Client.Manager;
    manager.RunScript(script);

    using var table = manager.FetchTable(tableName);
    var rowsAtOpen = table.NumRows;
    Console.WriteLine($"{tableName}: {rowsAtOpen:N0} rows at open");

    var observer = new SnapshotObserver(rowsAtOpen, settlesAt);
    using var cookie = table.Subscribe(observer);
    var timeout = TimeSpan.FromSeconds(120);
    Exception? error;
    try {
      await observer.FirstUpdate.WaitAsync(timeout);
      // Marked before the script runs, so that no update the delta produces can land ahead of the mark.
      observer.MarkDelta();
      if (delta != null) {
        manager.RunScript(delta);
      }
      error = await observer.Finished.WaitAsync(timeout);
    } catch (TimeoutException) {
      var target = settlesAt ?? rowsAtOpen;
      throw new Exception($"Timed out after {observer.UpdateCount} updates, having reached only " +
                          $"{observer.PeakRowCount:N0} of {target:N0} rows");
    }

    await Assert.That(error).IsNull();
    if (settlesAt == null) {
      await Assert.That(observer.PeakRowCount).IsGreaterThanOrEqualTo(rowsAtOpen);
    } else {
      await Assert.That(observer.Last!.NumRows).IsEqualTo(settlesAt.Value);
    }
    return new Delivery(observer.Last!, observer.SinceDelta);
  }

  /// <summary>
  /// A suffix that makes a test's server-side names its own. The server's script session is shared by every
  /// console, and the canonical test run drives two copies of this suite, one per target framework, against one
  /// server at the same time. The second script of a test refers to its input table by name, so that name must
  /// not be one the other copy can rebind in between.
  /// </summary>
  private static string RunId() {
    return Guid.NewGuid().ToString("N")[..8];
  }

  /// <summary>
  /// Returns a description of the first row for which 'check' returns a complaint, or "" if there is none.
  /// </summary>
  private static string FirstBadRow(int numRows, Func<int, string?> check) {
    for (var i = 0; i != numRows; ++i) {
      var complaint = check(i);
      if (complaint != null) {
        return $"row {i}: {complaint}";
      }
    }
    return "";
  }

  private static Int64[] Longs(IClientTable table, int column) {
    return ((Int64Array)ArrowArrayConverter.ColumnSourceToArray(table.GetColumn(column), table.NumRows)).Values.ToArray();
  }

  private static double[] Doubles(IClientTable table, int column) {
    return ((DoubleArray)ArrowArrayConverter.ColumnSourceToArray(table.GetColumn(column), table.NumRows)).Values.ToArray();
  }

  /// <summary>
  /// Logs each update and completes once the table has reached its opening row count, or exactly 'settlesAt' rows
  /// when that is given, or on error. Keeps the table as of the completing update for the caller to inspect, and
  /// what every update reported, so that the caller can look at the ones that followed the delta.
  /// </summary>
  private sealed class SnapshotObserver(Int64 rowsAtOpen, Int64? settlesAt) : IObserver<TickingUpdate> {
    private const int MaxLoggedUpdates = 40;
    private readonly object _sync = new();
    private readonly TaskCompletionSource<bool> _firstUpdate =
      new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<Exception?> _finished =
      new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Stopwatch _sinceSubscribed = Stopwatch.StartNew();
    private readonly List<Reported> _reported = new();
    private int _deltaMark;
    private double _previousAtMs;

    /// <summary>
    /// Completes once the first update has arrived, or once the subscription has ended without one.
    /// </summary>
    public Task FirstUpdate => _firstUpdate.Task;
    public Task<Exception?> Finished => _finished.Task;
    public int UpdateCount { get; private set; }
    public Int64 PeakRowCount { get; private set; }
    public IClientTable? Last { get; private set; }

    /// <summary>
    /// What the updates from the delta mark on reported, in order.
    /// </summary>
    public IReadOnlyList<Reported> SinceDelta {
      get {
        lock (_sync) {
          return _reported.Skip(_deltaMark).ToList();
        }
      }
    }

    /// <summary>
    /// Marks the point from which updates count as following the delta.
    /// </summary>
    public void MarkDelta() {
      lock (_sync) {
        _deltaMark = _reported.Count;
      }
    }

    public void OnNext(TickingUpdate update) {
      if (_finished.Task.IsCompleted) {
        // The table keeps ticking after the snapshot; only the completing update is of interest.
        return;
      }

      var nowMs = _sinceSubscribed.Elapsed.TotalMilliseconds;
      ++UpdateCount;
      var rowCount = update.Current.NumRows;
      PeakRowCount = Math.Max(PeakRowCount, rowCount);
      // An update that modifies nothing carries no per-column sets at all.
      var modified = update.ModifiedRowsIndexSpace.Length == 0 ? 0UL
        : update.ModifiedRowsIndexSpace.Max(rows => rows.Count);
      var reported = new Reported(update.Prev.NumRows, update.RemovedRowsIndexSpace.Count,
        update.AddedRowsIndexSpace.Count, modified);
      lock (_sync) {
        _reported.Add(reported);
      }

      if (UpdateCount <= MaxLoggedUpdates) {
        Console.WriteLine($"  #{UpdateCount,-3} +{nowMs - _previousAtMs,6:N0}ms  t={nowMs,7:N0}ms" +
                          $"  added={reported.Added,8:N0}  removed={reported.Removed,7:N0}" +
                          $"  modified={reported.Modified,7:N0}  rows={rowCount,9:N0}");
      }
      _previousAtMs = nowMs;
      _firstUpdate.TrySetResult(true);

      var complete = settlesAt == null ? PeakRowCount >= rowsAtOpen : rowCount == settlesAt.Value;
      if (complete) {
        Last = update.Current;
        _finished.TrySetResult(null);
      }
    }

    public void OnError(Exception error) {
      Console.WriteLine($"  error after {UpdateCount} updates: {error.Message}");
      _finished.TrySetResult(error);
      _firstUpdate.TrySetResult(false);
    }

    public void OnCompleted() {
      _finished.TrySetResult(null);
      _firstUpdate.TrySetResult(false);
    }
  }
}
