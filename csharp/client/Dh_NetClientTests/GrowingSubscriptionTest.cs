//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using System.Diagnostics;
using Apache.Arrow;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

/// <summary>
/// Subscribes to tables whose initial snapshot the server delivers in several rounds, with deltas arriving in
/// between, and checks that the whole table arrives intact. These run against a real server the message shapes
/// that BarrageProcessorTest feeds in by hand. Each table comes from the bug report that led to the fix.
///
/// Two things about the tables are deliberate. Each has a column computed by a Python function that sleeps for a
/// few microseconds per row, so that every snapshot round costs the server real time. The server sizes a round to
/// about a quarter of an update graph cycle, so the rounds come out small and the snapshot as a whole spans
/// several cycles, which guarantees, on any machine, that the ticking part of the table produces deltas between
/// rounds. Without that, a table of this size arrives in two back-to-back rounds and whether a tick lands in the
/// gap is luck. Secondly, a column whose value is the same in every row at the moment the table is fetched is
/// advertised by the server as run-end encoded, which this client cannot decode on the subscription path. So the
/// tables avoid constant columns, and in particular the joined table below is seeded with a row so that the joined
/// column is not null everywhere until the first tick.
/// </summary>
public class GrowingSubscriptionTest {
  private const int BaseRows = 20_000;

  /// <summary>
  /// The Python preamble shared by both tables: the function that makes a row expensive to snapshot.
  /// </summary>
  private const string Expensive = """
    from deephaven import empty_table, time_table, merge
    import time

    def expensive(i):
        time.sleep(0.00002)
        return i * 3
    """;

  /// <summary>
  /// A table that keeps appending rows while its snapshot is being delivered. The add phase used to be sized from
  /// added_rows of the first round, so the rounds that also carried the rows appended meanwhile had more data than
  /// it was willing to take: "There is excess data in the chunk that I won't be able to process".
  /// </summary>
  [Test]
  public async Task LargeAppendingTableIsFullyDelivered() {
    var script = Expensive + "\n\n" + $"""
      _base = empty_table({BaseRows}).update(["K = ii", "Price = ii * 1.5"]).update_view(["Slow = (long) expensive(K)"])
      _tick = (time_table("PT0.002S").update(["K = 9_000_000 + ii", "Price = ii * 2.5"])
          .update_view(["Slow = (long) expensive(K)"]).drop_columns("Timestamp"))
      growing_appends = merge([_base, _tick])
      """;
    var table = await AssertFullyDelivered(script, "growing_appends");

    // The base rows come first, with keys 0, 1, 2, ...; the appended rows follow, with keys from 9,000,000 up.
    // Every row's other columns are functions of its key, so a misaligned row shows up as a wrong value.
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
    var script = Expensive + "\n\n" + $"""
      # A one-row table that ticks, joined to every row of the big table so that every row is modified each cycle.
      _seed = empty_table(1).update(["Beat = (long) 0"])
      _beat = merge([_seed, time_table("PT0.002S").update(["Beat = ii"]).drop_columns("Timestamp")]).last_by()
      _cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
      growing_modifies = (empty_table({BaseRows}).update(["K = ii"] + _cols)
          .update_view(["Slow = (long) expensive(K)"])
          .natural_join(_beat, on=[], joins=["Beat"])
          .update_view(["BeatK = Beat * 1_000_000 + K"])
          .drop_columns("Beat"))
      """;
    var table = await AssertFullyDelivered(script, "growing_modifies");

    // Columns are K, C0..C29, Slow, BeatK. BeatK combines the beat the row was last modified with and the row's
    // own key; every row has seen the same beat, because each round carries the beat as of that round and the
    // deltas between rounds bring the earlier rows up to date.
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
  /// Defines the table, subscribes, and waits until the subscription has held at least as many rows as the table
  /// had when it was opened, which means the initial snapshot has been delivered in full. Returns the table as of
  /// that update.
  /// </summary>
  private static async Task<IClientTable> AssertFullyDelivered(string script, string tableName) {
    using var ctx = CommonContextForTests.Create(new ClientOptions().SetSessionType(SessionTypes.Python));
    var manager = ctx.Client.Manager;
    manager.RunScript(script);

    using var table = manager.FetchTable(tableName);
    var rowsAtOpen = table.NumRows;
    Console.WriteLine($"{tableName}: {rowsAtOpen:N0} rows at open");

    var observer = new SnapshotObserver(rowsAtOpen);
    using var cookie = table.Subscribe(observer);
    Exception? error;
    try {
      error = await observer.Finished.WaitAsync(TimeSpan.FromSeconds(120));
    } catch (TimeoutException) {
      throw new Exception($"Timed out after {observer.UpdateCount} updates, having reached only " +
                          $"{observer.PeakRowCount:N0} of {rowsAtOpen:N0} rows");
    }

    await Assert.That(error).IsNull();
    await Assert.That(observer.PeakRowCount).IsGreaterThanOrEqualTo(rowsAtOpen);
    return observer.Last!;
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
  /// Logs each update and completes once the table has reached its opening row count, or on error. Keeps the table
  /// as of the completing update for the caller to inspect.
  /// </summary>
  private sealed class SnapshotObserver(Int64 rowsAtOpen) : IObserver<TickingUpdate> {
    private const int MaxLoggedUpdates = 40;
    private readonly TaskCompletionSource<Exception?> _finished =
      new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Stopwatch _sinceSubscribed = Stopwatch.StartNew();
    private double _previousAtMs;

    public Task<Exception?> Finished => _finished.Task;
    public int UpdateCount { get; private set; }
    public Int64 PeakRowCount { get; private set; }
    public IClientTable? Last { get; private set; }

    public void OnNext(TickingUpdate update) {
      if (_finished.Task.IsCompleted) {
        // The table keeps ticking after the snapshot; only the completing update is of interest.
        return;
      }

      var nowMs = _sinceSubscribed.Elapsed.TotalMilliseconds;
      ++UpdateCount;
      var rowCount = update.Current.NumRows;
      PeakRowCount = Math.Max(PeakRowCount, rowCount);

      if (UpdateCount <= MaxLoggedUpdates) {
        Console.WriteLine($"  #{UpdateCount,-3} +{nowMs - _previousAtMs,6:N0}ms  t={nowMs,7:N0}ms" +
                          $"  added={update.AddedRowsIndexSpace.Count,8:N0}" +
                          $"  removed={update.RemovedRowsIndexSpace.Count,7:N0}  rows={rowCount,9:N0}");
      }
      _previousAtMs = nowMs;

      if (PeakRowCount >= rowsAtOpen) {
        Last = update.Current;
        _finished.TrySetResult(null);
      }
    }

    public void OnError(Exception error) {
      Console.WriteLine($"  error after {UpdateCount} updates: {error.Message}");
      _finished.TrySetResult(error);
    }

    public void OnCompleted() {
      _finished.TrySetResult(null);
    }
  }
}
