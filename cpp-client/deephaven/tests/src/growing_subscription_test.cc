/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/ticking/ticking.h"

using deephaven::dhcore::chunk::DoubleChunk;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::ticking::TickingCallback;
using deephaven::dhcore::ticking::TickingUpdate;

// Subscribes to tables whose initial snapshot the server delivers in several rounds, with a delta
// arriving in between, and checks that the whole table arrives intact. These run against a real
// server the message shapes that the BarrageProcessor tests feed in by hand. Each table comes from
// the bug report that led to the fix.
//
// Whether the delta lands between rounds is not left to timing. The part of a table that changes
// is an input table, empty when the subscription opens, and the test waits for the first round to
// arrive before adding rows to it, so that the change is applied while the rest of the snapshot is
// still being delivered. The rows are added through the client rather than a script, because a
// script queues behind every other script on the server's shared session and holds the update
// graph's exclusive lock for as long as it runs, formula compilation included, while snapshot
// rounds carry on without that lock. The helper hands back what the subscription reported for
// every update from the delta on, and each test says what observing its delta looks like, so that
// it cannot pass without the scenario it describes. A loaded server can still complete a snapshot
// before the delta is applied; the helper then starts over with a fresh table, a bounded number of
// times, and fails loudly if the delta never lands in time. The one table that ticks on its own
// does so on every update graph cycle for as long as the subscription lives.
//
// Two things about the tables are deliberate. Each has a column computed by a Python function that
// sleeps for a few microseconds per row, so that every snapshot round costs the server real time.
// The server sizes a round to about a quarter of an update graph cycle, so the rounds come out
// small and the snapshot as a whole spans several cycles, which leaves the delta room to land
// between rounds. Without that, a table of this size arrives in two back-to-back rounds. Secondly,
// a column whose value is the same in every row at the moment the table is fetched is advertised
// by the server as run-end encoded, which this client cannot decode on the subscription path. So
// the tables avoid constant columns, and in particular the joined table below is seeded with a row
// so that the joined column is not null everywhere until the first tick.
namespace deephaven::client::tests {
namespace {
constexpr int64_t kBaseRows = 20'000;

// The Python preamble shared by every table: the function that makes a row expensive to snapshot.
const char *kExpensive = R"xxx(
from deephaven import empty_table, time_table, merge, input_table
import time

def expensive(i):
    time.sleep(0.00002)
    return i * 3
)xxx";

// A suffix that makes a test's server-side names its own. The server's script session is shared by
// every console, so two clients running this test against one server at the same time would
// otherwise rebind each other's tables, and an attempt that starts over needs a table of its own.
std::string RunId() {
  std::random_device device;
  return fmt::format("{:08x}", device());
}

// What the subscription reported for one update: the row count before it, and the sizes of the
// removed, added and modified sets it carried, the last being the largest over the columns.
struct Reported {
  size_t rows_before = 0;
  size_t removed = 0;
  size_t added = 0;
  size_t modified = 0;
};

// The table as of the completing update, and what the subscription reported for every update from
// the delta on.
struct Delivery {
  std::shared_ptr<ClientTable> table;
  std::vector<Reported> since_delta;
};

// A table to subscribe to and the change to make to it once its first snapshot round has arrived.
struct Scenario {
  // The table's name on the server, before the run id is appended.
  std::string table_name;
  // The script that defines the table, given the run id.
  std::function<std::string(const std::string &run)> script;
  // The change to make once the first round has arrived, given the manager and the run id; empty
  // for a table that ticks on its own.
  std::function<void(TableHandleManager &thm, const std::string &run)> delta;
  // Whether what the subscription reported from the delta on shows the delta having landed while
  // the snapshot was still being delivered.
  std::function<bool(const std::vector<Reported> &since)> delta_observed;
  // What delta_observed looks for, for the failure message.
  std::string delta_description;
  // Whether the table is complete: for a table of fixed size, once it has reached the row count it
  // had when it was opened; for a table whose row count the delta changes, exactly the row count it
  // settles at.
  std::function<bool(const ClientTable &table)> is_complete;
};

constexpr int kMaxAttempts = 3;

// Logs each update and completes once 'is_complete' accepts the table, or on error. Keeps the
// table as of the completing update for the caller to inspect, and what every update reported, so
// that the caller can look at the ones that followed the delta.
class SnapshotObserver final : public TickingCallback {
public:
  explicit SnapshotObserver(std::function<bool(const ClientTable &)> is_complete) :
      is_complete_(std::move(is_complete)) {}

  void OnTick(TickingUpdate update) final {
    std::unique_lock guard(mutex_);
    if (last_ != nullptr) {
      // The table keeps ticking after the snapshot; only the completing update is of interest.
      return;
    }
    auto now = std::chrono::steady_clock::now();
    auto since_start = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_).count();
    ++update_count_;
    const auto &current = update.Current();
    // An update that modifies nothing carries no per-column sets at all.
    size_t modified = 0;
    for (const auto &rows : update.ModifiedRows()) {
      modified = std::max(modified, rows->Size());
    }
    Reported reported{update.Prev()->NumRows(), update.RemovedRows()->Size(),
        update.AddedRows()->Size(), modified};
    reported_.push_back(reported);
    if (update_count_ <= kMaxLoggedUpdates) {
      fmt::print(std::cerr,
          "  #{:<3} t={:>7}ms  added={:>8}  removed={:>7}  modified={:>7}  rows={:>9}\n",
          update_count_, since_start, reported.added, reported.removed, reported.modified,
          current->NumRows());
    }
    peak_row_count_ = std::max(peak_row_count_, current->NumRows());
    if (is_complete_(*current)) {
      last_ = current;
    }
    cond_var_.notify_all();
  }

  void OnFailure(std::exception_ptr ep) final {
    std::unique_lock guard(mutex_);
    exception_ptr_ = std::move(ep);
    cond_var_.notify_all();
  }

  // Waits for the first update to arrive; rethrows a subscription failure that arrived instead.
  void WaitForFirstUpdate(std::chrono::seconds timeout) {
    std::unique_lock guard(mutex_);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (update_count_ == 0 && exception_ptr_ == nullptr) {
      if (cond_var_.wait_until(guard, deadline) == std::cv_status::timeout) {
        throw std::runtime_error("Timed out waiting for the first update");
      }
    }
    if (exception_ptr_ != nullptr) {
      std::rethrow_exception(exception_ptr_);
    }
  }

  // Marks the point from which updates count as following the delta.
  void MarkDelta() {
    std::unique_lock guard(mutex_);
    delta_mark_ = reported_.size();
  }

  // Waits for the completing update and returns its table along with what every update from the
  // delta mark on reported; rethrows a subscription failure.
  Delivery WaitForCompletion(std::chrono::seconds timeout) {
    std::unique_lock guard(mutex_);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (last_ == nullptr && exception_ptr_ == nullptr) {
      if (cond_var_.wait_until(guard, deadline) == std::cv_status::timeout) {
        throw std::runtime_error(fmt::format(
            "Timed out after {} updates, having held at most {} rows", update_count_, peak_row_count_));
      }
    }
    if (exception_ptr_ != nullptr) {
      std::rethrow_exception(exception_ptr_);
    }
    std::vector<Reported> since_delta(reported_.begin() + static_cast<std::ptrdiff_t>(delta_mark_),
        reported_.end());
    return {last_, std::move(since_delta)};
  }

private:
  static constexpr int kMaxLoggedUpdates = 40;

  std::function<bool(const ClientTable &)> is_complete_;
  std::chrono::steady_clock::time_point start_ = std::chrono::steady_clock::now();
  std::mutex mutex_;
  std::condition_variable cond_var_;
  int update_count_ = 0;
  size_t peak_row_count_ = 0;
  std::vector<Reported> reported_;
  size_t delta_mark_ = 0;
  std::shared_ptr<ClientTable> last_;
  std::exception_ptr exception_ptr_;
};

// One attempt at the scenario, on a table of its own: defines the table, subscribes, and once the
// first snapshot round has arrived makes the delta, so that the change is applied while the rest of
// the snapshot is still being delivered. Then waits for the first update that 'is_complete'
// accepts and returns its table, along with what the subscription reported for each update from
// the delta on (for a table that ticks on its own, every update).
Delivery Attempt(TableHandleManager &thm, const Scenario &scenario) {
  auto run = RunId();
  thm.RunScript(scenario.script(run));
  auto table_name = fmt::format("{}_{}", scenario.table_name, run);
  auto table = thm.FetchTable(table_name);
  fmt::print(std::cerr, "{}: {} rows at open\n", table_name, table.NumRows());

  auto observer = std::make_shared<SnapshotObserver>(scenario.is_complete);
  auto cookie = table.Subscribe(observer);
  const auto timeout = std::chrono::seconds(120);
  observer->WaitForFirstUpdate(timeout);
  if (scenario.delta) {
    // Marked before the delta is made, so that no update it produces can land ahead of the mark.
    observer->MarkDelta();
    scenario.delta(thm, run);
  }
  auto result = observer->WaitForCompletion(timeout);
  table.Unsubscribe(std::move(cookie));
  return result;
}

// Runs the scenario and returns what it delivered. A delta is applied only once the server's
// update graph gets to it, and a loaded server can finish the snapshot first, in which case the
// subscription reports nothing of the kind the scenario describes. Such an attempt is discarded and
// the scenario is tried again with a fresh table, up to kMaxAttempts times.
Delivery SubscribeUntil(const Scenario &scenario) {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();
  for (int attempt = 1; ; ++attempt) {
    auto delivery = Attempt(thm, scenario);
    if (scenario.delta_observed(delivery.since_delta)) {
      return delivery;
    }
    if (attempt == kMaxAttempts) {
      throw std::runtime_error(fmt::format(
          "In {} attempts the snapshot was never still being delivered when the delta landed, so it "
          "could not be checked that {}", attempt, scenario.delta_description));
    }
    fmt::print(std::cerr,
        "  attempt {}: the delta landed after the snapshot had completed; starting over with a fresh "
        "table\n", attempt);
  }
}

bool IsComplete(const ClientTable &table, int64_t rows) {
  return table.NumRows() == static_cast<size_t>(rows);
}

std::vector<int64_t> Longs(const ClientTable &table, size_t column_index) {
  auto rows = table.GetRowSequence();
  auto chunk = Int64Chunk::Create(rows->Size());
  table.GetColumn(column_index)->FillChunk(*rows, &chunk, nullptr);
  return {chunk.data(), chunk.data() + chunk.Size()};
}

std::vector<double> Doubles(const ClientTable &table, size_t column_index) {
  auto rows = table.GetRowSequence();
  auto chunk = DoubleChunk::Create(rows->Size());
  table.GetColumn(column_index)->FillChunk(*rows, &chunk, nullptr);
  return {chunk.data(), chunk.data() + chunk.Size()};
}

// Returns a description of the first row for which 'check' returns a complaint, or "" if none.
std::string FirstBadRow(size_t num_rows,
    const std::function<std::optional<std::string>(size_t)> &check) {
  for (size_t i = 0; i != num_rows; ++i) {
    auto complaint = check(i);
    if (complaint.has_value()) {
      return fmt::format("row {}: {}", i, *complaint);
    }
  }
  return "";
}
}  // namespace

// A table that gains rows at its end while its snapshot is being delivered. The add phase used to
// be sized from added_rows of the first round, so the rounds that also carried the rows appended
// meanwhile had more data than it was willing to take: "There is excess data in the chunk".
TEST_CASE("Growing subscription: large appending table is fully delivered", "[growing]") {
  constexpr int64_t kAppendedRows = 2'000;
  auto delivery = SubscribeUntil(Scenario{
      .table_name = "growing_appends",
      .script = [](const std::string &run) {
        return std::string(kExpensive) + fmt::format(R"xxx(
_base = empty_table({0}).update(["K = ii", "Price = ii * 1.5"]).update_view(["Slow = (long) expensive(K)"])
# The rows appended once the snapshot is under way; empty until then, with the columns of the rows
# to come.
appended_{1} = input_table(init_table=empty_table(0).update(["K = ii", "Price = ii * 2.5"]))
growing_appends_{1} = merge([_base, appended_{1}.update_view(["Slow = (long) expensive(K)"])])
)xxx", kBaseRows, run);
      },
      .delta = [](TableHandleManager &thm, const std::string &run) {
        auto appended = thm.FetchTable(fmt::format("appended_{}", run));
        appended.AddTable(thm.EmptyTable(kAppendedRows).Update("K = 9_000_000 + ii", "Price = ii * 2.5"));
      },
      // The appended rows lie beyond the region the snapshot has delivered, so their delta reaches
      // the client with no data for them: an update that adds nothing. The rows themselves follow in
      // a later round.
      .delta_observed = [](const std::vector<Reported> &since) {
        return std::any_of(since.begin(), since.end(),
            [](const Reported &r) { return r.added == 0 && r.removed == 0; });
      },
      .delta_description = "the delta appending rows beyond the delivered region arrives as an update "
          "that adds nothing",
      .is_complete = [](const ClientTable &t) { return IsComplete(t, kBaseRows + kAppendedRows); },
  });

  // The base rows come first, with keys 0, 1, 2, ...; the appended rows follow, with keys from
  // 9,000,000 up. Every row's other columns are functions of its key, so a misaligned row shows up
  // as a wrong value.
  const auto &table = *delivery.table;
  auto keys = Longs(table, 0);
  auto prices = Doubles(table, 1);
  auto slow = Longs(table, 2);
  CHECK(FirstBadRow(keys.size(), [&](size_t i) -> std::optional<std::string> {
    auto index = static_cast<int64_t>(i);
    auto expected_key = index < kBaseRows ? index : 9'000'000 + (index - kBaseRows);
    if (keys[i] != expected_key) {
      return fmt::format("K is {}, expected {}", keys[i], expected_key);
    }
    auto expected_price = index < kBaseRows ? keys[i] * 1.5 : (keys[i] - 9'000'000) * 2.5;
    if (prices[i] != expected_price) {
      return fmt::format("Price is {}, expected {}", prices[i], expected_price);
    }
    if (slow[i] != keys[i] * 3) {
      return fmt::format("Slow is {}, expected {}", slow[i], keys[i] * 3);
    }
    return {};
  }) == "");
}

// A wide table whose every row is modified on every cycle. The add phase, still waiting for rows
// the first round had announced but not delivered, used to consume the next message's modify data,
// which legitimately has data for one column and none for the others: "Chunks have inconsistent
// sizes".
TEST_CASE("Growing subscription: wide modified table is fully delivered", "[growing]") {
  auto delivery = SubscribeUntil(Scenario{
      .table_name = "growing_modifies",
      .script = [](const std::string &run) {
        return std::string(kExpensive) + fmt::format(R"xxx(
# A one-row table that ticks, joined to every row of the big table so that every row is modified
# each cycle.
_seed = empty_table(1).update(["Beat = (long) 0"])
_beat = merge([_seed, time_table("PT0.002S").update(["Beat = ii"]).drop_columns("Timestamp")]).last_by()
_cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
growing_modifies_{1} = (empty_table({0}).update(["K = ii"] + _cols)
    .update_view(["Slow = (long) expensive(K)"])
    .natural_join(_beat, on=[], joins=["Beat"])
    .update_view(["BeatK = Beat * 1_000_000 + K"])
    .drop_columns("Beat"))
)xxx", kBaseRows, run);
      },
      // The beat ticks on every cycle, so the deltas between rounds modify the rows delivered so far.
      .delta_observed = [](const std::vector<Reported> &since) {
        return std::any_of(since.begin(), since.end(),
            [](const Reported &r) { return r.modified != 0; });
      },
      .delta_description = "the rows delivered so far are modified between rounds",
      .is_complete = [](const ClientTable &t) { return IsComplete(t, kBaseRows); },
  });

  // Columns are K, C0..C29, Slow, BeatK. BeatK combines the beat the row was last modified with and
  // the row's own key; every row has seen the same beat, because each round carries the beat as of
  // that round and the deltas between rounds bring the earlier rows up to date.
  const auto &table = *delivery.table;
  std::vector<std::vector<int64_t>> columns;
  for (size_t c = 0; c != 33; ++c) {
    columns.push_back(Longs(table, c));
  }
  const auto &keys = columns[0];
  const auto &slow = columns[31];
  const auto &beat_k = columns[32];
  auto beat = beat_k[0] / 1'000'000;
  CHECK(FirstBadRow(keys.size(), [&](size_t i) -> std::optional<std::string> {
    auto index = static_cast<int64_t>(i);
    if (keys[i] != index) {
      return fmt::format("K is {}, expected {}", keys[i], index);
    }
    for (size_t c = 0; c != 30; ++c) {
      auto expected = keys[i] * static_cast<int64_t>(c + 1);
      if (columns[c + 1][i] != expected) {
        return fmt::format("C{} is {}, expected {}", c, columns[c + 1][i], expected);
      }
    }
    if (slow[i] != keys[i] * 3) {
      return fmt::format("Slow is {}, expected {}", slow[i], keys[i] * 3);
    }
    auto expected_beat_k = beat * 1'000'000 + keys[i];
    if (beat_k[i] != expected_beat_k) {
      return fmt::format("BeatK is {}, expected {}", beat_k[i], expected_beat_k);
    }
    return {};
  }) == "");
}

// A table whose rows are being removed from the top of its key space while the snapshot is
// delivering it from the bottom. A full subscription receives the server's entire removed set, so
// a delta between rounds names rows this client has not been given yet. Those used to be converted
// strictly and throw out of SpaceMapper.
TEST_CASE("Growing subscription: table losing undelivered rows is fully delivered", "[growing]") {
  // The delta removes a few rows from the bottom of the key space, which the first round has
  // delivered, along with many from the top, which the snapshot has not reached. The former make
  // the delta visible to the client.
  constexpr int64_t kRemovedFromBottom = 10;
  constexpr int64_t kRemovedFromTop = 1'000;
  auto delivery = SubscribeUntil(Scenario{
      .table_name = "growing_removes",
      .script = [](const std::string &run) {
        return std::string(kExpensive) + fmt::format(R"xxx(
# The keys of the rows removed once the snapshot is under way; empty until then.
blacklist_{1} = input_table(init_table=empty_table(0).update(["K = ii"]))
_cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
growing_removes_{1} = (empty_table({0}).update(["K = ii"] + _cols)
    .update_view(["Slow = (long) expensive(K)"])
    .where_not_in(blacklist_{1}, "K"))
)xxx", kBaseRows, run);
      },
      .delta = [](TableHandleManager &thm, const std::string &run) {
        auto blacklist = thm.FetchTable(fmt::format("blacklist_{}", run));
        auto bottom = thm.EmptyTable(kRemovedFromBottom).Update("K = ii");
        auto top = thm.EmptyTable(kRemovedFromTop).Update(
            fmt::format("K = {} + ii", kBaseRows - kRemovedFromTop));
        blacklist.AddTable(bottom.Merge(top));
      },
      // A delta that lands after the snapshot has completed removes rows too, all of them at once;
      // only one that arrives while the rows removed from the top are still undelivered shows the
      // scenario.
      .delta_observed = [](const std::vector<Reported> &since) {
        return std::any_of(since.begin(), since.end(), [](const Reported &r) {
          return r.removed != 0 && r.rows_before <= static_cast<size_t>(kBaseRows - kRemovedFromTop);
        });
      },
      .delta_description = "the delta removing rows arrives while the rows removed from the top are "
          "still undelivered",
      .is_complete = [](const ClientTable &t) {
        return IsComplete(t, kBaseRows - kRemovedFromBottom - kRemovedFromTop);
      },
  });

  // The delta is the one update that removes rows. It must have erased only the rows from the
  // bottom, and it must have arrived while the client held fewer rows than the first key removed
  // from the top, so that none of the rows removed from the top was held and the removes naming
  // them had to be skipped.
  std::vector<Reported> removes;
  for (const auto &reported : delivery.since_delta) {
    if (reported.removed != 0) {
      removes.push_back(reported);
    }
  }
  {
    INFO("exactly one update should have removed rows");
    REQUIRE(removes.size() == 1);
  }
  CHECK(removes[0].removed == static_cast<size_t>(kRemovedFromBottom));
  {
    INFO("the delta should have arrived before the snapshot reached the rows removed from the top");
    CHECK(removes[0].rows_before <= static_cast<size_t>(kBaseRows - kRemovedFromTop));
  }

  // Columns are K, C0..C29, Slow. What survives is the keys between the two removed ranges, in
  // order.
  const auto &table = *delivery.table;
  std::vector<std::vector<int64_t>> columns;
  for (size_t c = 0; c != 32; ++c) {
    columns.push_back(Longs(table, c));
  }
  const auto &keys = columns[0];
  const auto &slow = columns[31];
  CHECK(FirstBadRow(keys.size(), [&](size_t i) -> std::optional<std::string> {
    auto expected_key = kRemovedFromBottom + static_cast<int64_t>(i);
    if (keys[i] != expected_key) {
      return fmt::format("K is {}, expected {}", keys[i], expected_key);
    }
    for (size_t c = 0; c != 30; ++c) {
      auto expected = keys[i] * static_cast<int64_t>(c + 1);
      if (columns[c + 1][i] != expected) {
        return fmt::format("C{} is {}, expected {}", c, columns[c + 1][i], expected);
      }
    }
    if (slow[i] != keys[i] * 3) {
      return fmt::format("Slow is {}, expected {}", slow[i], keys[i] * 3);
    }
    return {};
  }) == "");
}

// A table that gains rows ahead of the region the snapshot has delivered. The server tracks what
// the client holds as a range of positions, so each insertion pushes the last delivered rows out of
// that range, and the next round sends them again. This client used to insert a re-sent row
// strictly: "Some elements of [...] were already in the set".
TEST_CASE("Growing subscription: table gaining rows ahead of the snapshot is fully delivered",
    "[growing]") {
  constexpr int64_t kInsertedRows = 1'000;
  auto delivery = SubscribeUntil(Scenario{
      .table_name = "growing_inserts",
      .script = [](const std::string &run) {
        return std::string(kExpensive) + fmt::format(R"xxx(
# The rows inserted once the snapshot is under way; empty until then. This constituent comes first
# in the merge, so its rows take the lowest row keys and land ahead of every base row.
inserted_{1} = input_table(init_table=empty_table(0).update(["K = ii", "Price = ii * 2.5"]))
_base = empty_table({0}).update(["K = ii", "Price = ii * 1.5"]).update_view(["Slow = (long) expensive(K)"])
growing_inserts_{1} = merge([inserted_{1}.update_view(["Slow = (long) expensive(K)"]), _base])
)xxx", kBaseRows, run);
      },
      .delta = [](TableHandleManager &thm, const std::string &run) {
        auto inserted = thm.FetchTable(fmt::format("inserted_{}", run));
        inserted.AddTable(thm.EmptyTable(kInsertedRows).Update("K = 9_000_000 + ii", "Price = ii * 2.5"));
      },
      // The inserted rows push as many delivered rows out of the server's range, and the rounds
      // that follow send each of those again, which this client reports as a row removed and added
      // back. Nothing else removes a row.
      .delta_observed = [](const std::vector<Reported> &since) {
        return std::any_of(since.begin(), since.end(),
            [](const Reported &r) { return r.removed != 0; });
      },
      .delta_description = "the rows the insertion pushed out of the server's range are sent again",
      .is_complete = [](const ClientTable &t) { return IsComplete(t, kInsertedRows + kBaseRows); },
  });

  size_t re_sent = 0;
  for (const auto &reported : delivery.since_delta) {
    re_sent += reported.removed;
  }
  INFO("each delivered row the insertion pushed out of the server's range should have been sent "
      "again");
  CHECK(re_sent == static_cast<size_t>(kInsertedRows));

  // The inserted rows come first, with keys from 9,000,000 up; the base rows follow, with keys 0,
  // 1, 2, ...
  const auto &table = *delivery.table;
  auto keys = Longs(table, 0);
  auto prices = Doubles(table, 1);
  auto slow = Longs(table, 2);
  CHECK(FirstBadRow(keys.size(), [&](size_t i) -> std::optional<std::string> {
    auto index = static_cast<int64_t>(i);
    auto expected_key = index < kInsertedRows ? 9'000'000 + index : index - kInsertedRows;
    if (keys[i] != expected_key) {
      return fmt::format("K is {}, expected {}", keys[i], expected_key);
    }
    auto expected_price = index < kInsertedRows ? (keys[i] - 9'000'000) * 2.5 : keys[i] * 1.5;
    if (prices[i] != expected_price) {
      return fmt::format("Price is {}, expected {}", prices[i], expected_price);
    }
    if (slow[i] != keys[i] * 3) {
      return fmt::format("Slow is {}, expected {}", slow[i], keys[i] * 3);
    }
    return {};
  }) == "");
}
}  // namespace deephaven::client::tests
