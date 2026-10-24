/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
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

// Subscribes to tables whose initial snapshot the server delivers in several rounds, with deltas
// arriving in between, and checks that the whole table arrives intact. These run against a real
// server the message shapes that the BarrageProcessor tests feed in by hand. Each table comes from
// the bug report that led to the fix.
//
// Two things about the tables are deliberate. Each has a column computed by a Python function that
// sleeps for a few microseconds per row, so that every snapshot round costs the server real time.
// The server sizes a round to about a quarter of an update graph cycle, so the rounds come out
// small and the snapshot as a whole spans several cycles, which guarantees, on any machine, that
// the ticking part of the table produces deltas between rounds. Without that, a table of this size
// arrives in two back-to-back rounds and whether a tick lands in the gap is luck. Secondly, a
// column whose value is the same in every row at the moment the table is fetched is advertised by
// the server as run-end encoded, which this client cannot decode on the subscription path. So the
// tables avoid constant columns, and in particular the joined table below is seeded with a row so
// that the joined column is not null everywhere until the first tick.
namespace deephaven::client::tests {
namespace {
constexpr int64_t kBaseRows = 20'000;

// The Python preamble shared by every table: the function that makes a row expensive to snapshot.
const char *kExpensive = R"xxx(
from deephaven import empty_table, time_table, merge
import time

def expensive(i):
    time.sleep(0.00002)
    return i * 3
)xxx";

// Logs each update and completes once 'is_complete' accepts the table, or on error. Keeps the
// table as of the completing update for the caller to inspect.
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
    if (update_count_ <= kMaxLoggedUpdates) {
      fmt::print(std::cerr, "  #{:<3} t={:>7}ms  added={:>8}  removed={:>7}  rows={:>9}\n",
          update_count_, since_start, update.AddedRows()->Size(), update.RemovedRows()->Size(),
          current->NumRows());
    }
    peak_row_count_ = std::max(peak_row_count_, current->NumRows());
    if (is_complete_(*current)) {
      last_ = current;
      cond_var_.notify_all();
    }
  }

  void OnFailure(std::exception_ptr ep) final {
    std::unique_lock guard(mutex_);
    exception_ptr_ = std::move(ep);
    cond_var_.notify_all();
  }

  // Waits for the completing update and returns its table; rethrows a subscription failure.
  std::shared_ptr<ClientTable> WaitForCompletion(std::chrono::seconds timeout) {
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
    return last_;
  }

private:
  static constexpr int kMaxLoggedUpdates = 40;

  std::function<bool(const ClientTable &)> is_complete_;
  std::chrono::steady_clock::time_point start_ = std::chrono::steady_clock::now();
  std::mutex mutex_;
  std::condition_variable cond_var_;
  int update_count_ = 0;
  size_t peak_row_count_ = 0;
  std::shared_ptr<ClientTable> last_;
  std::exception_ptr exception_ptr_;
};

// Defines the table, subscribes, and returns the table as of the first update that 'is_complete'
// accepts.
std::shared_ptr<ClientTable> SubscribeUntil(const std::string &script, const std::string &table_name,
    std::function<bool(const ClientTable &)> is_complete) {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();
  thm.RunScript(script);
  auto table = thm.FetchTable(table_name);
  fmt::print(std::cerr, "{}: {} rows at open\n", table_name, table.NumRows());

  auto observer = std::make_shared<SnapshotObserver>(std::move(is_complete));
  auto cookie = table.Subscribe(observer);
  auto result = observer->WaitForCompletion(std::chrono::seconds(120));
  table.Unsubscribe(std::move(cookie));
  return result;
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

// A table that keeps appending rows while its snapshot is being delivered. The add phase used to be
// sized from added_rows of the first round, so the rounds that also carried the rows appended
// meanwhile had more data than it was willing to take: "There is excess data in the chunk".
TEST_CASE("Growing subscription: large appending table is fully delivered", "[growing]") {
  auto script = std::string(kExpensive) + fmt::format(R"xxx(
_base = empty_table({0}).update(["K = ii", "Price = ii * 1.5"]).update_view(["Slow = (long) expensive(K)"])
_tick = (time_table("PT0.002S").update(["K = 9_000_000 + ii", "Price = ii * 2.5"])
    .update_view(["Slow = (long) expensive(K)"]).drop_columns("Timestamp"))
growing_appends = merge([_base, _tick])
)xxx", kBaseRows);
  auto table = SubscribeUntil(script, "growing_appends",
      [](const ClientTable &t) { return t.NumRows() >= static_cast<size_t>(kBaseRows); });

  // The base rows come first, with keys 0, 1, 2, ...; the appended rows follow, with keys from
  // 9,000,000 up. Every row's other columns are functions of its key, so a misaligned row shows up
  // as a wrong value.
  auto keys = Longs(*table, 0);
  auto prices = Doubles(*table, 1);
  auto slow = Longs(*table, 2);
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
  auto script = std::string(kExpensive) + fmt::format(R"xxx(
# A one-row table that ticks, joined to every row of the big table so that every row is modified
# each cycle.
_seed = empty_table(1).update(["Beat = (long) 0"])
_beat = merge([_seed, time_table("PT0.002S").update(["Beat = ii"]).drop_columns("Timestamp")]).last_by()
_cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
growing_modifies = (empty_table({0}).update(["K = ii"] + _cols)
    .update_view(["Slow = (long) expensive(K)"])
    .natural_join(_beat, on=[], joins=["Beat"])
    .update_view(["BeatK = Beat * 1_000_000 + K"])
    .drop_columns("Beat"))
)xxx", kBaseRows);
  auto table = SubscribeUntil(script, "growing_modifies",
      [](const ClientTable &t) { return t.NumRows() == static_cast<size_t>(kBaseRows); });

  // Columns are K, C0..C29, Slow, BeatK. BeatK combines the beat the row was last modified with and
  // the row's own key; every row has seen the same beat, because each round carries the beat as of
  // that round and the deltas between rounds bring the earlier rows up to date.
  std::vector<std::vector<int64_t>> columns;
  for (size_t c = 0; c != 33; ++c) {
    columns.push_back(Longs(*table, c));
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
// the deltas between rounds name rows this client has not been given yet. Those used to be
// converted strictly and throw out of SpaceMapper.
TEST_CASE("Growing subscription: table losing undelivered rows is fully delivered", "[growing]") {
  constexpr int64_t kRemovedRows = 1'000;
  auto script = std::string(kExpensive) + fmt::format(R"xxx(
# Removes the highest keys first, one every 2ms, and stops after {1} of them.
_blacklist = (time_table("PT0.002S").update(["K = (long) ({2} - ii)"])
    .drop_columns("Timestamp").head({1}))
_cols = ["C%d = ii * %d" % (i, i + 1) for i in range(30)]
growing_removes = (empty_table({0}).update(["K = ii"] + _cols)
    .update_view(["Slow = (long) expensive(K)"])
    .where_not_in(_blacklist, "K"))
)xxx", kBaseRows, kRemovedRows, kBaseRows - 1);
  auto table = SubscribeUntil(script, "growing_removes", [](const ClientTable &t) {
    return t.NumRows() == static_cast<size_t>(kBaseRows - kRemovedRows);
  });

  // Columns are K, C0..C29, Slow. What survives is the lowest keys, in order.
  std::vector<std::vector<int64_t>> columns;
  for (size_t c = 0; c != 32; ++c) {
    columns.push_back(Longs(*table, c));
  }
  const auto &keys = columns[0];
  const auto &slow = columns[31];
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
    return {};
  }) == "");
}

// A table that gains rows ahead of the region the snapshot has delivered. The server tracks what
// the client holds as a range of positions, so each insertion pushes the last delivered rows out of
// that range, and the next round sends them again. This client used to insert a re-sent row
// strictly: "Some elements of [...] were already in the set".
TEST_CASE("Growing subscription: table gaining rows ahead of the snapshot is fully delivered",
    "[growing]") {
  constexpr int64_t kTickRows = 1'000;
  auto script = std::string(kExpensive) + fmt::format(R"xxx(
# The ticking constituent comes first in the merge, so its rows take the lowest row keys and land
# ahead of every base row. It stops after {1} rows so that the table settles.
_tick = (time_table("PT0.002S").update(["K = 9_000_000 + ii", "Price = ii * 2.5"])
    .update_view(["Slow = (long) expensive(K)"]).drop_columns("Timestamp").head({1}))
_base = empty_table({0}).update(["K = ii", "Price = ii * 1.5"]).update_view(["Slow = (long) expensive(K)"])
growing_inserts = merge([_tick, _base])
)xxx", kBaseRows, kTickRows);
  auto table = SubscribeUntil(script, "growing_inserts", [](const ClientTable &t) {
    return t.NumRows() == static_cast<size_t>(kTickRows + kBaseRows);
  });

  // The ticking rows come first, with keys from 9,000,000 up; the base rows follow, with keys 0,
  // 1, 2, ...
  auto keys = Longs(*table, 0);
  auto prices = Doubles(*table, 1);
  auto slow = Longs(*table, 2);
  CHECK(FirstBadRow(keys.size(), [&](size_t i) -> std::optional<std::string> {
    auto index = static_cast<int64_t>(i);
    auto expected_key = index < kTickRows ? 9'000'000 + index : index - kTickRows;
    if (keys[i] != expected_key) {
      return fmt::format("K is {}, expected {}", keys[i], expected_key);
    }
    auto expected_price = index < kTickRows ? (keys[i] - 9'000'000) * 2.5 : keys[i] * 1.5;
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
