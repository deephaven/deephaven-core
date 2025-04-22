/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::chunk::DoubleChunk;
using deephaven::dhcore::chunk::FloatChunk;
using deephaven::dhcore::chunk::Int8Chunk;
using deephaven::dhcore::chunk::Int16Chunk;
using deephaven::dhcore::chunk::Int32Chunk;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::chunk::StringChunk;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::client::tests {
class CommonBase : public deephaven::dhcore::ticking::TickingCallback {
public:
  void OnFailure(std::exception_ptr ep) final {
    std::unique_lock guard(mutex_);
    exception_ptr_ = std::move(ep);
    cond_var_.notify_all();
  }

  std::pair<bool, std::exception_ptr> WaitForUpdate() {
    std::unique_lock guard(mutex_);
    while (true) {
      if (done_ || exception_ptr_ != nullptr) {
        return std::make_pair(done_, exception_ptr_);
      }
      cond_var_.wait(guard);
    }
  }

protected:
  void NotifyDone() {
    std::unique_lock guard(mutex_);
    done_ = true;
    cond_var_.notify_all();
  }

  std::mutex mutex_;
  std::condition_variable cond_var_;
  bool done_ = false;
  std::exception_ptr exception_ptr_;
};

class ReachesNRowsCallback final : public CommonBase {
public:
  explicit ReachesNRowsCallback(size_t target_rows) : target_rows_(target_rows) {}

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    std::cout << "=== The Full Table ===\n"
              << update.Current()->Stream(true, true)
              << '\n';
    if (update.Current()->NumRows() >= target_rows_) {
      NotifyDone();
    }
  }

private:
  size_t target_rows_ = 0;
};

TEST_CASE("Ticking Table: eventually reaches 10 rows", "[ticking]") {
  const auto max_rows = 10;
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();

  auto table = tm.TimeTable(std::chrono::milliseconds(500)).Update("II = ii");
  table.BindToVariable("ticking");
  auto callback = std::make_shared<ReachesNRowsCallback>(max_rows);
  auto cookie = table.Subscribe(callback);

  while (true) {
    auto [done, eptr] = callback->WaitForUpdate();
    if (done) {
      break;
    }
    if (eptr != nullptr) {
      std::rethrow_exception(eptr);
    }
  }

  table.Unsubscribe(std::move(cookie));
}

class AllValuesGreaterThanNCallback final : public CommonBase {
public:
  explicit AllValuesGreaterThanNCallback(int64_t target) : target_(target) {}

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    const auto &current = update.Current();
    std::cout << "=== The Full Table ===\n"
              << current->Stream(true, true)
              << '\n';
    if (current->NumRows() == 0) {
      return;
    }
    auto col = current->GetColumn("Value", true);
    auto rs = current->GetRowSequence();
    auto data = Int64Chunk::Create(rs->Size());
    col->FillChunk(*rs, &data, nullptr);
    auto all_greater = true;
    for (auto element : data) {
      if (element < target_) {
        all_greater = false;
        break;
      }
    }
    if (all_greater) {
      NotifyDone();
    }
  }

private:
  int64_t target_ = 0;
};

TEST_CASE("Ticking Table: modified rows are eventually all greater than 10", "[ticking]") {
  const int64_t target = 10;
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();

  auto table = tm.TimeTable(std::chrono::milliseconds(500))
      .View({"Key = (long)(ii % 10)", "Value = ii"})
      .LastBy("Key");
  auto callback = std::make_shared<AllValuesGreaterThanNCallback>(target);
  auto cookie = table.Subscribe(callback);

  while (true) {
    auto [done, eptr] = callback->WaitForUpdate();
    if (done) {
      break;
    }
    if (eptr != nullptr) {
      std::rethrow_exception(eptr);
    }
  }

  table.Unsubscribe(std::move(cookie));
}

class WaitForPopulatedTableCallback final : public CommonBase {
public:
  explicit WaitForPopulatedTableCallback(int64_t target) : target_(target) {}

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    const auto &current = update.Current();
    std::cout << "=== The Full Table ===\n"
        << current->Stream(true, true)
        << '\n';

    if (current->NumRows() < static_cast<size_t>(target_)) {
      // table not yet fully populated.
      return;
    }

    if (current->NumRows() > static_cast<size_t>(target_)) {
      // table has more rows than expected.
      auto message = fmt::format("Expected table to have {} rows, got {}",
          target_, current->NumRows());
      throw std::runtime_error(message);
    }

    auto chars = MakeReservedVector<std::optional<char16_t>>(target_);
    auto int8s = MakeReservedVector<std::optional<int8_t>>(target_);
    auto int16s = MakeReservedVector<std::optional<int16_t>>(target_);
    auto int32s = MakeReservedVector<std::optional<int32_t>>(target_);
    auto int64s = MakeReservedVector<std::optional<int64_t>>(target_);
    auto floats = MakeReservedVector<std::optional<float>>(target_);
    auto doubles = MakeReservedVector<std::optional<double>>(target_);
    auto bools = MakeReservedVector<std::optional<bool>>(target_);
    auto strings = MakeReservedVector<std::optional<std::string>>(target_);
    auto date_times = MakeReservedVector<std::optional<DateTime>>(target_);
    auto local_dates = MakeReservedVector<std::optional<LocalDate>>(target_);
    auto local_times = MakeReservedVector<std::optional<LocalTime>>(target_);

    auto date_time_start = DateTime::Parse("2001-03-01T12:34:56Z");

    for (int64_t i = 0; i != target_; ++i) {
      chars.emplace_back(static_cast<char>('a' + i));
      int8s.emplace_back(static_cast<int8_t>(i));
      int16s.emplace_back(static_cast<int16_t>(i));
      int32s.emplace_back(static_cast<int32_t>(i));
      int64s.emplace_back(static_cast<int64_t>(i));
      floats.emplace_back(static_cast<float>(i));
      doubles.emplace_back(static_cast<double>(i));
      bools.emplace_back((i % 2) == 0);
      strings.emplace_back(fmt::format("hello {}", i));
      date_times.emplace_back(DateTime::FromNanos(date_time_start.Nanos() + i));
      local_dates.emplace_back(LocalDate::Of(2001, 3, i + 1));
      local_times.emplace_back(LocalTime::Of(12, 34, 46 + i));
    }

    if (target_ == 0) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Target should not be 0"));
    }
    auto t2 = target_ / 2;
    // Set the middle element to the unset optional, which for the purposes of this test is
    // our representation of null.
    chars[t2] = {};
    int8s[t2] = {};
    int16s[t2] = {};
    int32s[t2] = {};
    int64s[t2] = {};
    floats[t2] = {};
    doubles[t2] = {};
    bools[t2] = {};
    strings[t2] = {};
    date_times[t2] = {};
    local_dates[t2] = {};
    local_times[t2] = {};

    TableMaker expected;
    expected.AddColumn("Chars", chars);
    expected.AddColumn("Bytes", int8s);
    expected.AddColumn("Shorts", int16s);
    expected.AddColumn("Ints", int32s);
    expected.AddColumn("Longs", int64s);
    expected.AddColumn("Floats", floats);
    expected.AddColumn("Doubles", doubles);
    expected.AddColumn("Bools", bools);
    expected.AddColumn("Strings", strings);
    expected.AddColumn("DateTimes", date_times);
    expected.AddColumn("LocalDates", local_dates);
    expected.AddColumn("LocalTimes", local_times);

    TableComparerForTests::Compare(expected, *current);

    NotifyDone();
  }

private:
  int64_t target_ = 0;
};

TEST_CASE("Ticking Table: all the data is eventually present", "[ticking]") {
  const int64_t target = 10;
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();

  auto table = tm.TimeTable("PT0:00:0.5")
      .Update({"II = (int)((ii * 7) % 10)",
          "Chars = II == 5 ? null : (char)(II + 'a')",
          "Bytes = II == 5 ? null : (byte)II",
          "Shorts = II == 5 ? null : (short)II",
          "Ints = II == 5 ? null : (int)II",
          "Longs = II == 5 ? null : (long)II",
          "Floats = II == 5 ? null : (float)II",
          "Doubles = II == 5 ? null : (double)II",
          "Bools = II == 5 ? null : ((II % 2) == 0)",
          "Strings = II == 5 ? null : (`hello ` + II)",
          "DateTimes = II == 5 ? null : ('2001-03-01T12:34:56Z' + II)",
          "LocalDates = ii == 5 ? null : '2001-03-01' + ((int)II * 'P1D')",
          "LocalTimes = ii == 5 ? null : '12:34:46'.plus((int)II * 'PT1S')"
      })
      .LastBy("II")
      .Sort(SortPair::Ascending("II"))
      .DropColumns({"II", "Timestamp"});

  auto callback = std::make_shared<WaitForPopulatedTableCallback>(target);
  auto cookie = table.Subscribe(callback);

  while (true) {
    auto [done, eptr] = callback->WaitForUpdate();
    if (done) {
      break;
    }
    if (eptr != nullptr) {
      std::rethrow_exception(eptr);
    }
  }

  table.Unsubscribe(std::move(cookie));
}

class WaitForGroupedTableCallback final : public CommonBase {
public:
  explicit WaitForGroupedTableCallback(size_t target) : target_(target) {}

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    const auto &current = update.Current();
    std::cout << "=== The Full Table ===\n"
        << current->Stream(true, true)
        << '\n';
    if (update.Current()->NumRows() < target_) {
      return;
    }

    TableMaker expected;
    expected.AddColumn<int64_t>("Key", {0, 1, 2});
    expected.AddColumn<std::vector<std::optional<char16_t>>>("Chars", {
        {'a', 'd', 'g', 'j'},
        {'b', 'e', 'h'},
        {'c', {}, 'i'}
    });
    expected.AddColumn<std::vector<std::optional<int8_t>>>("Bytes", {
        {0, 3, 6, 9},
        {1, 4, 7},
        {2, {}, 8}
    });
    expected.AddColumn<std::vector<std::optional<int16_t>>>("Shorts", {
        {0, 3, 6, 9},
        {1, 4, 7},
        {2, {}, 8}
    });
    expected.AddColumn<std::vector<std::optional<int32_t>>>("Ints", {
        {0, 3, 6, 9},
        {1, 4, 7},
        {2, {}, 8}
    });
    expected.AddColumn<std::vector<std::optional<int64_t>>>("Longs", {
        {0, 3, 6, 9},
        {1, 4, 7},
        {2, {}, 8}
    });
    expected.AddColumn<std::vector<std::optional<float>>>("Floats", {
        {0, 3, 6, 9},
        {1, 4, 7},
        {2, {}, 8}
    });
    expected.AddColumn<std::vector<std::optional<double>>>("Doubles", {
        {0, 3, 6, 9},
        {1, 4, 7},
        {2, {}, 8}
    });
    expected.AddColumn<std::vector<std::optional<bool>>>("Bools", {
        {true, false, true, false},
        {false, true, false},
        {true, {}, true}
    });
    expected.AddColumn<std::vector<std::optional<std::string>>>("Strings", {
        {"hello 0", "hello 3", "hello 6", "hello 9"},
        {"hello 1", "hello 4", "hello 7"},
        {"hello 2", {}, "hello 8"}
    });
    auto start = DateTime(2001, 3, 1, 12, 34, 56).Nanos();
    auto with_offset = [&start](int64_t offset) {
      return DateTime::FromNanos(start + offset);
    };
    expected.AddColumn<std::vector<std::optional<DateTime>>>("DateTimes", {
        {with_offset(0), with_offset(3), with_offset(6), with_offset(9)},
        {with_offset(1), with_offset(4), with_offset(7)},
        {with_offset(2), {}, with_offset(8)}
    });
    expected.AddColumn<std::vector<std::optional<LocalDate>>>("LocalDates", {
        {LocalDate::Of(2001, 3, 1), LocalDate::Of(2001, 3, 4), LocalDate::Of(2001, 3, 7), LocalDate::Of(2001, 3, 10)},
        {LocalDate::Of(2001, 3, 2), LocalDate::Of(2001, 3, 5), LocalDate::Of(2001, 3, 8)},
        {LocalDate::Of(2001, 3, 3), {}, LocalDate::Of(2001, 3, 9)},
    });
    expected.AddColumn<std::vector<std::optional<LocalTime>>>("LocalTimes", {
        {LocalTime::Of(12, 34, 46), LocalTime::Of(12, 34, 49), LocalTime::Of(12, 34, 52), LocalTime::Of(12, 34, 55)},
        {LocalTime::Of(12, 34, 47), LocalTime::Of(12, 34, 50), LocalTime::Of(12, 34, 53)},
        {LocalTime::Of(12, 34, 48), {}, LocalTime::Of(12, 34, 54)}
    });

    TableComparerForTests::Compare(expected, *update.Current());

    NotifyDone();
  }

private:
  size_t target_ = 0;
};

TEST_CASE("Ticking Table: Ticking grouped data", "[ticking]") {
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();

  auto table = tm.EmptyTable(10)
      .Select({"Key = (ii % 3)",
          "Chars = ii == 5 ? null : (char)(ii + 'a')",
          "Bytes = ii == 5 ? null : (byte)ii",
          "Shorts = ii == 5 ? null : (short)ii",
          "Ints = ii == 5 ? null : (int)ii",
          "Longs = ii == 5 ? null : (long)ii",
          "Floats = ii == 5 ? null : (float)ii",
          "Doubles = ii == 5 ? null : (double)ii",
          "Bools = ii == 5 ? null : ((ii % 2) == 0)",
          "Strings = ii == 5 ? null : (`hello ` + ii)",
          "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii",
          "LocalDates = ii == 5 ? null : '2001-03-01' + ((int)ii * 'P1D')",
          "LocalTimes = ii == 5 ? null : '12:34:46'.plus((int)ii * 'PT1S')"
      })
      .By("Key");

  constexpr const int kNumTicks = 1;

  auto callback = std::make_shared<WaitForGroupedTableCallback>(kNumTicks);
  auto cookie = table.Subscribe(callback);

  while (true) {
    auto [done, eptr] = callback->WaitForUpdate();
    if (done) {
      break;
    }
    if (eptr != nullptr) {
      std::rethrow_exception(eptr);
    }
  }

  table.Unsubscribe(std::move(cookie));
}
}  // namespace deephaven::client::tests
