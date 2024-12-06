/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <condition_variable>
#include <iostream>
#include <mutex>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
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

    CompareColumn(*current, "Chars", chars);
    CompareColumn(*current, "Bytes", int8s);
    CompareColumn(*current, "Shorts", int16s);
    CompareColumn(*current, "Ints", int32s);
    CompareColumn(*current, "Longs", int64s);
    CompareColumn(*current, "Floats", floats);
    CompareColumn(*current, "Doubles", doubles);
    CompareColumn(*current, "Bools", bools);
    CompareColumn(*current, "Strings", strings);
    CompareColumn(*current, "DateTimes", date_times);
    CompareColumn(*current, "LocalDates", local_dates);
    CompareColumn(*current, "LocalTimes", local_times);

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
      .Sort(SortPair::Ascending("II"));

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
}  // namespace deephaven::client::tests
