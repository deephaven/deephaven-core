/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <condition_variable>
#include <iostream>
#include <mutex>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DateTime;
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

TEST_CASE("Ticking Table eventually reaches 10 rows", "[ticking]") {
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

TEST_CASE("Ticking Table modified rows are eventually all greater than 10", "[ticking]") {
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

    auto int8s = MakeReservedVector<int8_t>(target_);
    auto int16s = MakeReservedVector<int16_t>(target_);
    auto int32s = MakeReservedVector<int32_t>(target_);
    auto int64s = MakeReservedVector<int64_t>(target_);
    auto floats = MakeReservedVector<float>(target_);
    auto doubles = MakeReservedVector<double>(target_);
    auto bools = MakeReservedVector<bool>(target_);
    auto strings = MakeReservedVector<std::string>(target_);
    auto date_times = MakeReservedVector<DateTime>(target_);

    auto date_time_start = DateTime::Parse("2001-03-01T12:34:56Z");

    for (int64_t i = 0; i != target_; ++i) {
      int8s.push_back(i);
      int16s.push_back(i);
      int32s.push_back(i);
      int64s.push_back(i);
      floats.push_back(i);
      doubles.push_back(i);
      bools.push_back((i % 2) == 0);
      strings.push_back(fmt::format("hello {}", i));
      date_times.push_back(DateTime::FromNanos(date_time_start.Nanos() + i));
    }

    CompareColumn<Int8Chunk>(*current, "Bytes", int8s);
    CompareColumn<Int16Chunk>(*current, "Shorts", int16s);
    CompareColumn<Int32Chunk>(*current, "Ints", int32s);
    CompareColumn<Int64Chunk>(*current, "Longs", int64s);
    CompareColumn<FloatChunk>(*current, "Floats", floats);
    CompareColumn<DoubleChunk>(*current, "Doubles", doubles);
    CompareColumn<StringChunk>(*current, "Strings", strings);
    CompareColumn<BooleanChunk>(*current, "Bools", bools);
    CompareColumn<DateTimeChunk>(*current, "DateTimes", date_times);

    NotifyDone();
  }

  template<typename ChunkType, typename T>
  void CompareColumn(const ClientTable &table, std::string_view column_name,
      const std::vector<T> &expected) {
    static_assert(std::is_same_v<typename ChunkType::value_type, T>);
    if (expected.size() != table.NumRows()) {
      auto message = fmt::format("Expected 'expected' to have size {}, have {}",
          table.NumRows(), expected.size());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
    auto cs = table.GetColumn(column_name, true);
    auto rs = RowSequence::CreateSequential(0, table.NumRows());
    auto chunk = ChunkType::Create(table.NumRows());
    cs->FillChunk(*rs, &chunk, nullptr);

    for (size_t row_num = 0; row_num != expected.size(); ++row_num) {
      const auto &expected_elt = expected[row_num];
      const auto &actual_elt = chunk.data()[row_num];
      if (expected_elt != actual_elt) {
        auto message = fmt::format(R"(In column "{}", row {}, expected={}, actual={})",
            column_name, row_num, expected_elt, actual_elt);
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }
    }
  }

private:
  int64_t target_ = 0;
};

TEST_CASE("Ticking Table all the data is eventually present", "[ticking]") {
  const int64_t target = 10;
  auto client = TableMakerForTests::CreateClient();
  auto tm = client.GetManager();

  auto table = tm.TimeTable("PT0:00:0.5")
      .Update({"II = (int)((ii * 7) % 10)",
          "Bytes = (byte)II",
          "Chars = (char)(II + 'a')",
          "Shorts = (short)II",
          "Ints = (int)II",
          "Longs = (long)II",
          "Floats = (float)II",
          "Doubles = (double)II",
          "Strings = `hello ` + II",
          "Bools = (II % 2) == 0",
          "DateTimes = '2001-03-01T12:34:56Z' + II"
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
