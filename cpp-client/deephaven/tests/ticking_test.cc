/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <condition_variable>
#include <iostream>
#include <mutex>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::chunk::Int64Chunk;

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
  table.BindToVariable("ticking");
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
}  // namespace deephaven::client::tests
