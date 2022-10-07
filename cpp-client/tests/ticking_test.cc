/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <arrow/flight/client.h>
#include <arrow/flight/client_auth.h>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/client/utility/utility.h"

#include <iostream>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compare.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

using deephaven::client::chunk::Int64Chunk;
using deephaven::client::Client;
using deephaven::client::NumCol;
using deephaven::client::StrCol;
using deephaven::client::TableHandle;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
class CommonBase : public deephaven::client::TickingCallback {
public:
  void onFailure(std::exception_ptr ep) final {
    std::unique_lock guard(mutex_);
    exceptionPtr_ = std::move(ep);
    condVar_.notify_all();
  }

  std::pair<bool, std::exception_ptr> waitForUpdate() {
    std::unique_lock guard(mutex_);
    while (true) {
      if (done_ || exceptionPtr_ != nullptr) {
        return std::make_pair(done_, exceptionPtr_);
      }
      condVar_.wait(guard);
    }
  }

protected:
  void notifyDone() {
    std::unique_lock guard(mutex_);
    done_ = true;
    condVar_.notify_all();
  }

  std::mutex mutex_;
  std::condition_variable condVar_;
  bool done_ = false;
  std::exception_ptr exceptionPtr_;

};

class ReachesNRowsCallback final : public CommonBase {
public:
  explicit ReachesNRowsCallback(size_t targetRows) : targetRows_(targetRows) {}

  void onTick(deephaven::client::TickingUpdate update) final {
    std::cout << "=== The Full Table ===\n"
              << update.current()->stream(true, true)
              << '\n';
    if (update.current()->numRows() >= targetRows_) {
      notifyDone();
    }
  }

private:
  size_t targetRows_ = 0;
};

TEST_CASE("Ticking table eventually reaches 10 rows", "[ticking]") {
  const auto maxRows = 10;
  auto client = TableMakerForTests::createClient();
  auto tm = client.getManager();

  auto table = tm.timeTable(0, 500'000'000).update("II = ii");
  table.bindToVariable("ticking");
  auto callback = std::make_shared<ReachesNRowsCallback>(maxRows);
  auto cookie = table.subscribe(callback);

  while (true) {
    auto [done, eptr] = callback->waitForUpdate();
    if (done) {
      break;
    }
    if (eptr != nullptr) {
      std::rethrow_exception(eptr);
    }
  }

  table.unsubscribe(std::move(cookie));
}

class AllValuesGreaterThanNCallback final : public CommonBase {
public:
  explicit AllValuesGreaterThanNCallback(int64_t target) : target_(target) {}

  void onTick(deephaven::client::TickingUpdate update) final {
    const auto &current = update.current();
    std::cout << "=== The Full Table ===\n"
              << current->stream(true, true)
              << '\n';
    if (current->numRows() == 0) {
      return;
    }
    auto col = current->getColumn("Value", true);
    auto rs = current->getRowSequence();
    auto data = Int64Chunk::create(rs->size());
    col->fillChunk(*rs, &data, nullptr);
    auto allGreater = true;
    for (auto element : data) {
      if (element < target_) {
        allGreater = false;
        break;
      }
    }
    if (allGreater) {
      notifyDone();
    }
  }

private:
  int64_t target_ = 0;
};

TEST_CASE("Ticking table modified rows are eventually all greater than 10", "[ticking]") {
  const int64_t target = 10;
  auto client = TableMakerForTests::createClient();
  auto tm = client.getManager();

  auto table = tm.timeTable(0, 500'000'000)
      .view({"Key = (long)(ii % 10)", "Value = ii"})
      .lastBy("Key");
  table.bindToVariable("ticking");
  auto callback = std::make_shared<AllValuesGreaterThanNCallback>(target);
  auto cookie = table.subscribe(callback);

  while (true) {
    auto [done, eptr] = callback->waitForUpdate();
    if (done) {
      break;
    }
    if (eptr != nullptr) {
      std::rethrow_exception(eptr);
    }
  }

  table.unsubscribe(std::move(cookie));
}
}  // namespace deephaven::client::tests
