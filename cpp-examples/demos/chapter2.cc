/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <charconv>
#include <exception>
#include <iostream>
#include <sstream>
#include "deephaven/client/client.h"
#include "deephaven/client/chunk/chunk.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::chunk::Int64Chunk;
using deephaven::client::column::Int64ColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::table::Table;
using deephaven::client::utility::verboseCast;

namespace {
void mainMenu(const TableHandleManager &manager);

// demo options
void printDynamicTableFull(const TableHandleManager &manager);
void printDiffs(const TableHandleManager &manager);
void sumColumnsFull(const TableHandleManager &manager);
void sumColumnsDiff(const TableHandleManager &manager);

// utilities
void printTable(const TableHandle &table, bool nullAware);
void checkNotNull(const void *p, std::string_view where);
int64_t readNumber(std::string_view prompt);
std::string readString(std::string_view prompt);

// a bunch of work to create an ostream adaptor
template<typename ARROW_OPTIONAL>
class OptionalAdaptor {
public:
  explicit OptionalAdaptor(const ARROW_OPTIONAL &optional) : optional_(optional) {}

private:
  const ARROW_OPTIONAL &optional_;

  friend std::ostream &operator<<(std::ostream &s, const OptionalAdaptor &self) {
    if (!self.optional_.has_value()) {
      return s << "null";
    }
    return s << *self.optional_;
  }
};

template<typename ARROW_OPTIONAL>
inline OptionalAdaptor<ARROW_OPTIONAL> adaptOptional(const ARROW_OPTIONAL &optional) {
  return OptionalAdaptor<ARROW_OPTIONAL>(optional);
}
}  // namespace

int main() {
  try {
    const char *server = "localhost:10000";
    auto client = Client::connect(server);
    auto manager = client.getManager();

    while (true) {
      mainMenu(manager);
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void mainMenu(const TableHandleManager &manager) {
  auto selection = readNumber(
      "*** CHAPTER 2: MAIN MENU ***\n"
      "1 - print full table\n"
      "2 - print diffs\n"
      "3 - sum column - full\n"
      "4 - sum column - using diffs\n"
      "\n"
      "Please select 1-4: "
  );

  switch (selection) {
    case 1:
      printDynamicTableFull(manager);
      break;

    case 2:
      printDiffs(manager);
      break;

    case 3:
      sumColumnsFull(manager);
      break;

    case 4:
      sumColumnsDiff(manager);
      break;

    default:
      std::cout << "Invalid selection\n";
  }
}

class CallbackPrintFull final : public deephaven::client::TickingCallback {
public:
  void onTick(deephaven::client::TickingUpdate update) final {
    std::cout << "=== The Full Table ===\n"
      << update.current()->stream(true, true)
      << '\n';
  }

  void onFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }
};

void printDynamicTableFull(const TableHandleManager &manager) {
  auto tableName = readString("Please enter the table name: ");

  auto table = manager.fetchTable(tableName);
  auto callback = std::make_shared<CallbackPrintFull>();
  auto cookie = table.subscribe(std::move(callback));
  auto dummy = readString("Press enter to unsubscribe: ");
  table.unsubscribe(std::move(cookie));
}

class CallbackPrintDiff final : public deephaven::client::TickingCallback {
public:
  void onTick(deephaven::client::TickingUpdate update) final {
    if (update.beforeRemoves() != update.afterRemoves()) {
      std::cout << "=== REMOVES ===\n"
                << update.beforeRemoves()->stream(true, true, update.removedRows())
                << '\n';
    }
    if (update.beforeAdds() != update.afterAdds()) {
      std::cout << "=== ADDS ===\n"
                << update.afterAdds()->stream(true, true, update.addedRows())
                << '\n';
    }
    if (update.beforeModifies() != update.afterModifies()) {
      std::cout << "=== MODIFIES (BEFORE) ===\n"
                << update.beforeModifies()->stream(true, true, update.modifiedRows())
                << '\n';
      std::cout << "=== MODIFIES (AFTER) ===\n"
                << update.afterModifies()->stream(true, true, update.modifiedRows())
                << '\n';
    }
  }

  void onFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }
};

void printDiffs(const TableHandleManager &manager) {
  auto tableName = readString("Please enter the table name: ");

  auto table = manager.fetchTable(tableName);
  auto callback = std::make_shared<CallbackPrintDiff>();
  auto cookie = table.subscribe(std::move(callback));
  auto dummy = readString("Press enter to unsubscribe: ");
  table.unsubscribe(std::move(cookie));
}

class CallbackSumFull final : public deephaven::client::TickingCallback {
public:
  void onTick(deephaven::client::TickingUpdate update) final {
    const auto &current = update.current();
    auto int64ColGeneric = current->getColumn("IntValue", true);

    const auto *typedInt64Col =
        verboseCast<const Int64ColumnSource*>(DEEPHAVEN_EXPR_MSG(int64ColGeneric.get()));

    auto rows = current->getRowSequence();

    size_t chunkSize = 8192;
    auto dataChunk = Int64Chunk::create(chunkSize);

    int64_t int64Sum = 0;
    while (!rows->empty()) {
      auto theseRows = rows->take(chunkSize);
      auto theseRowsSize = theseRows->size();
      rows = rows->drop(chunkSize);

      typedInt64Col->fillChunk(*theseRows, &dataChunk, nullptr);

      for (size_t i = 0; i < theseRowsSize; ++i) {
        int64Sum += dataChunk.data()[i];
        ++numOps_;
      }
    }
    std::cout << "int64Sum is " << int64Sum << ", cumulative number of ops is " << numOps_ << '\n';
  }

  void onFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

private:
  size_t numOps_ = 0;
};

void sumColumnsFull(const TableHandleManager &manager) {
  auto tableName = readString("Please enter the table name: ");

  auto table = manager.fetchTable(tableName);
  auto callback = std::make_shared<CallbackSumFull>();
  auto cookie = table.subscribe(std::move(callback));
  auto dummy = readString("Press enter to unsubscribe: ");
  table.unsubscribe(std::move(cookie));
}

class CallbackSumDiff final : public deephaven::client::TickingCallback {
public:
  void onTick(deephaven::client::TickingUpdate update) final {
    processDeltas(*update.beforeRemoves(), update.removedRows(), -1);
    processDeltas(*update.afterAdds(), update.addedRows(), 1);
    std::cout << "int64Sum is " << int64Sum_ << ", cumulative number of ops is " << numOps_ << '\n';
  }

  void onFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

private:
  void processDeltas(const Table &table, std::shared_ptr<RowSequence> rows, int64_t parity) {
    auto int64ColGeneric = table.getColumn("IntValue", true);

    const auto *typedInt64Col =
        verboseCast<const Int64ColumnSource*>(DEEPHAVEN_EXPR_MSG(int64ColGeneric.get()));

    size_t chunkSize = 8192;
    auto dataChunk = Int64Chunk::create(chunkSize);

    while (!rows->empty()) {
      auto theseRows = rows->take(chunkSize);
      auto theseRowsSize = theseRows->size();
      rows = rows->drop(chunkSize);

      typedInt64Col->fillChunk(*theseRows, &dataChunk, nullptr);

      for (size_t i = 0; i < theseRowsSize; ++i) {
        int64Sum_ += dataChunk.data()[i] * parity;
        ++numOps_;
      }
    }
  }

private:
  int64_t int64Sum_ = 0;
  size_t numOps_ = 0;
};

void sumColumnsDiff(const TableHandleManager &manager) {
  auto tableName = readString("Please enter the table name: ");

  auto table = manager.fetchTable(tableName);
  auto callback = std::make_shared<CallbackSumDiff>();
  auto cookie = table.subscribe(std::move(callback));
  auto dummy = readString("Press enter to unsubscribe: ");
  table.unsubscribe(std::move(cookie));
}

std::string readString(std::string_view prompt) {
  std::cout << prompt;
  std::string line;
  std::getline(std::cin, line);
  return line;
}

int64_t readNumber(std::string_view prompt) {
  while (true) {
    auto line = readString(prompt);
    if (line.empty()) {
      continue;
    }
    int64_t result;
    auto [ptr, ec] = std::from_chars(line.data(), line.data() + line.size(), result);
    if (ec == std::errc()) {
      return result;
    }
    std::cout << "Input not parseable as int64. Please try again\n";
  }
}
}  // namespace
