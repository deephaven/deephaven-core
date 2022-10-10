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
void printFull(const TableHandleManager &manager);
void printDiffs(const TableHandleManager &manager);
void sumDiffs(const TableHandleManager &manager);

// utilities
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
      "*** CHAPTER 3: MAIN MENU ***\n"
      "1 - print full table\n"
      "2 - print diffs with library\n"
      "3 - process diffs\n"
      "\n"
      "Please select 1-3: "
  );

  switch (selection) {
    case 1:
      printFull(manager);
      break;

    case 2:
      printDiffs(manager);
      break;

    case 3:
      sumDiffs(manager);
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

void printFull(const TableHandleManager &manager) {
  auto tableName = readString("Please enter the table name: ");

  auto table = manager.fetchTable(tableName);
  auto callback = std::make_shared<CallbackPrintFull>();
  auto cookie = table.subscribe(std::move(callback));
  auto dummy = readString("Press enter to unsubscribe: ");
  table.unsubscribe(std::move(cookie));
}

class CallbackPrintDiffs final : public deephaven::client::TickingCallback {
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
  auto callback = std::make_shared<CallbackPrintDiffs>();
  auto cookie = table.subscribe(std::move(callback));
  auto dummy = readString("Press enter to unsubscribe: ");
  table.unsubscribe(std::move(cookie));
}

class CallbackSumDiffs final : public deephaven::client::TickingCallback {
public:
  void onTick(deephaven::client::TickingUpdate update) final {
    auto colIndex = update.current()->getColumnIndex("IntValue", true);

    processDeltas(*update.beforeRemoves(), colIndex, update.removedRows(), -1);
    processDeltas(*update.afterAdds(), colIndex, update.addedRows(), 1);

    size_t numModifies = 0;
    if (update.modifiedRows().size() > colIndex) {
      auto rowSequence = update.modifiedRows()[colIndex];
      processDeltas(*update.beforeModifies(), colIndex, rowSequence, -1);
      processDeltas(*update.afterModifies(), colIndex, rowSequence, 1);

      numModifies += rowSequence->size();
    }
    std::cout << "int64Sum is " << int64Sum_
      << ", adds=" << update.addedRows()->size()
      << ", removes=" << update.removedRows()->size()
      << ", modifies=" << numModifies
      << '\n';
  }

  void onFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

private:
  void processDeltas(const Table &table, size_t colIndex, std::shared_ptr<RowSequence> rows,
      int64_t parity) {
    const auto int64ColGeneric = table.getColumn(colIndex);
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
      }
    }
  }

private:
  int64_t int64Sum_ = 0;
};

void sumDiffs(const TableHandleManager &manager) {
  auto tableName = readString("Please enter the table name: ");

  auto table = manager.fetchTable(tableName);

  auto callback = std::make_shared<CallbackSumDiffs>();
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
