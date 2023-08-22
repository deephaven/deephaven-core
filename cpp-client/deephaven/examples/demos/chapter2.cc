/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <charconv>
#include <exception>
#include <iostream>
#include <sstream>
#include "deephaven/client/client.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::column::Int64ColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::utility::VerboseCast;

namespace {
void MainMenu(const TableHandleManager &manager);

// demo options
void PrintDynamicTableFull(const TableHandleManager &manager);
void PrintDiffs(const TableHandleManager &manager);
void SumColumnsFull(const TableHandleManager &manager);
void SumColumnsDiff(const TableHandleManager &manager);

// utilities
int64_t ReadNumber(std::string_view prompt);
std::string ReadString(std::string_view prompt);

// a bunch of work to create an ostream adaptor
template<typename ArrowOptional>
class OptionalAdaptor {
public:
  explicit OptionalAdaptor(const ArrowOptional &optional) : optional_(optional) {}

private:
  const ArrowOptional &optional_;

  friend std::ostream &operator<<(std::ostream &s, const OptionalAdaptor &self) {
    if (!self.optional_.has_value()) {
      return s << "null";
    }
    return s << *self.optional_;
  }
};

template<typename ArrowOptional>
inline OptionalAdaptor<ArrowOptional> AdaptOptional(const ArrowOptional &optional) {
  return OptionalAdaptor<ArrowOptional>(optional);
}
}  // namespace

int main(int argc, char *argv[]) {
  try {
    const char *server = "localhost:10000";
    if (argc > 1) {
      if (argc != 2 || std::strcmp("-h", argv[1]) == 0) {
        std::cerr << "Usage: " << argv[0] << " [host:port]\n";
        std::exit(1);
      }
      server = argv[1];
    }
    auto client = Client::Connect(server);
    auto manager = client.GetManager();

    while (true) {
      MainMenu(manager);
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void MainMenu(const TableHandleManager &manager) {
  auto selection = ReadNumber(
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
      PrintDynamicTableFull(manager);
      break;

    case 2:
      PrintDiffs(manager);
      break;

    case 3:
      SumColumnsFull(manager);
      break;

    case 4:
      SumColumnsDiff(manager);
      break;

    default:
      std::cout << "Invalid selection\n";
  }
}

class CallbackPrintFull final : public deephaven::dhcore::ticking::TickingCallback {
public:
  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    std::cout << "=== The Full Table ===\n"
      << update.Current()->Stream(true, true)
      << '\n';
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }
};

void PrintDynamicTableFull(const TableHandleManager &manager) {
  auto table_name = ReadString("Please enter the table name: ");

  auto table = manager.FetchTable(table_name);
  auto callback = std::make_shared<CallbackPrintFull>();
  auto cookie = table.Subscribe(std::move(callback));
  auto dummy = ReadString("Press enter to unsubscribe: ");
  table.Unsubscribe(std::move(cookie));
}

class CallbackPrintDiff final : public deephaven::dhcore::ticking::TickingCallback {
public:
  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    if (update.BeforeRemoves() != update.AfterRemoves()) {
      std::cout << "=== REMOVES ===\n"
                << update.BeforeRemoves()->Stream(true, true, update.RemovedRows())
                << '\n';
    }
    if (update.BeforeAdds() != update.AfterAdds()) {
      std::cout << "=== ADDS ===\n"
                << update.AfterAdds()->Stream(true, true, update.AddedRows())
                << '\n';
    }
    if (update.BeforeModifies() != update.AfterModifies()) {
      std::cout << "=== MODIFIES (BEFORE) ===\n"
                << update.BeforeModifies()->Stream(true, true, update.ModifiedRows())
                << '\n';
      std::cout << "=== MODIFIES (AFTER) ===\n"
                << update.AfterModifies()->Stream(true, true, update.ModifiedRows())
                << '\n';
    }
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }
};

void PrintDiffs(const TableHandleManager &manager) {
  auto table_name = ReadString("Please enter the table name: ");

  auto table = manager.FetchTable(table_name);
  auto callback = std::make_shared<CallbackPrintDiff>();
  auto cookie = table.Subscribe(std::move(callback));
  auto dummy = ReadString("Press enter to unsubscribe: ");
  table.Unsubscribe(std::move(cookie));
}

class CallbackSumFull final : public deephaven::dhcore::ticking::TickingCallback {
public:
  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    const auto &current = update.Current();
    auto int64_col_generic = current->GetColumn("IntValue", true);

    const auto *typed_int64_col =
        VerboseCast<const Int64ColumnSource*>(DEEPHAVEN_EXPR_MSG(int64_col_generic.get()));

    auto rows = current->GetRowSequence();

    constexpr const size_t kChunkSize = 8192;
    auto data_chunk = Int64Chunk::Create(kChunkSize);

    int64_t int64_sum = 0;
    while (!rows->Empty()) {
      auto these_rows = rows->Take(kChunkSize);
      auto these_rows_size = these_rows->Size();
      rows = rows->Drop(kChunkSize);

      typed_int64_col->FillChunk(*these_rows, &data_chunk, nullptr);

      for (size_t i = 0; i < these_rows_size; ++i) {
        int64_sum += data_chunk.data()[i];
        ++numOps_;
      }
    }
    std::cout << "int64Sum is " << int64_sum << ", cumulative number of ops is " << numOps_ << '\n';
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

private:
  size_t numOps_ = 0;
};

void SumColumnsFull(const TableHandleManager &manager) {
  auto table_name = ReadString("Please enter the table name: ");

  auto table = manager.FetchTable(table_name);
  auto callback = std::make_shared<CallbackSumFull>();
  auto cookie = table.Subscribe(std::move(callback));
  auto dummy = ReadString("Press enter to unsubscribe: ");
  table.Unsubscribe(std::move(cookie));
}

class CallbackSumDiff final : public deephaven::dhcore::ticking::TickingCallback {
public:
  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    ProcessDeltas(*update.BeforeRemoves(), update.RemovedRows(), -1);
    ProcessDeltas(*update.AfterAdds(), update.AddedRows(), 1);
    std::cout << "int64Sum is " << int64_sum_ << ", cumulative number of ops is " << num_ops_ << '\n';
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

private:
  void ProcessDeltas(const ClientTable &table, std::shared_ptr<RowSequence> rows, int64_t parity) {
    auto int64_col_generic = table.GetColumn("IntValue", true);

    const auto *typed_int64_col =
        VerboseCast<const Int64ColumnSource*>(DEEPHAVEN_EXPR_MSG(int64_col_generic.get()));

    size_t chunk_size = 8192;
    auto data_chunk = Int64Chunk::Create(chunk_size);

    while (!rows->Empty()) {
      auto these_rows = rows->Take(chunk_size);
      auto these_rows_size = these_rows->Size();
      rows = rows->Drop(chunk_size);

      typed_int64_col->FillChunk(*these_rows, &data_chunk, nullptr);

      for (size_t i = 0; i < these_rows_size; ++i) {
        int64_sum_ += data_chunk.data()[i] * parity;
        ++num_ops_;
      }
    }
  }

  int64_t int64_sum_ = 0;
  size_t num_ops_ = 0;
};

void SumColumnsDiff(const TableHandleManager &manager) {
  auto table_name = ReadString("Please enter the table name: ");

  auto table = manager.FetchTable(table_name);
  auto callback = std::make_shared<CallbackSumDiff>();
  auto cookie = table.Subscribe(std::move(callback));
  auto dummy = ReadString("Press enter to unsubscribe: ");
  table.Unsubscribe(std::move(cookie));
}

std::string ReadString(std::string_view prompt) {
  std::cout << prompt;
  std::string line;
  std::getline(std::cin, line);
  return line;
}

int64_t ReadNumber(std::string_view prompt) {
  while (true) {
    auto line = ReadString(prompt);
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
