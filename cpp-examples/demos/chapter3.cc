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
void PrintFull(const TableHandleManager &manager);
void PrintDiffs(const TableHandleManager &manager);
void SumDiffs(const TableHandleManager &manager);

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
      "*** CHAPTER 3: MAIN MENU ***\n"
      "1 - print full table\n"
      "2 - print diffs with library\n"
      "3 - process diffs\n"
      "\n"
      "Please select 1-3: "
  );

  switch (selection) {
    case 1:
      PrintFull(manager);
      break;

    case 2:
      PrintDiffs(manager);
      break;

    case 3:
      SumDiffs(manager);
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

void PrintFull(const TableHandleManager &manager) {
  auto table_name = ReadString("Please enter the table name: ");

  auto table = manager.FetchTable(table_name);
  auto callback = std::make_shared<CallbackPrintFull>();
  auto cookie = table.Subscribe(std::move(callback));
  auto dummy = ReadString("Press enter to unsubscribe: ");
  table.Unsubscribe(std::move(cookie));
}

class CallbackPrintDiffs final : public deephaven::dhcore::ticking::TickingCallback {
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
  auto callback = std::make_shared<CallbackPrintDiffs>();
  auto cookie = table.Subscribe(std::move(callback));
  auto dummy = ReadString("Press enter to unsubscribe: ");
  table.Unsubscribe(std::move(cookie));
}

class CallbackSumDiffs final : public deephaven::dhcore::ticking::TickingCallback {
public:
  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    auto col_index = *update.Current()->GetColumnIndex("Int64Value", true);

    ProcessDeltas(*update.BeforeRemoves(), col_index, update.RemovedRows(), -1);
    ProcessDeltas(*update.AfterAdds(), col_index, update.AddedRows(), 1);

    size_t num_modifies = 0;
    if (update.ModifiedRows().size() > col_index) {
      auto row_sequence = update.ModifiedRows()[col_index];
      ProcessDeltas(*update.BeforeModifies(), col_index, row_sequence, -1);
      ProcessDeltas(*update.AfterModifies(), col_index, row_sequence, 1);

      num_modifies += row_sequence->Size();
    }
    std::cout << "int64Sum is " << int64_sum_
      << ", adds=" << update.AddedRows()->Size()
      << ", removes=" << update.RemovedRows()->Size()
      << ", modifies=" << num_modifies
      << '\n';
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

private:
  void ProcessDeltas(const ClientTable &table, size_t col_index, std::shared_ptr<RowSequence> rows,
      int64_t parity) {
    const auto int64_col_generic = table.GetColumn(col_index);
    const auto *typed_int64_col =
        VerboseCast<const Int64ColumnSource*>(DEEPHAVEN_EXPR_MSG(int64_col_generic.get()));

    constexpr const size_t kChunkSize = 8192;
    auto data_chunk = Int64Chunk::Create(kChunkSize);

    while (!rows->Empty()) {
      auto these_rows = rows->Take(kChunkSize);
      auto these_rows_size = these_rows->Size();
      rows = rows->Drop(kChunkSize);

      typed_int64_col->FillChunk(*these_rows, &data_chunk, nullptr);

      for (size_t i = 0; i < these_rows_size; ++i) {
        int64_sum_ += data_chunk.data()[i] * parity;
      }
    }
  }

  int64_t int64_sum_ = 0;
};

void SumDiffs(const TableHandleManager &manager) {
  auto table_name = ReadString("Please enter the table name: ");

  auto table = manager.FetchTable(table_name);

  auto callback = std::make_shared<CallbackSumDiffs>();
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
