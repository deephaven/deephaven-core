/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <charconv>
#include <exception>
#include <iostream>
#include <sstream>
#include <arrow/array.h>
#include <arrow/flight/client.h>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/arrow_util.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::OkOrThrow;

namespace {
void MainMenu(const TableHandleManager &manager);

// demo options
void PrintStaticTable(const TableHandleManager &manager);
void WithArrow(const TableHandleManager &manager);
void InteractiveWhereClause1(const TableHandleManager &manager);

// utilities
void PrintTable(const TableHandle &table, bool null_aware);
void CheckNotNull(const void *p, std::string_view where);
int64_t ReadNumber(std::istream &s);

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

  try {
    while (true) {
      MainMenu(manager);
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void MainMenu(const TableHandleManager &manager) {
  std::cout << "*** CHAPTER 1: MAIN MENU ***\n"
               "1 - PrintStaticTable\n"
               "2 - WithArrow\n"
               "3 - InteractiveWhereClause1\n"
               "\n"
               "Please select 1-3: ";

  auto selection = ReadNumber(std::cin);

  switch (selection) {
    case 1:
      PrintStaticTable(manager);
      break;

    case 2:
      WithArrow(manager);
      break;

    case 3:
      InteractiveWhereClause1(manager);
      break;

    default:
      std::cout << "Invalid selection\n";
  }
}

void PrintStaticTable(const TableHandleManager &manager) {
  auto table = manager.FetchTable("demo1");
  std::cout << table.Stream(true) << '\n';
}

void WithArrow(const TableHandleManager &manager) {
  auto table = manager.FetchTable("demo1");

  std::cout << "Do you want the null-aware version? [y/n]";
  std::string response;
  std::getline(std::cin, response);

  auto null_aware = !response.empty() && response[0] == 'y';
  PrintTable(table, null_aware);
}

void InteractiveWhereClause1(const TableHandleManager &manager) {
  std::cout << "Enter limit: ";
  auto limit = ReadNumber(std::cin);

  std::ostringstream where_clause;
  where_clause << "Int64Value < " << limit;
  std::cout << "NOTE: 'where' expression is " << where_clause.str() << '\n';

  auto table = manager.FetchTable("demo1");
  table = table.Where(where_clause.str());
  PrintTable(table, true);
}

void PrintTable(const TableHandle &table, bool null_aware) {
  auto fsr = table.GetFlightStreamReader();

  while (true) {
    auto chunk = fsr->Next();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(chunk));
    if (chunk->data == nullptr) {
      break;
    }

    auto int64_data = chunk->data->GetColumnByName("Int64Value");
    CheckNotNull(int64_data.get(), DEEPHAVEN_LOCATION_STR("Int64Value column not found"));
    auto double_data = chunk->data->GetColumnByName("DoubleValue");
    CheckNotNull(double_data.get(), DEEPHAVEN_LOCATION_STR("DoubleValue column not found"));

    auto int64_array = std::dynamic_pointer_cast<arrow::Int64Array>(int64_data);
    CheckNotNull(int64_array.get(), DEEPHAVEN_LOCATION_STR("intData was not an arrow::Int64Array"));
    auto double_array = std::dynamic_pointer_cast<arrow::DoubleArray>(double_data);
    CheckNotNull(double_array.get(), DEEPHAVEN_LOCATION_STR("doubleData was not an arrow::DoubleArray"));

    if (int64_array->length() != double_array->length()) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Lengths differ"));
    }

    if (!null_aware) {
      for (int64_t i = 0; i < int64_array->length(); ++i) {
        auto int64_value = int64_array->Value(i);
        auto double_value = double_array->Value(i);
        std::cout << int64_value << ' ' << double_value << '\n';
      }
    } else {
      for (int64_t i = 0; i < int64_array->length(); ++i) {
        // We use the indexing operator (operator[]) which returns an arrow "optional<T>" type,
        // which is a workalike to std::optional<T>.
        auto int64_value = (*int64_array)[i];
        auto double_value = (*double_array)[i];
        // We use our own "ostream adaptor" to provide a handy way to write these arrow optionals
        // to an ostream.
        std::cout << AdaptOptional(int64_value) << ' ' << AdaptOptional(double_value) << '\n';
      }
    }
  }
}

void CheckNotNull(const void *p, std::string_view where) {
  if (p != nullptr) {
    return;
  }
  throw std::runtime_error(std::string(where));
}

int64_t ReadNumber(std::istream &s) {
  while (true) {
    std::string line;
    std::getline(s, line);
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
