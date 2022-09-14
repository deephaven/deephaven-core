/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <charconv>
#include <exception>
#include <iostream>
#include <sstream>
#include "deephaven/client/client.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;

namespace {
void mainMenu();

// demo options
void printStaticTable(const TableHandleManager &manager);
void withArrow(const TableHandleManager &manager);
void interactiveWhereClause1(const TableHandleManager &manager);
void interactiveWhereClause2(const TableHandleManager &manager);

// utilities
void printTable(const TableHandle &table, bool nullAware);
void checkNotNull(const void *p, std::string_view where);
int64_t readNumber(std::istream &s);

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
    while (true) {
      mainMenu();
    }
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void mainMenu() {
  std::cout << "*** CHAPTER 1: MAIN MENU ***\n"
               "1 - printStaticTable\n"
               "2 - withArrow\n"
               "3 - interactiveWhereClause1\n"
               "4 - interactiveWhereClause2\n"
               "\n"
               "Please select 1-4: ";

  auto selection = readNumber(std::cin);

  const char *server = "localhost:10000";
  auto client = Client::connect(server);
  auto manager = client.getManager();

  switch (selection) {
    case 1:
      printStaticTable(manager);
      break;

    case 2:
      withArrow(manager);
      break;

    case 3:
      interactiveWhereClause1(manager);
      break;

    case 4:
      interactiveWhereClause2(manager);
      break;

    default:
      std::cout << "Invalid selection\n";
  }
}

void printStaticTable(const TableHandleManager &manager) {
  auto table = manager.fetchTable("demo1");
  std::cout << table.stream(true) << std::endl;
}

void withArrow(const TableHandleManager &manager) {
  auto table = manager.fetchTable("demo1");

  std::cout << "Do you want the null-aware version? [y/n]";
  std::string response;
  std::getline(std::cin, response);

  auto nullAware = !response.empty() && response[0] == 'y';
  printTable(table, nullAware);
}

void interactiveWhereClause1(const TableHandleManager &manager) {
  std::cout << "Enter limit: ";
  auto limit = readNumber(std::cin);

  std::ostringstream whereClause;
  whereClause << "Int64Value < " << limit;
  std::cout << "NOTE: 'where' expression is " << whereClause.str() << '\n';

  auto table = manager.fetchTable("demo1");
  table = table.where(whereClause.str());
  printTable(table, true);
}

void interactiveWhereClause2(const TableHandleManager &manager) {
  std::cout << "Enter limit: ";
  auto limit = readNumber(std::cin);

  auto table = manager.fetchTable("demo1");
  auto intCol = table.getNumCol("Int64Value");
  table = table.where(intCol < limit);
  printTable(table, true);
}

void printTable(const TableHandle &table, bool nullAware) {
  auto fsr = table.getFlightStreamReader();

  while (true) {
    arrow::flight::FlightStreamChunk chunk;
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->Next(&chunk)));
    if (chunk.data == nullptr) {
      break;
    }

    auto int64Data = chunk.data->GetColumnByName("Int64Value");
    checkNotNull(int64Data.get(), DEEPHAVEN_DEBUG_MSG("Int64Value column not found"));
    auto doubleData = chunk.data->GetColumnByName("DoubleValue");
    checkNotNull(doubleData.get(), DEEPHAVEN_DEBUG_MSG("DoubleValue column not found"));

    auto int64Array = std::dynamic_pointer_cast<arrow::Int64Array>(int64Data);
    checkNotNull(int64Array.get(), DEEPHAVEN_DEBUG_MSG("intData was not an arrow::Int64Array"));
    auto doubleArray = std::dynamic_pointer_cast<arrow::DoubleArray>(doubleData);
    checkNotNull(doubleArray.get(), DEEPHAVEN_DEBUG_MSG("doubleData was not an arrow::DoubleArray"));

    if (int64Array->length() != doubleArray->length()) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Lengths differ"));
    }

    if (!nullAware) {
      for (int64_t i = 0; i < int64Array->length(); ++i) {
        auto int64Value = int64Array->Value(i);
        auto doubleValue = doubleArray->Value(i);
        std::cout << int64Value << ' ' << doubleValue << '\n';
      }
    } else {
      for (int64_t i = 0; i < int64Array->length(); ++i) {
        // We use the indexing operator (operator[]) which returns an arrow "optional<T>" type,
        // which is a workalike to std::optional<T>.
        auto int64Value = (*int64Array)[i];
        auto doubleValue = (*doubleArray)[i];
        // We use our own "ostream adaptor" to provide a handy way to write these arrow optionals
        // to an ostream.
        std::cout << adaptOptional(int64Value) << ' ' << adaptOptional(doubleValue) << '\n';
      }
    }
  }
}

void checkNotNull(const void *p, std::string_view where) {
  if (p != nullptr) {
    return;
  }
  throw std::runtime_error(std::string(where));
}

int64_t readNumber(std::istream &s) {
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
