/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::OkOrThrow;
using deephaven::client::utility::TableMaker;

namespace {
TableHandle MakeTable(const TableHandleManager &manager);
void DumpSymbolColumn(const TableHandle &table_handle);
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

  try {
    auto client = Client::Connect(server);
    auto manager = client.GetManager();
    auto table = MakeTable(manager);
    DumpSymbolColumn(table);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
TableHandle MakeTable(const TableHandleManager &manager) {
  TableMaker tm;
  std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
  std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
  tm.AddColumn("Symbol", symbols);
  tm.AddColumn("Price", prices);
  return tm.MakeTable(manager);
}

void DumpSymbolColumn(const TableHandle &table_handle) {
  auto fsr = table_handle.GetFlightStreamReader();
  while (true) {
    auto res = fsr->Next();
    OkOrThrow(DEEPHAVEN_LOCATION_EXPR(res));
    if (res->data == nullptr) {
      break;
    }

    auto symbol_chunk = res->data->GetColumnByName("Symbol");
    if (symbol_chunk == nullptr) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Symbol column not found"));
    }
    auto price_chunk = res->data->GetColumnByName("Price");
    if (price_chunk == nullptr) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Price column not found"));
    }

    auto symbol_as_string_array = std::dynamic_pointer_cast<arrow::StringArray>(symbol_chunk);
    auto price_as_double_array = std::dynamic_pointer_cast<arrow::DoubleArray>(price_chunk);
    if (symbol_as_string_array == nullptr) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("symbolChunk was not an arrow::StringArray"));
    }
    if (price_as_double_array == nullptr) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("priceChunk was not an arrow::DoubleArray"));
    }

    if (symbol_as_string_array->length() != price_as_double_array->length()) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Lengths differ"));
    }

    for (int64_t i = 0; i < symbol_as_string_array->length(); ++i) {
      auto symbol = symbol_as_string_array->GetView(i);
      auto price = price_as_double_array->Value(i);
      std::cout << symbol << ' ' << price << '\n';
    }
  }
}
}  // namespace
