/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::NumCol;
using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::TableMaker;

namespace {
TableHandle makeTable(const TableHandleManager &manager);
void dumpSymbolColumn(const TableHandle &tableHandle);
}  // namespace

int main() {
  const char *server = "localhost:10000";

  try {
    auto client = Client::connect(server);
    auto manager = client.getManager();
    auto table = makeTable(manager);
    dumpSymbolColumn(table);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
TableHandle makeTable(const TableHandleManager &manager) {
  TableMaker tm;
  std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
  std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
  tm.addColumn("Symbol", symbols);
  tm.addColumn("Price", prices);
  return tm.makeTable(manager);
}

void dumpSymbolColumn(const TableHandle &tableHandle) {
  auto fsr = tableHandle.getFlightStreamReader();
  while (true) {
    arrow::flight::FlightStreamChunk chunk;
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->Next(&chunk)));
    if (chunk.data == nullptr) {
      break;
    }

    auto symbolChunk = chunk.data->GetColumnByName("Symbol");
    if (symbolChunk == nullptr) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Symbol column not found"));
    }
    auto priceChunk = chunk.data->GetColumnByName("Price");
    if (priceChunk == nullptr) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Price column not found"));
    }

    auto symbolAsStringArray = std::dynamic_pointer_cast<arrow::StringArray>(symbolChunk);
    auto priceAsDoubleArray = std::dynamic_pointer_cast<arrow::DoubleArray>(priceChunk);
    if (symbolAsStringArray == nullptr) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("symbolChunk was not an arrow::StringArray"));
    }
    if (priceAsDoubleArray == nullptr) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("priceChunk was not an arrow::DoubleArray"));
    }

    if (symbolAsStringArray->length() != priceAsDoubleArray->length()) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Lengths differ"));
    }

    for (int64_t i = 0; i < symbolAsStringArray->length(); ++i) {
      auto symbol = symbolAsStringArray->GetView(i);
      auto price = priceAsDoubleArray->Value(i);
      std::cout << symbol << ' ' << price << '\n';
    }
  }
}
}  // namespace
