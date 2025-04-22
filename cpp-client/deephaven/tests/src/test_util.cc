/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/tests/test_util.h"

#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"

using deephaven::client::TableHandle;
using deephaven::client::utility::ArrowUtil;
using deephaven::client::utility::OkOrThrow;
using deephaven::dhcore::utility::separatedList;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::ValueOrThrow;

namespace deephaven::client::tests {
std::map<std::string, std::string, std::less<>> *GlobalEnvironmentForTests::environment_ = nullptr;

void GlobalEnvironmentForTests::Init(char **envp) {
  if (environment_ != nullptr) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("It's an error to call Init() twice"));
  }
  environment_ = new std::map<std::string, std::string, std::less<>>();

  for (char **current = envp; *current != nullptr; ++current) {
    std::string_view sv(*current);

    // Find the equal sign and split the strings into keys and values.
    auto pos = sv.find('=');
    if (pos == std::string_view::npos) {
      continue;
    }
    auto key = sv.substr(0, pos);
    auto value = sv.substr(pos + 1);
    environment_->try_emplace(std::string(key), value);
  }
}

std::string_view GlobalEnvironmentForTests::GetEnv(std::string_view key,
    std::string_view default_value) {
  if (environment_ == nullptr) {
    return default_value;
  }
  auto ip = environment_->find(key);
  if (ip == environment_->end()) {
    return default_value;
  }
  return ip->second;
}

ColumnNamesForTests::ColumnNamesForTests() : importDate_("ImportDate"), ticker_("Ticker"),
  open_("Open"), close_("Close"), volume_("Volume") {}
ColumnNamesForTests::ColumnNamesForTests(ColumnNamesForTests &&other) noexcept = default;
ColumnNamesForTests &ColumnNamesForTests::operator=(ColumnNamesForTests &&other) noexcept = default;
ColumnNamesForTests::~ColumnNamesForTests() = default;

ColumnDataForTests::ColumnDataForTests() {
  importDate_ = std::vector<std::string> {
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-01",
    "2017-11-02",
    };

  ticker_ = std::vector<std::string> {
    "XRX",
    "XRX",
    "XYZZY",
    "IBM",
    "GME",
    "AAPL",
    "AAPL",
    "AAPL",
    "ZNGA",
    "ZNGA",
    "T",
    };

  open_ = std::vector<double> {
    83.1,
    50.5,
    92.3,
    40.1,
    681.43,
    22.1,
    26.8,
    31.5,
    541.2,
    685.3,
    18.8
  };

  close_ = std::vector<double> {
    88.2,
    53.8,
    88.5,
    38.7,
    453,
    23.5,
    24.2,
    26.7,
    538.2,
    544.9,
    13.4
  };

  volume_ = std::vector<int64_t> {
    345000,
    87000,
    6060842,
    138000,
    138000000,
    100000,
    250000,
    19000,
    46123,
    48300,
    1500
  };
}

ColumnDataForTests::ColumnDataForTests(ColumnDataForTests &&other) noexcept = default;
ColumnDataForTests &ColumnDataForTests::operator=(ColumnDataForTests &&other) noexcept = default;
ColumnDataForTests::~ColumnDataForTests() = default;

TableMakerForTests TableMakerForTests::Create() {
  auto client = CreateClient();
  auto manager = client.GetManager();

  ColumnNamesForTests cn;
  ColumnDataForTests cd;

  TableMaker maker;
  maker.AddColumn(cn.ImportDate(), cd.ImportDate());
  maker.AddColumn(cn.Ticker(), cd.Ticker());
  maker.AddColumn(cn.Open(), cd.Open());
  maker.AddColumn(cn.Close(), cd.Close());
  maker.AddColumn(cn.Volume(), cd.Volume());

  auto test_table = maker.MakeTable(manager);
  return TableMakerForTests(std::move(client), std::move(test_table), std::move(cn), std::move(cd));
}

Client TableMakerForTests::CreateClient(const ClientOptions &options) {
  auto host = GlobalEnvironmentForTests::GetEnv("DH_HOST", "localhost");
  auto port = GlobalEnvironmentForTests::GetEnv("DH_PORT", "10000");
  auto connection_string = fmt::format("{}:{}", host, port);
  fmt::print(std::cerr, "Connecting to {}\n", connection_string);
  auto client = Client::Connect(connection_string, options);
  return client;
}

TableMakerForTests::TableMakerForTests(TableMakerForTests::ClientType &&client,
    TableHandle &&test_table, ColumnNamesForTests &&column_names, ColumnDataForTests &&column_data) :
    client_(std::move(client)),
    testTable_(std::move(test_table)), columnNames_(std::move(column_names)),
    columnData_(std::move(column_data)) {}

TableMakerForTests::TableMakerForTests(TableMakerForTests &&) noexcept = default;
TableMakerForTests &TableMakerForTests::operator=(TableMakerForTests &&) noexcept = default;
TableMakerForTests::~TableMakerForTests() = default;

void TableComparerForTests::Compare(const TableMaker &expected, const TableHandle &actual) {
  auto exp_as_arrow_table = expected.MakeArrowTable();
  auto act_as_arrow_table = actual.ToArrowTable();
  Compare(*exp_as_arrow_table, *act_as_arrow_table);
}

void TableComparerForTests::Compare(const TableMaker &expected, const arrow::Table &actual) {
  auto exp_as_arrow_table = expected.MakeArrowTable();
  Compare(*exp_as_arrow_table, actual);
}

void TableComparerForTests::Compare(const TableMaker &expected, const ClientTable &actual) {
  auto exp_as_arrow_table = expected.MakeArrowTable();
  auto act_as_arrow_table = ArrowUtil::MakeArrowTable(actual);
  Compare(*exp_as_arrow_table, *act_as_arrow_table);
}

void TableComparerForTests::Compare(const arrow::Table &expected, const arrow::Table &actual) {
  if (expected.num_columns() != actual.num_columns()) {
    auto message = fmt::format("Expected table has {} columns, but actual table has {} columns",
        expected.num_columns(), actual.num_columns());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  auto num_cols = expected.num_columns();
  // Collect all type issues (if any) into a single exception
  std::vector<std::string> issues;
  for (int i = 0; i != num_cols; ++i) {
    const auto &exp = expected.field(i);
    const auto &act = actual.field(i);

    if (exp->name() != act->name()) {
      auto message = fmt::format("Column {}: Expected column name {}, actual is {}", i, exp->name(),
          act->name());
      issues.emplace_back(std::move(message));
    }

    if (!exp->type()->Equals(*act->type())) {
      auto message = fmt::format("Column {}: Expected column type {}, actual is {}", i,
          exp->type()->ToString(), act->type()->ToString());
      issues.emplace_back(std::move(message));
    }
  }

  if (!issues.empty()) {
    auto message = fmt::to_string(separatedList(issues.begin(), issues.end()));
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  for (int i = 0; i != num_cols; ++i) {
    const auto &exp = expected.column(i);
    const auto &act = actual.column(i);

    if (exp->length() != act->length()) {
      auto message = fmt::format("Column {}: Expected length {}, actual length {}",
          expected.field(i)->name(), exp->length(), act->length());
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }

    int64_t element_index = 0;
    int exp_chunk_num = 0;
    int act_chunk_num = 0;
    int exp_chunk_index = 0;
    int act_chunk_index = 0;
    while (element_index < exp->length()) {
      if (exp_chunk_num >= exp->num_chunks() || act_chunk_num >= act->num_chunks()) {
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Logic error"));
      }
      const auto &exp_chunk = exp->chunk(exp_chunk_num);
      if (exp_chunk_index == exp_chunk->length()) {
        // Exhausted current chunk on "expected" side. Bump to next chunk and start over.
        exp_chunk_index = 0;
        ++exp_chunk_num;
        continue;
      }

      const auto &act_chunk = act->chunk(act_chunk_num);
      if (act_chunk_index == act_chunk->length()) {
        // Exhausted current chunk on "actual" side. Bump to next chunk and start over.
        act_chunk_index = 0;
        ++act_chunk_num;
        continue;
      }

      const auto exp_item = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(exp_chunk->GetScalar(exp_chunk_index)));
      const auto act_item = ValueOrThrow(DEEPHAVEN_LOCATION_EXPR(act_chunk->GetScalar(act_chunk_index)));

      if (!exp_item->Equals(*act_item)) {
        auto message = fmt::format("Column {}: Columns differ at element {}: {} vs {}",
            expected.field(i)->name(), element_index, exp_item->ToString(), act_item->ToString());
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }

      ++element_index;
      ++exp_chunk_index;
      ++act_chunk_index;
    }
  }
}
}  // namespace deephaven::client::tests
