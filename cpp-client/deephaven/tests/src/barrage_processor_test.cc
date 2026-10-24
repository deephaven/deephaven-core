/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/tests/barrage_test_util.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/barrage_processor.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/flatbuf/Barrage_generated.h"
#include "flatbuffers/flatbuffers.h"

using deephaven::dhcore::ElementType;
using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::ticking::BarrageProcessor;
using deephaven::dhcore::ticking::TickingUpdate;
using io::deephaven::barrage::flatbuf::BarrageMessageType;
using io::deephaven::barrage::flatbuf::BarrageModColumnMetadata;
using io::deephaven::barrage::flatbuf::CreateBarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::CreateBarrageModColumnMetadata;
using io::deephaven::barrage::flatbuf::CreateBarrageUpdateMetadata;

// Feeds BarrageProcessor the messages a server sends while it delivers the initial snapshot of a
// large table in rounds, with no server involved. The shapes come straight from the server's
// writer: a snapshot round announces the table's whole row set in added_rows and carries data only
// for its own slice, named in added_rows_included; a delta between rounds can carry data for rows
// added_rows does not mention and no data for rows it does.
namespace deephaven::client::tests {
namespace {
// Encodes a RowSequence in the format the server uses for every row set inside
// BarrageUpdateMetadata, which is the format IndexDecoder reads. A row set is written as signed
// deltas from a running offset: a range is its first key followed by its last key negated, a lone
// key is just that key, and an End command closes the list. The server packs small deltas into
// byte and short arrays; this encoder writes every delta as a full long, which the decoder accepts
// just the same and which keeps this code trivially right.
// The two commands this encoder uses, as IndexDecoder names them: an offset carried as a long, and
// the end of the list.
constexpr int8_t kOffsetCommandWithLong = 8 | 3;
constexpr int8_t kEndCommand = 32;

std::vector<int8_t> EncodeRowSequence(const RowSequence &rows) {
  std::vector<int8_t> result;
  int64_t offset = 0;
  auto write = [&result, &offset](int64_t key, bool is_range_end) {
    auto delta = key - offset;
    if (is_range_end) {
      delta = -delta;
    }
    result.push_back(kOffsetCommandWithLong);
    int8_t bytes[sizeof(delta)];
    std::memcpy(bytes, &delta, sizeof(delta));
    result.insert(result.end(), bytes, bytes + sizeof(delta));
    offset = key;
  };
  rows.ForEachInterval([&write](uint64_t begin, uint64_t end) {
    if (begin == end) {
      return;
    }
    auto first = static_cast<int64_t>(begin);
    auto last = static_cast<int64_t>(end) - 1;
    write(first, false);
    if (last != first) {
      write(last, true);
    }
  });
  result.push_back(kEndCommand);
  return result;
}

// Builds the serialized BarrageMessageWrapper carrying a BarrageUpdateMetadata. Passing nullptr
// for 'included_rows' leaves the added_rows_included field absent, which is how the server says
// "same as added_rows". There is one modified-rows set per column. Shifts are always empty.
std::vector<uint8_t> Metadata(const RowSequence &removed_rows, const RowSequence &added_rows,
    const RowSequence *included_rows,
    const std::vector<std::shared_ptr<RowSequence>> &modified_rows_per_column,
    bool is_snapshot = false) {
  flatbuffers::FlatBufferBuilder md;

  auto removed_offset = md.CreateVector(EncodeRowSequence(removed_rows));
  // Shift data is three row sets back to back: range starts, range ends, destinations.
  auto empty_rows = EncodeRowSequence(*None());
  std::vector<int8_t> shifts;
  for (int i = 0; i != 3; ++i) {
    shifts.insert(shifts.end(), empty_rows.begin(), empty_rows.end());
  }
  auto shift_offset = md.CreateVector(shifts);
  auto added_offset = md.CreateVector(EncodeRowSequence(added_rows));
  flatbuffers::Offset<flatbuffers::Vector<int8_t>> included_offset = 0;
  if (included_rows != nullptr) {
    included_offset = md.CreateVector(EncodeRowSequence(*included_rows));
  }
  std::vector<flatbuffers::Offset<BarrageModColumnMetadata>> mod_nodes;
  for (const auto &modified_rows : modified_rows_per_column) {
    auto mod_rows_offset = md.CreateVector(EncodeRowSequence(*modified_rows));
    mod_nodes.push_back(CreateBarrageModColumnMetadata(md, mod_rows_offset));
  }
  auto mod_nodes_offset = md.CreateVector(mod_nodes);

  auto metadata = CreateBarrageUpdateMetadata(md, 0, 0, is_snapshot, 0, false, 0,
      added_offset, removed_offset, shift_offset, included_offset, mod_nodes_offset);
  md.Finish(metadata);

  flatbuffers::FlatBufferBuilder wrapper;
  auto payload_offset = wrapper.CreateVector(
      reinterpret_cast<const int8_t *>(md.GetBufferPointer()), md.GetSize());
  auto wrapped = CreateBarrageMessageWrapper(wrapper, BarrageProcessor::kDeephavenMagicNumber,
      BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata, payload_offset);
  wrapper.Finish(wrapped);
  return {wrapper.GetBufferPointer(), wrapper.GetBufferPointer() + wrapper.GetSize()};
}

// Metadata for a message that has no removes, shifts or modifies.
std::vector<uint8_t> AddsOnly(const RowSequence &added_rows, const RowSequence *included_rows,
    bool is_snapshot = false) {
  return Metadata(*None(), added_rows, included_rows, {None(), None()}, is_snapshot);
}

// Drives a BarrageProcessor one record batch at a time. The table has two long columns, Key and
// Value, and every row's Value is ten times its Key, so a table's contents can be checked from the
// keys it should hold.
class Driver {
public:
  Driver() : processor_(Schema::Create({"Key", "Value"},
      {ElementType::Of(ElementTypeId::kInt64), ElementType::Of(ElementTypeId::kInt64)})) {}

  static int64_t ValueFor(int64_t key) {
    return key * 10;
  }

  // Feeds one record batch holding the full rows for 'keys', in order. 'metadata' is nullptr for
  // every batch of a message after the first, as it is on the wire.
  std::optional<TickingUpdate> FeedRows(const std::vector<uint8_t> *metadata, const RowSequence &keys) {
    auto key_column = Elements(keys);
    std::vector<int64_t> value_column;
    for (auto key : key_column) {
      value_column.push_back(ValueFor(key));
    }
    return Feed(metadata, key_column, value_column);
  }

  // Feeds one record batch with explicit data per column. The columns may differ in length, as the
  // batches carrying modify data do.
  std::optional<TickingUpdate> Feed(const std::vector<uint8_t> *metadata,
      const std::vector<int64_t> &key_column, const std::vector<int64_t> &value_column) {
    std::vector<std::shared_ptr<ColumnSource>> sources{LongColumn(key_column), LongColumn(value_column)};
    std::vector<size_t> sizes{key_column.size(), value_column.size()};
    return processor_.ProcessNextChunk(sources, sizes,
        metadata == nullptr ? nullptr : metadata->data(),
        metadata == nullptr ? 0 : metadata->size());
  }

  // Checks that the table holds exactly the keys in 'key_runs', in order, each with its Value.
  static void CheckHolds(const ClientTable &table,
      const std::vector<std::shared_ptr<RowSequence>> &key_runs) {
    std::vector<int64_t> expected_keys;
    for (const auto &run : key_runs) {
      auto elements = Elements(*run);
      expected_keys.insert(expected_keys.end(), elements.begin(), elements.end());
    }
    std::vector<int64_t> expected_values;
    for (auto key : expected_keys) {
      expected_values.push_back(ValueFor(key));
    }
    CHECK(FirstDifference("Key", expected_keys, Column(table, 0)) == "");
    CHECK(FirstDifference("Value", expected_values, Column(table, 1)) == "");
  }

private:
  BarrageProcessor processor_;
};

// Subscribes to a 1000-row table and consumes the first snapshot round, which delivers keys
// 0..399.
Driver AfterFirstRound() {
  Driver driver;
  auto metadata = AddsOnly(*Range(0, 1000), Range(0, 400).get(), true);
  auto update = driver.FeedRows(&metadata, *Range(0, 400));
  if (!update.has_value()) {
    throw std::runtime_error("First round did not produce an update");
  }
  return driver;
}
}  // namespace

TEST_CASE("BarrageProcessor: snapshot round is sized from included rows", "[barrage]") {
  Driver driver;
  auto metadata = AddsOnly(*Range(0, 1000), Range(0, 400).get(), true);
  auto update = driver.FeedRows(&metadata, *Range(0, 400));
  // Sized from added_rows, the add phase would still be waiting for 600 more rows here.
  REQUIRE(update.has_value());
  CHECK(Render(*update->AddedRows()) == "[[0,400)]");
  Driver::CheckHolds(*update->Current(), {Range(0, 400)});
}

TEST_CASE("BarrageProcessor: included rows span record batches", "[barrage]") {
  Driver driver;
  auto metadata = AddsOnly(*Range(0, 1000), Range(0, 400).get(), true);
  CHECK(!driver.FeedRows(&metadata, *Range(0, 250)).has_value());
  auto update = driver.FeedRows(nullptr, *Range(250, 400));
  REQUIRE(update.has_value());
  Driver::CheckHolds(*update->Current(), {Range(0, 400)});
}

TEST_CASE("BarrageProcessor: later rounds announce the whole row set again", "[barrage]") {
  auto driver = AfterFirstRound();
  auto all = Range(0, 1000);
  auto second_metadata = AddsOnly(*all, Range(400, 800).get(), true);
  auto second = driver.FeedRows(&second_metadata, *Range(400, 800));
  auto third_metadata = AddsOnly(*all, Range(800, 1000).get(), true);
  auto third = driver.FeedRows(&third_metadata, *Range(800, 1000));
  REQUIRE(second.has_value());
  REQUIRE(third.has_value());
  CHECK(Render(*second->AddedRows()) == "[[400,800)]");
  CHECK(Render(*third->AddedRows()) == "[[800,1000)]");
  Driver::CheckHolds(*third->Current(), {all});
}

// A delta announcing a row beyond the delivered region carries no data for it: the included set is
// present but empty. The batch holds only modify data, here one row for the Value column and none
// for Key, and the add phase must not consume it. This is the shape behind "Chunks have
// inconsistent sizes".
TEST_CASE("BarrageProcessor: delta with empty included rows skips the add phase", "[barrage]") {
  auto driver = AfterFirstRound();
  auto metadata = Metadata(*None(), *Keys({1000}), None().get(), {None(), Keys({5})});
  auto update = driver.Feed(&metadata, {}, {999});
  REQUIRE(update.has_value());
  CHECK(update->AddedRows()->Empty());
  CHECK(Render(*update->ModifiedRows()[1]) == "[[5,6)]");
  CHECK(update->Current()->NumRows() == 400);
  CHECK(Column(*update->Current(), 1)[5] == 999);
}

// Removing a row inside the delivered region moves the next undelivered row into it, and the server
// sends that row's data without announcing it in added_rows. It has to be inserted all the same.
TEST_CASE("BarrageProcessor: scoped-in row arrives with empty added rows", "[barrage]") {
  auto driver = AfterFirstRound();
  auto metadata = Metadata(*Keys({10}), *None(), Keys({400}).get(), {None(), None()});
  auto update = driver.FeedRows(&metadata, *Keys({400}));
  REQUIRE(update.has_value());
  CHECK(Render(*update->RemovedRows()) == "[[10,11)]");
  CHECK(Render(*update->AddedRows()) == "[[399,400)]");
  Driver::CheckHolds(*update->Current(), {Range(0, 10), Range(11, 401)});
}

TEST_CASE("BarrageProcessor: absent included rows means same as added rows", "[barrage]") {
  Driver driver;
  auto first_metadata = AddsOnly(*Range(0, 1000), Range(0, 1000).get(), true);
  (void)driver.FeedRows(&first_metadata, *Range(0, 1000));
  auto delta_metadata = AddsOnly(*Range(1000, 1005), nullptr);
  auto update = driver.FeedRows(&delta_metadata, *Range(1000, 1005));
  REQUIRE(update.has_value());
  CHECK(Render(*update->AddedRows()) == "[[1000,1005)]");
  Driver::CheckHolds(*update->Current(), {Range(0, 1005)});
}

// While the initial snapshot is still arriving the server can re-send a row the client already
// holds. Treating that as an overwrite is a separate change; until it lands, the row must be
// rejected rather than inserted a second time.
TEST_CASE("BarrageProcessor: re-sent row is rejected", "[barrage]") {
  auto driver = AfterFirstRound();
  auto metadata = AddsOnly(*Range(0, 1000), Range(399, 800).get(), true);
  CHECK_THROWS_WITH(driver.FeedRows(&metadata, *Range(399, 800)),
      Catch::Contains("already in the set"));
}

// A full subscription is sent the server's entire removed set, which while the snapshot is still
// arriving can name rows this client has not been given yet. Those rows are not here to remove, so
// the table is unchanged, and the update says so the way it does for an empty removed set: by
// handing out the same table.
TEST_CASE("BarrageProcessor: remove of undelivered rows leaves the table untouched", "[barrage]") {
  auto driver = AfterFirstRound();
  auto metadata = Metadata(*Range(800, 820), *None(), nullptr, {None(), None()});
  auto update = driver.Feed(&metadata, {}, {});
  REQUIRE(update.has_value());
  CHECK(update->RemovedRows()->Empty());
  CHECK(update->AfterRemoves() == update->Prev());
  Driver::CheckHolds(*update->Current(), {Range(0, 400)});
}

// A removed set that straddles the delivered region erases only the rows this client holds, and
// the row count drops by that many. The rows that move into the delivered region as a result
// arrive in the same message, as included rows.
TEST_CASE("BarrageProcessor: remove straddling the delivered region erases only held rows", "[barrage]") {
  auto driver = AfterFirstRound();
  auto metadata = Metadata(*Range(390, 410), *None(), Range(410, 420).get(), {None(), None()});
  auto update = driver.FeedRows(&metadata, *Range(410, 420));
  REQUIRE(update.has_value());
  CHECK(Render(*update->RemovedRows()) == "[[390,400)]");
  CHECK(update->AfterRemoves()->NumRows() == 390);
  CHECK(Render(*update->AddedRows()) == "[[390,400)]");
  Driver::CheckHolds(*update->Current(), {Range(0, 390), Range(410, 420)});
}
}  // namespace deephaven::client::tests
