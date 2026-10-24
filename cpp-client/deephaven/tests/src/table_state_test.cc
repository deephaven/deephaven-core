/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/barrage_test_util.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/immer_table_state.h"
#include "deephaven/dhcore/types.h"

using deephaven::dhcore::ElementType;
using deephaven::dhcore::ElementTypeId;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::ticking::ImmerTableState;

// ImmerTableState holds only the rows the subscription has received data for. Erase is handed the
// server's removed set, which can name rows it does not hold, and must erase just the ones it
// does, reporting their positions.
namespace deephaven::client::tests {
namespace {
// An ImmerTableState holding exactly 'keys', each row's Key column being its key.
std::unique_ptr<ImmerTableState> Holding(const RowSequence &keys) {
  auto state = std::make_unique<ImmerTableState>(
      Schema::Create({"Key"}, {ElementType::Of(ElementTypeId::kInt64)}));
  auto index_space = state->AddKeys(keys);
  auto elements = Elements(keys);
  std::vector<std::shared_ptr<ColumnSource>> sources{LongColumn(elements)};
  std::vector<size_t> begins{0};
  std::vector<size_t> ends{elements.size()};
  state->AddData(sources, begins, ends, *index_space);
  return state;
}

void CheckHoldsKeys(const ImmerTableState &state, const std::vector<int64_t> &expected) {
  CHECK(FirstDifference("Key", expected, Column(*state.Snapshot(), 0)) == "");
}
}  // namespace

TEST_CASE("TableState: erase skips keys it does not hold", "[tablestate]") {
  auto state = Holding(*Keys({1, 2, 3, 10, 11, 12, 100}));

  // Of 0..4, only 1, 2 and 3 are held; they are the first three rows.
  auto erased = state->Erase(*Range(0, 5));
  CHECK(Render(*erased) == "[[0,3)]");
  CheckHoldsKeys(*state, {10, 11, 12, 100});

  // Of 5..199, only 10, 11, 12 and 100 are held, and they are now rows 0..3.
  erased = state->Erase(*Range(5, 200));
  CHECK(Render(*erased) == "[[0,4)]");
  CheckHoldsKeys(*state, {});
}

TEST_CASE("TableState: erase of keys it does not hold changes nothing", "[tablestate]") {
  auto state = Holding(*Keys({1, 2, 3, 10, 11, 12, 100}));
  auto erased = state->Erase(*Range(50, 60));
  CHECK(erased->Empty());
  CheckHoldsKeys(*state, {1, 2, 3, 10, 11, 12, 100});
}

TEST_CASE("TableState: erase maps each interval to the ranks of its held keys", "[tablestate]") {
  auto state = Holding(*Keys({1, 2, 3, 10, 11, 12, 100}));
  RowSequenceBuilder builder;
  builder.AddInterval(0, 5);     // holds 1, 2, 3: rows 0..2
  builder.AddInterval(11, 101);  // holds 11, 12, 100: rows 4..6
  auto erased = state->Erase(*builder.Build());
  CHECK(Render(*erased) == "[[0,3), [4,7)]");
  CheckHoldsKeys(*state, {10});
}

TEST_CASE("TableState: present keys returns the held subset", "[tablestate]") {
  auto state = Holding(*Keys({1, 2, 3, 10, 11, 12, 100}));
  CHECK(Render(*state->PresentKeys(*Range(0, 5))) == "[[1,4)]");
  CHECK(Render(*state->PresentKeys(*Range(50, 60))) == "[]");
  RowSequenceBuilder builder;
  builder.AddInterval(0, 5);
  builder.AddInterval(11, 101);
  CHECK(Render(*state->PresentKeys(*builder.Build())) == "[[1,4), [11,13), [100,101)]");
}

// The second erasure's positions count only the rows that survived the first, so each has to move
// past the rows the first one took out ahead of it.
TEST_CASE("TableState: combine erasures lifts the second past the first", "[tablestate]") {
  // Rows 0 and 1 go first; the next two rows of what remains were rows 2 and 3 originally.
  CHECK(Render(*ImmerTableState::CombineErasures(*Range(0, 2), *Range(0, 2))) == "[[0,4)]");
  // Rows 5..7 go first; row 396 of what remains was row 399.
  CHECK(Render(*ImmerTableState::CombineErasures(*Range(5, 8), *Range(396, 397)))
      == "[[5,8), [399,400)]");
  // Rows 3 and 5 go first; rows 3 and 4 of what remains were rows 4 and 6, and the lift grows
  // between them.
  CHECK(Render(*ImmerTableState::CombineErasures(*Keys({3, 5}), *Range(3, 5))) == "[[3,7)]");
  // Either erasure may be empty.
  CHECK(Render(*ImmerTableState::CombineErasures(*None(), *Range(2, 4))) == "[[2,4)]");
  CHECK(Render(*ImmerTableState::CombineErasures(*Range(2, 4), *None())) == "[[2,4)]");
}
}  // namespace deephaven::client::tests
