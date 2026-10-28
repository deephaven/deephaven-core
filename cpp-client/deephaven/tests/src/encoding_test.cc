/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::subscription::SubscriptionHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::ticking::TickingCallback;
using deephaven::dhcore::ticking::TickingUpdate;
using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::client::tests {

// Shared preamble for the setup scripts below: the Arrow pojo classes needed to describe an
// encoded column, plus the field builders for the two run-end-encoded flavors. A column is
// encoded by handing the server a BarrageSchema attribute whose field of the same name carries
// the desired encoding; columns absent from that schema keep their natural (unencoded) type.
static const char *kEncodingPreamble = R"xxx(
import jpy
from deephaven import new_table, time_table
from deephaven.column import string_col

_JIntCls      = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Int')
_JREECls      = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded')
_JDictEncCls  = jpy.get_type('org.apache.arrow.vector.types.pojo.DictionaryEncoding')
_JField       = jpy.get_type('org.apache.arrow.vector.types.pojo.Field')
_JFieldType   = jpy.get_type('org.apache.arrow.vector.types.pojo.FieldType')
_JSchema      = jpy.get_type('org.apache.arrow.vector.types.pojo.Schema')
_JHashMap     = jpy.get_type('java.util.HashMap')
_JArrayList   = jpy.get_type('java.util.ArrayList')
_JBarrageUtil = jpy.get_type('io.deephaven.extensions.barrage.util.BarrageUtil')
_JInt32       = _JIntCls(32, True)
_JREE         = _JREECls.INSTANCE

# A BarrageSchema attribute is the authoritative wire schema: its field list determines both the
# set and the order of the columns the server sends, not merely which of them are encoded. It must
# therefore describe every column of the table -- naming only the encoded column projects the rest
# away, which then desynchronizes the server's per-column chunk writers. So start from the table's
# natural schema and replace exactly one field.
def _encoded_schema(table, col_name, to_encoded_field):
    natural = _JBarrageUtil.makeSchema(
        _JBarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, table.j_table.getDefinition(), _JHashMap(), False)
    natural_fields = natural.getFields()
    fields = _JArrayList()
    for i in range(natural_fields.size()):
        f = natural_fields.get(i)
        fields.add(to_encoded_field(f) if f.getName() == col_name else f)
    return _JSchema(fields)

# The three field transforms. Each keeps the natural field's value type and metadata (which already
# carries deephaven:type) and only adds the encoding. The run-end-encoded parent is non-nullable:
# an REE array carries no validity buffer of its own, nulls live on the values child.
def _to_ree_field(f):
    run_ends = _JField.notNullable('run_ends', _JInt32)
    values = _JField('values', _JFieldType(f.isNullable(), f.getType(), None, f.getMetadata()),
                     f.getChildren())
    children = _JArrayList()
    children.add(run_ends)
    children.add(values)
    return _JField(f.getName(), _JFieldType(False, _JREE, None, f.getMetadata()), children)

def _to_dict_field(f):
    dict_enc = _JDictEncCls(0, False, _JInt32)
    return _JField(f.getName(),
                   _JFieldType(f.isNullable(), f.getType(), dict_enc, f.getMetadata()),
                   f.getChildren())

def _to_ree_dict_field(f):
    run_ends = _JField.notNullable('run_ends', _JInt32)
    dict_enc = _JDictEncCls(0, False, _JInt32)
    values = _JField('values', _JFieldType(f.isNullable(), f.getType(), dict_enc, f.getMetadata()),
                     f.getChildren())
    children = _JArrayList()
    children.add(run_ends)
    children.add(values)
    return _JField(f.getName(), _JFieldType(False, _JREE, None, f.getMetadata()), children)
)xxx";

// Static (snapshot) tables.
// ree_table     : 6 rows, Sym = ["a","a","a","b","b","b"]
// dict_table    : 5 rows, Sym = ["x","y","z","x","y"]
// reedict_table : 6 rows, Sym = ["a","a","a","b","b","b"], doubly-encoded RunEndEncoded<Dictionary<...>>
static const char *kStaticEncodingTables = R"xxx(
_ree_src   = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
ree_table  = _ree_src.with_attributes(
    {'BarrageSchema': _encoded_schema(_ree_src, 'Sym', _to_ree_field)})

_dict_src  = new_table([string_col('Sym', ['x', 'y', 'z', 'x', 'y'])])
dict_table = _dict_src.with_attributes(
    {'BarrageSchema': _encoded_schema(_dict_src, 'Sym', _to_dict_field)})

_reedict_src   = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
reedict_table  = _reedict_src.with_attributes(
    {'BarrageSchema': _encoded_schema(_reedict_src, 'Sym', _to_ree_dict_field)})
)xxx";

// Ticking tables. Sym is deliberately shaped to stress all three encodings while staying a pure
// function of row position:
//   - runs of three identical values, so REE has multi-row runs to expand
//   - a brand-new distinct value every three rows, so the dictionary keeps growing and the server
//     must ship isDelta=true DictionaryBatch messages on nearly every update rather than one
//     complete dictionary up front (the case a snapshot test cannot reach)
//   - a null every seventh row, which breaks runs and produces null dictionary indices; it also
//     makes the same dictionary value appear in two non-adjacent runs (e.g. sym3 at ii 9 and 11)
// Row ii always holds II == ii and Sym == (ii % 7 == 3 ? null : "sym" + ii / 3), so the client can
// compute the expected contents of the whole table from its row count alone. Note the (long) cast
// on the division: the query language's '/' is floating-point division even for integral operands,
// so without it every row would get a distinct fractional value instead of runs of three.
static const char *kTickingEncodingTables = R"xxx(
_ticking_src = (time_table('PT0.1S')
                .update(['II = ii', 'Sym = (ii % 7 == 3) ? null : (`sym` + (long)(ii / 3))'])
                .drop_columns(['Timestamp']))

# Each of these is an attribute-only copy of the one source above, so all three tests see
# identical data and differ only in how the server is asked to encode Sym.
ree_ticking_table = _ticking_src.with_attributes(
    {'BarrageSchema': _encoded_schema(_ticking_src, 'Sym', _to_ree_field)})

dict_ticking_table = _ticking_src.with_attributes(
    {'BarrageSchema': _encoded_schema(_ticking_src, 'Sym', _to_dict_field)})

reedict_ticking_table = _ticking_src.with_attributes(
    {'BarrageSchema': _encoded_schema(_ticking_src, 'Sym', _to_ree_dict_field)})
)xxx";

// A table for split and shared dictionary batches, driven a step at a time by the test so that the
// server's choices are fixed. It is the newest SPLIT_LIVE_ROWS rows of an append-only input table,
// and split_dict_add adds rows whose Sym values, apart from a null every seventh row, have never
// been seen. Row II always holds Sym == (II % 7 == 3 ? null : "sym" + II). SPLIT_LIVE_ROWS is
// defined ahead of this script, from kSplitLiveRows.
static const char *kSplitDictionaryTable = R"xxx(
from deephaven import dtypes, empty_table
from deephaven.table_factory import input_table

_split_src = input_table(col_defs={'II': dtypes.int64, 'Sym': dtypes.string})
_split_tail = _split_src.tail(SPLIT_LIVE_ROWS)
split_dict_ticking_table = _split_tail.with_attributes(
    {'BarrageSchema': _encoded_schema(_split_tail, 'Sym', _to_dict_field)})

# adds rows II = first .. first + count - 1, in one update
def split_dict_add(first, count):
    _split_src.add(empty_table(count).update([
        f'II = {first}L + ii', 'Sym = (II % 7 == 3) ? null : (`sym` + II)']))
)xxx";

namespace {
[[nodiscard]]
std::string StaticSetupScript() {
  return std::string(kEncodingPreamble) + kStaticEncodingTables;
}

[[nodiscard]]
std::string TickingSetupScript() {
  return std::string(kEncodingPreamble) + kTickingEncodingTables;
}

// The number of rows to wait for before declaring a ticking test finished. At the script's tick
// interval this spans several Barrage updates, so the dictionary grows across message boundaries
// rather than arriving complete in the initial snapshot.
constexpr size_t kTargetRows = 30;

/**
 * The Sym value the ticking setup script assigns to the row at position 'index'. Kept in sync with
 * the Sym formula in kTickingEncodingTables.
 */
[[nodiscard]]
std::optional<std::string> ExpectedSym(size_t index) {
  if (index % 7 == 3) {
    return {};
  }
  return fmt::format("sym{}", index / 3);
}

/**
 * Ticking callback for the encoded-column tests. On every update it recomputes the expected
 * contents of the entire table from the current row count and compares. Validating on each tick,
 * rather than only once at the end, means a bad dictionary delta or a mis-expanded run is caught on
 * the update that carries it. Finishes once the table has reached 'target_rows' rows.
 *
 * A mismatch throws out of OnTick, which the subscription machinery reports through OnFailure; the
 * test body then rethrows it on the main thread.
 */
class EncodedTickingCallback final : public TickingCallback {
public:
  explicit EncodedTickingCallback(size_t target_rows) : target_rows_(target_rows) {}

  void OnTick(TickingUpdate update) final {
    const auto &current = update.Current();
    auto num_rows = current->NumRows();
    // Flushed progress trace: if the process dies on a fatal signal, the last line recorded tells
    // us which update it died on.
    std::cout << "=== update: " << num_rows << " rows ===" << std::endl;
    if (num_rows == 0) {
      // The initial snapshot of a fresh time table can be empty.
      return;
    }

    auto iis = MakeReservedVector<int64_t>(num_rows);
    auto syms = MakeReservedVector<std::optional<std::string>>(num_rows);
    for (size_t i = 0; i != num_rows; ++i) {
      iis.emplace_back(static_cast<int64_t>(i));
      syms.emplace_back(ExpectedSym(i));
    }

    TableMaker expected;
    expected.AddColumn("II", iis);
    expected.AddColumn("Sym", syms);
    TableComparerForTests::Compare(expected, *current);

    if (num_rows >= target_rows_) {
      NotifyDone();
    }
  }

  void OnFailure(std::exception_ptr ep) final {
    std::unique_lock guard(mutex_);
    exception_ptr_ = std::move(ep);
    cond_var_.notify_all();
  }

  /**
   * Blocks until the subscription either finishes or fails.
   */
  std::pair<bool, std::exception_ptr> WaitForUpdate() {
    std::unique_lock guard(mutex_);
    while (true) {
      if (done_ || exception_ptr_ != nullptr) {
        return std::make_pair(done_, exception_ptr_);
      }
      cond_var_.wait(guard);
    }
  }

private:
  void NotifyDone() {
    std::unique_lock guard(mutex_);
    done_ = true;
    cond_var_.notify_all();
  }

  size_t target_rows_;
  std::mutex mutex_;
  std::condition_variable cond_var_;
  bool done_ = false;
  std::exception_ptr exception_ptr_;
};

/**
 * Subscribes to the named ticking table and validates every update until the table reaches
 * kTargetRows rows.
 */
void SubscribeAndValidate(const TableHandleManager &thm, std::string table_name) {
  auto table = thm.FetchTable(std::move(table_name));
  auto callback = std::make_shared<EncodedTickingCallback>(kTargetRows);
  auto cookie = table.Subscribe(callback);

  std::exception_ptr failure;
  while (true) {
    auto [done, eptr] = callback->WaitForUpdate();
    if (eptr != nullptr) {
      failure = std::move(eptr);
      break;
    }
    if (done) {
      break;
    }
  }

  // Unsubscribe on the failure path as well: this joins the subscription thread while the client is
  // still alive. Unwinding the test with that thread still running risks a use-after-free.
  table.Unsubscribe(std::move(cookie));
  if (failure != nullptr) {
    std::rethrow_exception(failure);
  }
}

// The split dictionary test's sizes. The table keeps kSplitLiveRows rows, and each step adds
// kSplitRowsPerStep. Both exceed the client's batch size of 4096, and a step's new values outgrow
// the table's rows on every second step, which makes the server reset the dictionary.
constexpr int64_t kSplitLiveRows = 15000;
constexpr int64_t kSplitRowsPerStep = 10000;

// How long to wait for a subscription to reach a step, before failing rather than hanging.
constexpr auto kSplitStepTimeout = std::chrono::seconds(60);

[[nodiscard]]
std::string SplitSetupScript() {
  return std::string(kEncodingPreamble) + "\nSPLIT_LIVE_ROWS = " + std::to_string(kSplitLiveRows) +
      "\n" + kSplitDictionaryTable;
}

/**
 * The Sym value the split dictionary script gives the row with this II. Kept in sync with the Sym
 * formula in kSplitDictionaryTable.
 */
[[nodiscard]]
std::optional<std::string> SplitExpectedSym(int64_t ii) {
  if (ii % 7 == 3) {
    return {};
  }
  return fmt::format("sym{}", ii);
}

/**
 * Ticking callback for the split dictionary test. On every update it checks that the table holds
 * consecutive II values and that every row's Sym is the value the script derives from its II, which
 * a misapplied dictionary batch would break. The test waits on it for the table to reach a step.
 */
class SplitDictionaryCallback final : public TickingCallback {
public:
  void OnTick(TickingUpdate update) final {
    const auto &current = update.Current();
    auto num_rows = current->NumRows();
    if (num_rows == 0) {
      return;
    }

    auto iis = Int64Chunk::Create(num_rows);
    current->GetColumn("II", true)->FillChunk(*current->GetRowSequence(), &iis, nullptr);
    const auto first_ii = iis[0];
    auto expected_iis = MakeReservedVector<int64_t>(num_rows);
    auto expected_syms = MakeReservedVector<std::optional<std::string>>(num_rows);
    for (size_t i = 0; i != num_rows; ++i) {
      const auto ii = first_ii + static_cast<int64_t>(i);
      expected_iis.emplace_back(ii);
      expected_syms.emplace_back(SplitExpectedSym(ii));
    }

    TableMaker expected;
    expected.AddColumn("II", expected_iis);
    expected.AddColumn("Sym", expected_syms);
    TableComparerForTests::Compare(expected, *current);

    std::unique_lock guard(mutex_);
    last_ii_ = iis[num_rows - 1];
    cond_var_.notify_all();
  }

  void OnFailure(std::exception_ptr ep) final {
    std::unique_lock guard(mutex_);
    exception_ptr_ = std::move(ep);
    cond_var_.notify_all();
  }

  /**
   * Blocks until the table's last row has II 'last_ii'. Rethrows the subscription's failure, and
   * throws if the table does not get there within kSplitStepTimeout.
   */
  void WaitForLastII(int64_t last_ii) {
    std::unique_lock guard(mutex_);
    const auto reached = cond_var_.wait_for(guard, kSplitStepTimeout, [&] {
      return exception_ptr_ != nullptr || last_ii_ >= last_ii;
    });
    if (exception_ptr_ != nullptr) {
      std::rethrow_exception(exception_ptr_);
    }
    if (!reached) {
      throw std::runtime_error(fmt::format(
          "Timed out waiting for II {}; the last update reached {}", last_ii, last_ii_));
    }
  }

private:
  std::mutex mutex_;
  std::condition_variable cond_var_;
  int64_t last_ii_ = -1;
  std::exception_ptr exception_ptr_;
};

/**
 * Subscriptions to one table, each with its own SplitDictionaryCallback. Unsubscribes all of them
 * when destroyed, on the failure path as well: that joins their threads while the client is still
 * alive.
 */
class SplitDictionarySubscriptions {
public:
  explicit SplitDictionarySubscriptions(TableHandle table) : table_(std::move(table)) {}
  SplitDictionarySubscriptions(const SplitDictionarySubscriptions &) = delete;
  SplitDictionarySubscriptions &operator=(const SplitDictionarySubscriptions &) = delete;

  ~SplitDictionarySubscriptions() {
    for (auto &handle : handles_) {
      try {
        table_.Unsubscribe(handle);
      } catch (...) {
        // the test's own failure, if any, is the one to report
      }
    }
  }

  void Subscribe() {
    auto callback = std::make_shared<SplitDictionaryCallback>();
    handles_.push_back(table_.Subscribe(callback));
    callbacks_.push_back(std::move(callback));
  }

  /** Waits for every subscription's table to reach the row with II 'last_ii'. */
  void WaitForLastII(int64_t last_ii) {
    for (const auto &callback : callbacks_) {
      callback->WaitForLastII(last_ii);
    }
  }

private:
  TableHandle table_;
  std::vector<std::shared_ptr<SplitDictionaryCallback>> callbacks_;
  std::vector<std::shared_ptr<SubscriptionHandle>> handles_;
};

/** Adds the rows II = first .. first + count - 1 to the split dictionary table, in one update. */
void AddSplitRows(const TableHandleManager &thm, int64_t first, int64_t count) {
  thm.RunScript(fmt::format("split_dict_add({}, {})", first, count));
}
}  // namespace

TEST_CASE("Static run-end-encoded table is fetched and decoded correctly", "[encoding]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(StaticSetupScript());
  auto t = thm.FetchTable("ree_table");

  TableMaker expected;
  expected.AddColumn<std::string>("Sym", {"a", "a", "a", "b", "b", "b"});
  TableComparerForTests::Compare(expected, t);
}

TEST_CASE("Static dictionary-encoded table is fetched and decoded correctly", "[encoding]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(StaticSetupScript());
  auto t = thm.FetchTable("dict_table");

  TableMaker expected;
  expected.AddColumn<std::string>("Sym", {"x", "y", "z", "x", "y"});
  TableComparerForTests::Compare(expected, t);
}

TEST_CASE("Static run-end + dictionary encoded table is fetched and decoded correctly", "[encoding]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(StaticSetupScript());
  auto t = thm.FetchTable("reedict_table");

  TableMaker expected;
  expected.AddColumn<std::string>("Sym", {"a", "a", "a", "b", "b", "b"});
  TableComparerForTests::Compare(expected, t);
}

TEST_CASE("Ticking run-end-encoded table is decoded correctly", "[encoding][ticking]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(TickingSetupScript());
  SubscribeAndValidate(thm, "ree_ticking_table");
}

TEST_CASE("Ticking dictionary-encoded table is decoded correctly", "[encoding][ticking]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(TickingSetupScript());
  SubscribeAndValidate(thm, "dict_ticking_table");
}

TEST_CASE("Ticking run-end + dictionary table is decoded correctly", "[encoding][ticking]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(TickingSetupScript());
  SubscribeAndValidate(thm, "reedict_ticking_table");
}


/*
 * The client subscribes with a batch size of 4096, so the server sends more dictionary values than
 * that as several DictionaryBatches for the one dictionary, back to back: a replacement followed by
 * deltas. Subscriptions to one table with the same options share one write of each update. One
 * that joins after the server has reset the dictionary puts the dictionary ahead of the others,
 * since its snapshot adds the values of rows older than the reset. Before the next shared write the
 * others are sent those values as a catch-up, also split, followed by the write's own values. Every
 * update of every subscription must still decode to the right values.
 */
TEST_CASE("Ticking dictionary-encoded table decodes split and shared dictionary batches",
    "[encoding][ticking]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(SplitSetupScript());
  AddSplitRows(thm, 0, kSplitLiveRows);
  int64_t next_ii = kSplitLiveRows;
  auto step = [&](SplitDictionarySubscriptions &subs) {
    AddSplitRows(thm, next_ii, kSplitRowsPerStep);
    next_ii += kSplitRowsPerStep;
    subs.WaitForLastII(next_ii - 1);
  };

  SplitDictionarySubscriptions subs(thm.FetchTable("split_dict_ticking_table"));
  // Two subscriptions, which share one write of each step's update. They subscribe one at a time:
  // in one run, a second subscription made while the first's snapshot was still arriving had its
  // snapshot rows placed out of order, for a reason not yet known.
  subs.Subscribe();
  subs.WaitForLastII(next_ii - 1);
  subs.Subscribe();
  subs.WaitForLastII(next_ii - 1);

  // The first step adds its values to the dictionary, which then holds more values than the table
  // has rows. So the server resets it before the second step's shared write, which replaces both
  // subscriptions' dictionaries with only that step's values.
  step(subs);
  step(subs);

  // The third subscription's snapshot adds the values of the rows older than the reset. Its first
  // record batch is preceded by every value the dictionary then holds, split into several
  // dictionary batches.
  subs.Subscribe();
  subs.WaitForLastII(next_ii - 1);

  // The next shared write sends the first two subscriptions the values they lack, as a catch-up
  // that is split, then the write's own values. The step after resets the dictionary again.
  step(subs);
  step(subs);
}

}  // namespace deephaven::client::tests
