/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include <set>
#include <thread>

#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/highlevel/ticking.h"
#include "deephaven/client/highlevel/sad/chunk_maker.h"
#include "deephaven/client/highlevel/sad/sad_chunk.h"
#include "deephaven/client/highlevel/sad/sad_context.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::highlevel::Client;
using deephaven::client::highlevel::NumCol;
using deephaven::client::highlevel::SortPair;
using deephaven::client::highlevel::TableHandle;
using deephaven::client::highlevel::TableHandleManager;
using deephaven::client::highlevel::TickingCallback;
using deephaven::client::highlevel::sad::ChunkMaker;
using deephaven::client::highlevel::sad::SadChunk;
using deephaven::client::highlevel::sad::SadChunkVisitor;
using deephaven::client::highlevel::sad::SadContext;
using deephaven::client::highlevel::sad::SadDoubleChunk;
using deephaven::client::highlevel::sad::SadIntChunk;
using deephaven::client::highlevel::sad::SadLongChunk;
using deephaven::client::highlevel::sad::SadSizeTChunk;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::valueOrThrow;

using std::size_t;

// Hey, we should standardize on either deephaven or io::deephaven

namespace {
class Callback final : public TickingCallback {
public:
  void onFailure(std::exception_ptr ep) final;
  void onTick(const std::shared_ptr<SadTable> &table) final;
  bool failed() const { return failed_; }

private:
    std::atomic<bool> failed_ = false;
};

// or maybe make a stream manipulator
std::string getWhat(std::exception_ptr ep);

void Callback::onFailure(std::exception_ptr ep) {
  streamf(std::cerr, "Callback reported failure: %o\n", getWhat(std::move(ep)));
  failed_ = true;
}

class ElementStreamer final : public SadChunkVisitor {
public:
  ElementStreamer(std::ostream &s, size_t index) : s_(s), index_(index) {}

  void visit(const SadIntChunk &chunk) const final {
    s_ << chunk.data()[index_];
  }

  void visit(const SadLongChunk &chunk) const final {
    s_ << chunk.data()[index_];
  }

  void visit(const SadDoubleChunk &chunk) const final {
    s_ << chunk.data()[index_];
  }

  void visit(const SadSizeTChunk &chunk) const final {
    s_ << chunk.data()[index_];
  }

private:
  std::ostream &s_;
  size_t index_ = 0;
};

template<typename T> inline std::vector<T> reserved_vector(size_t n) {
    std::vector<T> v;
    v.reserve(n);
    return v;
}

void Callback::onTick(const std::shared_ptr<SadTable> &table) {
  // Deliberately chosen to be small so I can test chunking. In production this would be a lot larger.
  const size_t chunkSize = 8;

  auto ncols = table->numColumns();
  auto selectedCols = reserved_vector<size_t>(ncols);

  for (size_t col = 0; col < ncols; ++col) {
    selectedCols.push_back(col);
  }
  auto nrows = table->numRows();

  auto outerIter = table->getRowSequence()->getRowSequenceIterator();

  for (size_t startRow = 0; startRow < nrows; startRow += chunkSize) {
    auto selectedRows = outerIter->getNextRowSequenceWithLength(chunkSize);
    auto thisSize = selectedRows->size();

    auto unwrappedTable = table->unwrap(selectedRows, selectedCols);
    auto rowKeys = unwrappedTable->getUnorderedRowKeys();

    auto contexts = reserved_vector<std::shared_ptr<SadContext>>(ncols);
    auto chunks = reserved_vector<std::shared_ptr<SadChunk>>(ncols);

    for (size_t col = 0; col < ncols; ++col) {
      const auto &c = unwrappedTable->getColumn(col);
      auto context = c->createContext(thisSize);
      auto chunk = ChunkMaker::createChunkFor(*c, thisSize);
      c->fillChunkUnordered(context.get(), *rowKeys, thisSize, chunk.get());
      chunks.push_back(std::move(chunk));
      contexts.push_back(std::move(context));
    }
    for (size_t j = 0; j < thisSize; ++j) {
      ElementStreamer es(std::cerr, j);
      auto chunk_accept = [&es](std::ostream &s, const std::shared_ptr<SadChunk> &chunk) {
        chunk->acceptVisitor(es);
      };
      std::cerr << deephaven::client::utility::separatedList(chunks.begin(), chunks.end(), ", ", chunk_accept) << '\n';
    }
  }
}
}  // namespace

void doit(const TableHandleManager &manager) {
  auto start = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  auto tt1 = manager
      .timeTable(start, 1 * 1'000'000'000L)
//      .select("Foo = (ii % 20)", "Bar = ii")
//      .select("Foo = ii < 3 ? ii * 10 : (i == 3 ? 15 : (i == 4 ? 12 : 3))"
//      // .select("Foo = ii < 10 ? ii * 10 : 23")
      .select("Foo = ((ii * 0xdeadbeef) + 0xbaddbabe) % 5000", "Bar = 34.2 + ii", "II = ii", "SomeInt = (int) Bar")
      .sort({SortPair::ascending("Foo", false)})
      .head(10)
      //.tail(3)  // this will give us deletes
      ;
  //auto tt2 = manager.timeTable(start, 2 * 1'000'000'000L).select("TS2 = Timestamp");
  //auto t = tt1.crossJoin(tt2, std::vector<std::string>(), {});


  // tt1.bindToVariable("showme");

  //      // .select("Foo = (ii % 20)")
  //      // .select("Foo = ii < 3 ? ii * 10 : (i == 3 ? 15 : (i == 4 ? 12 : 3))")
  //      // .select("Foo = ii < 10 ? ii * 10 : 23")
  //      .select("Foo = ((ii * 0xdeadbeef) + 0xbaddbabe) % 5000")
  //      .sort({SortPair::ascending("Foo", false)})
  //      .head(10);

  auto myCallback = std::make_shared<Callback>();
  tt1.subscribe(myCallback);
  uint32_t tens_of_seconds_to_run = 50000;
  while (tens_of_seconds_to_run-- > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds (100));
      if (myCallback->failed()) {
          std::cerr << "callback reported failure, aborting in main subscription thread.\n";
          break;
      }
  }
  std::cerr << "I unsubscribed here.\n";
  tt1.unsubscribe(std::move(myCallback));
  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::cerr << "exiting.\n";
}

// let's keep this off to the side
//  auto tLeft = manager
//      .emptyTable(10)
//      .select("II = ii", "Zamboni = `zamboni`+ii");
//  auto tRight = manager
//        .timeTable(start, 1 * 1'000'000'000L)
//        .update("II = 0L")
//        .tail(1);
//  auto tt1 = tLeft.naturalJoin(tRight, {"II"}, {});


// also this
//  auto tt1 = manager
//      .timeTable(start, 1 * 1'000'000'000L)
//      // .select("Foo = (ii % 20)")
//      // .select("Foo = ii < 3 ? ii * 10 : (i == 3 ? 15 : (i == 4 ? 12 : 3))")
//      // .select("Foo = ii < 10 ? ii * 10 : 23")
//      .select("Foo = ((ii * 0xdeadbeef) + 0xbaddbabe) % 5000")
//      .sort({SortPair::ascending("Foo", false)})
//      .head(10);
//.tail(3);  // this will give us deletes
//auto tt2 = manager.timeTable(start, 2 * 1'000'000'000L).select("TS2 = Timestamp");
//auto t = tt1.crossJoin(tt2, std::vector<std::string>(), {});


// tt1.bindToVariable("showme");

//      // .select("Foo = (ii % 20)")
//      // .select("Foo = ii < 3 ? ii * 10 : (i == 3 ? 15 : (i == 4 ? 12 : 3))")
//      // .select("Foo = ii < 10 ? ii * 10 : 23")
//      .select("Foo = ((ii * 0xdeadbeef) + 0xbaddbabe) % 5000")
//      .sort({SortPair::ascending("Foo", false)})
//      .head(10);


void makeModifiesHappen(const TableHandleManager &manager) {
  auto start = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();

  auto tLeft = manager
      .emptyTable(10)
      .select("II = ii", "Zamboni = ii");
  auto tRight = manager
      .timeTable(start, 1 * 1'000'000'000L)
      .select("II = 0L", "TS = (long)(Timestamp.getNanos()/1000000000)")
      .tail(1);
  auto tt1 = tLeft.naturalJoin(tRight, {"II"}, {}).select("II", "Zamboni", "TS");

  tt1.bindToVariable("showme");

  auto myCallback = std::make_shared<Callback>();
  tt1.subscribe(myCallback);
  std::this_thread::sleep_for(std::chrono::seconds(5'000));
  std::cerr << "I unsubscribed here\n";
  tt1.unsubscribe(std::move(myCallback));
  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::cerr << "exiting\n";
}

int main() {
  const char *server = "localhost:10000";

  try {
    auto client = Client::connect(server);
    auto manager = client.getManager();
    makeModifiesHappen(manager);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {

std::string getWhat(std::exception_ptr ep) {
  try {
    std::rethrow_exception(std::move(ep));
  } catch (const std::exception &e) {
    return e.what();
  } catch (...) {
    return "(unknown exception)";
  }
}


}  // namespace
