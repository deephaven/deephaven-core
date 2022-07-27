/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include <any>
#include <charconv>
#include <chrono>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <ratio>
#include <thread>

#include "deephaven/client/client.h"
#include "deephaven/client/ticking.h"
#include "deephaven/client/chunk/chunk_maker.h"
#include "deephaven/client/chunk/chunk.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/table/table.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/client/utility/utility.h"
#include "immer/algorithm.hpp"

using deephaven::client::Client;
using deephaven::client::NumCol;
using deephaven::client::SortPair;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::TickingCallback;
using deephaven::client::ClassicTickingUpdate;
using deephaven::client::ImmerTickingUpdate;
using deephaven::client::chunk::ChunkMaker;
using deephaven::client::chunk::AnyChunk;
using deephaven::client::chunk::ChunkVisitor;
using deephaven::client::chunk::Chunk;
using deephaven::client::chunk::DoubleChunk;
using deephaven::client::chunk::Int32Chunk;
using deephaven::client::chunk::Int64Chunk;
using deephaven::client::chunk::UInt64Chunk;
using deephaven::client::column::ColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::table::Table;
using deephaven::client::utility::makeReservedVector;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::separatedList;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::valueOrThrow;
using deephaven::client::utility::verboseCast;

using std::size_t;

namespace {
struct UserConfig {
  UserConfig(std::string server, bool wantLastBy, bool wantImmer, int64_t period, int64_t tableSize,
      int64_t endSerialNumber, int64_t keyFactor, int64_t numAdditionalCols);
  ~UserConfig();

  std::string server_;
  bool wantLastBy_ = false;
  bool wantImmer_ = false;
  int64_t period_ = 0;
  int64_t tableSize_ = 0;
  int64_t endSerialNumber_ = 0;
  int64_t keyFactor_ = 0;
  int64_t numAdditionalCols_ = 0;

  friend std::ostream &operator<<(std::ostream &s, const UserConfig &o);
};

struct RuntimeConfig {
  RuntimeConfig(size_t keyColumn, size_t serialNumberColumn) : keyColumn_(keyColumn),
      serialNumberColumn_(serialNumberColumn) {}

  size_t keyColumn_ = 0;
  size_t serialNumberColumn_ = 0;
};

UserConfig parseArgs(int argc, const char **argv);
void demo(std::shared_ptr<UserConfig> config, TableHandleManager *manager);
std::pair<TableHandle, std::shared_ptr<RuntimeConfig>> makeQuery(const UserConfig *userConfig,
    TableHandleManager *manager);
void showUsageAndExit();
int64_t getLong(std::string_view s);
}  // namespace

int main(int argc, const char **argv) {
  auto config = parseArgs(argc, argv);
  streamf(std::cout, "Config is %o\n", config);
  try {
    auto client = Client::connect(config.server_);
    auto manager = client.getManager();
    auto sharedConfig = std::make_shared<UserConfig>(std::move(config));
    demo(std::move(sharedConfig), &manager);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
UserConfig parseArgs(int argc, const char **argv) {
  if (argc != 9) {
    showUsageAndExit();
  }
  size_t nextIndex = 1;
  std::string serverAndPort = argv[nextIndex++];

  std::string_view tailVsLastBy = argv[nextIndex++];
  bool wantLastBy;
  if (tailVsLastBy == "lastby") {
    wantLastBy = true;
  } else if (tailVsLastBy == "tail") {
    wantLastBy = false;
  } else {
    showUsageAndExit();
  }

  std::string_view immerVsClassic = argv[nextIndex++];
  bool wantImmer;
  if (immerVsClassic == "immer") {
    wantImmer = true;
  } else if (immerVsClassic == "classic") {
    wantImmer = false;
  } else {
    showUsageAndExit();
  }

  auto period = getLong(argv[nextIndex++]);
  auto tableSize = getLong(argv[nextIndex++]);
  auto endSerialNumber = getLong(argv[nextIndex++]);
  auto keyFactor = getLong(argv[nextIndex++]);
  auto numAdditionalCols = getLong(argv[nextIndex++]);

  return UserConfig(std::move(serverAndPort), wantLastBy, wantImmer, period, tableSize,
      endSerialNumber, keyFactor, numAdditionalCols);
}

int64_t getLong(std::string_view s) {
  int64_t value;
  auto res = std::from_chars(s.begin(), s.end(), value);
  if (res.ec != std::errc()) {
    throw std::runtime_error(stringf("Couldn't parse %o, error code is %o", s, (int)res.ec));
  }
  if (res.ptr != s.end()) {
    throw std::runtime_error(stringf("Non-numeric material in %o", s));
  }
  return value;
}

void showUsageAndExit() {
  streamf(std::cerr,
      "Usage: server:port (tail/lastby) (immer/classic) period tableSize endSerialNumber keyFactor numAdditionalCols\n"
      "server:port\n"
      "  The server endpoint, such as localhost:10000\n"
      "(tail/lastby):\n"
      "  tail - creates an append-only table\n"
      "  lastby - creates a table with lastby (will experience shifts and modifies)\n"
      "(immer/classic):\n"
      "  immer - use Immer data structures to track the table data\n"
      "  classic - use traditional data structures to track the table data\n"
      "period:\n"
      "  The server adds a new row every 'period' nanoseconds\n"
      "tableSize:\n"
      "  The max size of the table (in general this is smaller than the number of new rows created because rows can wrap around and replace previous versions)\n"
      "endSerialNumber:\n"
      "  The (exclusive) upper bound of row numbers added to the table. The table will stop changing at this point\n"
      "keyFactor:\n"
      "  The serial number is multiplied by the keyFactor in order to spread rows around and cause a lot of shifts or modifies\n"
      "numAdditionalCols:\n"
      "  Number of additional autogenerated data columns\n"
      "\n"
      "One reasonable test would be:\n"
      "  localhost:10000 tail immer 10000000 1000 10000 3 5\n"
      );
  std::exit(1);
}

// or maybe make a stream manipulator
std::string getWhat(std::exception_ptr ep);

class SerialNumberProcessor final {
public:
  void process(Int64Chunk *unorderedSerialNumbers);

private:
  int64_t nextExpectedSerial_ = 0;
};

class Stopwatch {
public:
  Stopwatch();

  bool pulse(size_t *elapsedSeconds);

private:
  std::chrono::steady_clock::time_point start_;
  std::chrono::steady_clock::time_point last_;
};

class StatsProcessor {
public:
  void update(size_t rowsRemoved, size_t rowsAdded, size_t rowsModifiedEstimated,
      size_t cellsModified);
  void showStats(size_t numCols, size_t elapsedSeconds);

private:
  size_t lastRowsRemoved_ = 0;
  size_t lastRowsAdded_ = 0;
  size_t lastRowsModifiedEstimated_ = 0;
  size_t lastCellsModified_ = 0;

  size_t rowsRemoved_ = 0;
  size_t rowsAdded_ = 0;
  size_t rowsModifiedEstimated_ = 0;
  size_t cellsModified_ = 0;
};

class TableScanner {
public:
  void scanTable(const Table &table);
};

class FragmentationAnalyzer {
public:
  void analyze(const Table &table);

private:
  void analyzeHelper(std::string_view what, const ColumnSource &col);
};

class ClassicProcessor {
  struct Private {};
public:
  static std::shared_ptr<ClassicProcessor> create(std::shared_ptr<UserConfig> userConfig,
      std::shared_ptr<RuntimeConfig> runtimeConfig);

  ClassicProcessor(Private, std::shared_ptr<UserConfig> userConfig,
      std::shared_ptr<RuntimeConfig> runtimeConfi);
  ~ClassicProcessor();

  void processUpdate(const ClassicTickingUpdate &update);

private:
  void processSerialNumber(const ClassicTickingUpdate &update);

  std::shared_ptr<UserConfig> userConfig_;
  std::shared_ptr<RuntimeConfig> runtimeConfig_;
  SerialNumberProcessor serialNumberProcessor_;
  StatsProcessor statsProcessor_;
  TableScanner tableScanner_;
  Stopwatch stopwatch_;
};

class ImmerProcessor {
  struct Private {};
public:
  static std::shared_ptr<ImmerProcessor> create(std::shared_ptr<UserConfig> userConfig,
      std::shared_ptr<RuntimeConfig> runtimeConfig);

  ImmerProcessor(Private, std::shared_ptr<UserConfig> userConfig,
  std::shared_ptr<RuntimeConfig> runtimeConfi);
  ~ImmerProcessor();

  void post(ImmerTickingUpdate update);

private:
  static void runForever(std::shared_ptr<ImmerProcessor> self);
  void runForeverHelper();
  void processUpdate(const ImmerTickingUpdate &update);
  void processSerialNumber(const ImmerTickingUpdate &update);

  std::shared_ptr<UserConfig> userConfig_;
  std::shared_ptr<RuntimeConfig> runtimeConfig_;
  SerialNumberProcessor serialNumberProcessor_;
  StatsProcessor statsProcessor_;
  TableScanner tableScanner_;
  Stopwatch stopwatch_;
  FragmentationAnalyzer fragmentationAnalyzer_;
  std::mutex mutex_;
  std::condition_variable condvar_;
  std::queue<ImmerTickingUpdate> todo_;
};

class DemoCallback final : public TickingCallback {
public:
  explicit DemoCallback(std::shared_ptr<UserConfig> userConfig,
      std::shared_ptr<RuntimeConfig> runtimeConfig);
  ~DemoCallback() final;

  void onFailure(std::exception_ptr ep) final;
  void onTick(const ClassicTickingUpdate &update) final;
  void onTick(ImmerTickingUpdate update) final;

private:
  std::shared_ptr<ClassicProcessor> classicProcessor_;
  std::shared_ptr<ImmerProcessor> immerProcessor_;
};

void demo(std::shared_ptr<UserConfig> userConfig, TableHandleManager *manager) {
  auto [table, runtimeConfig] = makeQuery(userConfig.get(), manager);
  table.bindToVariable("showme");

  auto wantImmer = userConfig->wantImmer_;
  auto myCallback = std::make_shared<DemoCallback>(std::move(userConfig), std::move(runtimeConfig));
  auto handle = table.subscribe(myCallback, wantImmer);
  std::cout << "Press enter to stop:\n";
  std::string temp;
  std::getline(std::cin, temp);
  std::cerr << "Unsubscribing, then sleeping for 5 seconds\n";
  table.unsubscribe(handle);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::cerr << "exiting\n";
}

std::pair<TableHandle, std::shared_ptr<RuntimeConfig>> makeQuery(const UserConfig *userConfig,
    TableHandleManager *manager) {
  auto start = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();

  auto table = manager->timeTable(start, userConfig->period_).view("SerialNumber = ii");
  auto sn = table.getNumCol("SerialNumber");
  // Demo is done at 'maxSerialNumber'
  table = table.where(sn < userConfig->endSerialNumber_);
  // Key column: increasing, but wraps at 'tableSize'
  table = table.updateView(((sn * userConfig->keyFactor_) % userConfig->tableSize_).as("Key"));
  std::shared_ptr<RuntimeConfig> runtimeConfig;
  if (userConfig->wantLastBy_) {
    table = table.lastBy("Key");
    runtimeConfig = std::make_shared<RuntimeConfig>(0, 1);
  } else {
    table = table.tail(userConfig->tableSize_);
    runtimeConfig = std::make_shared<RuntimeConfig>(1, 0);
  }

  for (int32_t i = 0; i < userConfig->numAdditionalCols_; ++i) {
    auto colName = stringf("Col%o", i);
    table = table.updateView((sn * (i + 1)).as(colName));
  }

  return std::make_pair(std::move(table), std::move(runtimeConfig));
}

DemoCallback::DemoCallback(std::shared_ptr<UserConfig> userConfig,
    std::shared_ptr<RuntimeConfig> runtimeConfig) {
  classicProcessor_ = ClassicProcessor::create(userConfig, runtimeConfig);
  immerProcessor_ = ImmerProcessor::create(std::move(userConfig), std::move(runtimeConfig));
}
DemoCallback::~DemoCallback() = default;

void DemoCallback::onFailure(std::exception_ptr ep) {
  streamf(std::cerr, "Callback reported failure: %o\n", getWhat(std::move(ep)));
}

void DemoCallback::onTick(const ClassicTickingUpdate &update) {
  classicProcessor_->processUpdate(update);
}

void DemoCallback::onTick(ImmerTickingUpdate update) {
  immerProcessor_->post(std::move(update));
}

std::shared_ptr<ClassicProcessor> ClassicProcessor::create(std::shared_ptr<UserConfig> userConfig,
    std::shared_ptr<RuntimeConfig> runtimeConfig) {
  return std::make_shared<ClassicProcessor>(Private(), std::move(userConfig), std::move(runtimeConfig));
}

ClassicProcessor::ClassicProcessor(Private,std::shared_ptr<UserConfig> userConfig,
    std::shared_ptr<RuntimeConfig> runtimeConfig)
    : userConfig_(std::move(userConfig)), runtimeConfig_(std::move(runtimeConfig)) {}

ClassicProcessor::~ClassicProcessor() = default;

void ClassicProcessor::processUpdate(const ClassicTickingUpdate &update) {
  const auto &table = *update.currentTableIndexSpace();
  auto numCols = table.numColumns();
  auto expectedNumColumns = 2 + userConfig_->numAdditionalCols_;
  if (numCols != expectedNumColumns) {
    throw std::runtime_error(stringf("Expected %o columns, got %o", expectedNumColumns, numCols));
  }

  size_t rowsModifiedEstimated = 0;
  size_t cellsModified = 0;
  for (const auto &mods : update.modifiedRowsKeySpace()) {
    rowsModifiedEstimated = std::max(rowsModifiedEstimated, mods->size());
    cellsModified += mods->size();
  }
  statsProcessor_.update(update.removedRowsKeySpace()->size(),
      update.addedRowsKeySpace()->size(), rowsModifiedEstimated, cellsModified);
  processSerialNumber(update);
  size_t elapsedSeconds;
  if (stopwatch_.pulse(&elapsedSeconds)) {
    tableScanner_.scanTable(table);
    statsProcessor_.showStats(numCols, elapsedSeconds);
  }
}

void ClassicProcessor::processSerialNumber(const ClassicTickingUpdate &update) {
  // Do a bunch of work to make sure we received contiguous serial numbers without any skips.
  const auto &table = *update.currentTableIndexSpace();
  auto sni = runtimeConfig_->serialNumberColumn_;
  const auto &serialNumberCol = table.getColumn(sni);

  const auto &addedRows = update.addedRowsIndexSpace();
  const auto &modRowsVec = update.modifiedRowsIndexSpace();
  UInt64Chunk emptyModifiedRows;
  const auto *modifiedRowsToUse = sni < modRowsVec.size() ? &modRowsVec[sni] : &emptyModifiedRows;

  auto totalChunkSize = addedRows.size() + modifiedRowsToUse->size();
  if (totalChunkSize == 0) {
    return;
  }
  auto totalChunk = Int64Chunk::create(totalChunkSize);
  auto addSlice = totalChunk.take(addedRows.size());
  auto modSlice = totalChunk.drop(addedRows.size());

  serialNumberCol->fillChunkUnordered(addedRows, &addSlice);
  serialNumberCol->fillChunkUnordered(*modifiedRowsToUse, &modSlice);

  serialNumberProcessor_.process(&totalChunk);
}

std::shared_ptr<ImmerProcessor> ImmerProcessor::create(std::shared_ptr<UserConfig> userConfig,
    std::shared_ptr<RuntimeConfig> runtimeConfig) {
  auto result = std::make_shared<ImmerProcessor>(Private(), std::move(userConfig),
      std::move(runtimeConfig));
  std::thread t(&runForever, result);
  t.detach();
  return result;
}

ImmerProcessor::ImmerProcessor(Private,std::shared_ptr<UserConfig> userConfig,
    std::shared_ptr<RuntimeConfig> runtimeConfig)
    : userConfig_(std::move(userConfig)), runtimeConfig_(std::move(runtimeConfig)) {}

    ImmerProcessor::~ImmerProcessor() = default;

void ImmerProcessor::post(ImmerTickingUpdate update) {
  mutex_.lock();
  auto empty = todo_.empty();
  todo_.push(std::move(update));
  mutex_.unlock();
  if (empty) {
    condvar_.notify_all();
  }
}

void ImmerProcessor::runForever(std::shared_ptr<ImmerProcessor> self) {
  std::cerr << "Immer processor thread starting up\n";
  try {
    self->runForeverHelper();
  } catch (const std::exception &e) {
    streamf(std::cerr, "Caught exception: %o\n", e.what());
  }
  std::cerr << "Immer processor thread shutting down\n";
}

void ImmerProcessor::runForeverHelper() {
  while (true) {
    std::unique_lock guard(mutex_);
    while (todo_.empty()) {
      condvar_.wait(guard);
    }
    auto update = std::move(todo_.front());
    todo_.pop();
    guard.unlock();

    processUpdate(update);
  }
}

void ImmerProcessor::processUpdate(const ImmerTickingUpdate &update) {
  const auto &table = *update.current();
  auto numCols = table.numColumns();
  auto expectedNumColumns = 2 + userConfig_->numAdditionalCols_;
  if (numCols != expectedNumColumns) {
    throw std::runtime_error(stringf("Expected %o columns, got %o", expectedNumColumns, numCols));
  }
  size_t rowsModifiedEstimated = 0;
  size_t cellsModified = 0;
  for (const auto &mods : update.modified()) {
    rowsModifiedEstimated = std::max(rowsModifiedEstimated, mods->size());
    cellsModified += mods->size();
  }
  statsProcessor_.update(update.removed()->size(),
      update.added()->size(), rowsModifiedEstimated, cellsModified);

  processSerialNumber(update);
  size_t elapsedSeconds;
  if (stopwatch_.pulse(&elapsedSeconds)) {
    tableScanner_.scanTable(table);
    statsProcessor_.showStats(numCols, elapsedSeconds);
    fragmentationAnalyzer_.analyze(*update.current());
  }
}

void ImmerProcessor::processSerialNumber(const ImmerTickingUpdate &update) {
  // Do a bunch of work to make sure we received contiguous serial numbers without any skips.
  const auto &table = *update.current();
  auto sni = runtimeConfig_->serialNumberColumn_;
  const auto &serialNumberCol = table.getColumn(sni);

  const auto &addedRows = update.added();
  const auto &modRowsVec = update.modified();
  auto emptyModifiedRows = RowSequence::createEmpty();
  const auto *modifiedRowsToUse = sni < modRowsVec.size() ? modRowsVec[sni].get() : emptyModifiedRows.get();

  auto totalChunkSize = addedRows->size() + modifiedRowsToUse->size();
  if (totalChunkSize == 0) {
    return;
  }
  auto totalChunk = Int64Chunk::create(totalChunkSize);
  auto addSlice = totalChunk.take(addedRows->size());
  auto modSlice = totalChunk.drop(addedRows->size());

  serialNumberCol->fillChunk(*addedRows, &addSlice);
  serialNumberCol->fillChunk(*modifiedRowsToUse, &modSlice);

  serialNumberProcessor_.process(&totalChunk);
}

void SerialNumberProcessor::process(Int64Chunk *unorderedSerialNumbers) {
  auto &usns = *unorderedSerialNumbers;
  std::sort(usns.begin(), usns.end());
  auto firstSerial = *usns.begin();
  auto lastSerial = *(usns.end() - 1);  // Safe because chunk is not empty.
  if (lastSerial - firstSerial + 1 != usns.size()) {
    throw std::runtime_error(stringf("Serial numbers not contiguous [%o..%o]", firstSerial, lastSerial));
  }
  if (firstSerial != nextExpectedSerial_) {
    throw std::runtime_error(stringf("Expected next serial %o, got %o", nextExpectedSerial_,
        firstSerial));
  }
  nextExpectedSerial_ = lastSerial + 1;
  streamf(std::cout, "next expected serial %o\n", nextExpectedSerial_);
}

Stopwatch::Stopwatch() {
  auto now = std::chrono::steady_clock::now();
  start_ = now;
  last_ = now;
}

bool Stopwatch::pulse(size_t *elapsedSeconds) {
  auto now = std::chrono::steady_clock::now();
  *elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(now - start_).count();
  auto thisPulse = std::chrono::duration_cast<std::chrono::seconds>(now - last_).count();
  if (thisPulse < 5) {
    return false;
  }
  last_ = now;
  return true;
}

void StatsProcessor::update(size_t rowsRemoved, size_t rowsAdded,
    size_t rowsModifiedEstimated, size_t cellsModified) {
  rowsRemoved_ += rowsRemoved;
  rowsAdded_ += rowsAdded;
  rowsModifiedEstimated_ += rowsModifiedEstimated;
  cellsModified_ += cellsModified;
}

void StatsProcessor::showStats(size_t numCols, size_t elapsedSeconds) {
  auto deltaRemoved = rowsRemoved_ - lastRowsRemoved_;
  auto deltaAdded = rowsAdded_ - lastRowsAdded_;
  auto deltaModifiedRows = rowsModifiedEstimated_ - lastRowsModifiedEstimated_;
  auto deltaModifiedCells = cellsModified_ - lastCellsModified_;
  auto totalRows = rowsRemoved_ + rowsAdded_ + rowsModifiedEstimated_;
  auto totalCells = rowsRemoved_ * numCols + rowsAdded_ * numCols + cellsModified_;
  auto rowsPerSec = totalRows / elapsedSeconds;
  auto cellsPerSec = totalCells / elapsedSeconds;

  lastRowsRemoved_ = rowsRemoved_;
  lastRowsAdded_ = rowsAdded_;
  lastRowsModifiedEstimated_ = rowsModifiedEstimated_;
  lastCellsModified_ = cellsModified_;
  streamf(std::cout,
      "removed %o rows, added %o rows, modified %o rows (%o cells) (totals %o, %o, %o, %o). "
      "Total %o rows (%o cells) in %o seconds (%o rows / second) (%o cells / second)\n",
      deltaRemoved, deltaAdded, deltaModifiedRows, deltaModifiedCells,
      rowsRemoved_, rowsAdded_, rowsModifiedEstimated_, cellsModified_,
      totalRows, totalCells, elapsedSeconds, rowsPerSec, cellsPerSec);
}

void TableScanner::scanTable(const Table &table) {
  auto start = std::chrono::system_clock::now();
  auto ncols = table.numColumns();
  auto nrows = table.numRows();
  std::vector<int64_t> results(ncols);
  auto allRows = RowSequence::createSequential(0, nrows);
  for (size_t colNum = 0; colNum < ncols; ++colNum) {
    const auto &col = table.getColumn(colNum);
    auto chunk = ChunkMaker::createChunkFor(*col, nrows);
    col->fillChunk(*allRows, &chunk.unwrap());

    // Assume for now that all the columns are int64_t
    const auto &typedChunk = chunk.get<Int64Chunk>();
    int64_t total = 0;
    for (auto element : typedChunk) {
      total += element;
    }
    results[colNum] = total;
  }
  auto end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  auto durationMillis = (double)duration / 1'000;
  streamf(std::cout, "Table scan took %o millis. Sums are %o\n", durationMillis,
      separatedList(results.begin(), results.end(), ", "));
}

void FragmentationAnalyzer::analyze(const Table &table) {
  for (size_t i = 0; i < table.numColumns(); ++i) {
    const auto &col = table.getColumn(i);
    auto what = stringf("Col %o", i);
    analyzeHelper(what, *col);
  }
}

void FragmentationAnalyzer::analyzeHelper(std::string_view what, const ColumnSource &col) {
  const auto underlying = col.backdoor();
  const auto * const *vecpp = std::any_cast<const immer::flex_vector<int64_t>*>(&underlying);
  if (vecpp == nullptr) {
    return;
  }
  const auto *src = *vecpp;
  std::map<size_t, size_t> sizes;
  auto analyze = [&sizes](const int64_t *begin, const int64_t *end) {
    auto size = end - begin;
    ++sizes[size];
  };
  immer::for_each_chunk(src->begin(), src->end(), analyze);

  auto renderSize = [](std::ostream &s, const std::pair<size_t, size_t> &p) {
    streamf(s, "%o:%o", p.first, p.second);
  };

  streamf(std::cout, "Immer histogram for %o: %o\n", what, separatedList(sizes.begin(), sizes.end(),
      ", ", renderSize));
}

UserConfig::UserConfig(std::string server, bool wantLastBy, bool wantImmer, int64_t period,
    int64_t tableSize, int64_t endSerialNumber, int64_t keyFactor, int64_t numAdditionalCols) :
    server_(std::move(server)), wantLastBy_(wantLastBy), wantImmer_(wantImmer), period_(period),
    tableSize_(tableSize),endSerialNumber_(endSerialNumber), keyFactor_(keyFactor),
    numAdditionalCols_(numAdditionalCols) {}
UserConfig::~UserConfig() = default;

std::string getWhat(std::exception_ptr ep) {
  try {
    std::rethrow_exception(std::move(ep));
  } catch (const std::exception &e) {
    return e.what();
  } catch (...) {
    return "(unknown exception)";
  }
}

std::ostream &operator<<(std::ostream &s, const UserConfig &o) {
  return streamf(s, "server: %o\n"
                    "wantLastBy: %o\n"
                    "wantImmer: %o\n"
                    "period: %o\n"
                    "tableSize: %o\n"
                    "endSerialNumber: %o\n"
                    "keyFactor: %o\n"
                    "numAdditionalCols: %o",
      o.server_, o.wantLastBy_, o.wantImmer_, o.period_, o.tableSize_, o.endSerialNumber_, o.keyFactor_,
      o.numAdditionalCols_);
}
}  // namespace
