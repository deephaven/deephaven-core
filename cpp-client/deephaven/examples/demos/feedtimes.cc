/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include "deephaven/client/client.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::column::DateTimeColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::utility::VerboseCast;

using my_clock = std::chrono::high_resolution_clock;
using my_time_point = my_clock::time_point;

namespace {
void Run(const TableHandleManager &manager, const char *table_name, const char *column_name, int period_seconds);
}  // namespace

int main(int argc, char *argv[]) {
  try {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <host:port> <table-name> <timestamp-column-name> <period-seconds>\n";
        std::exit(1);
      }

    int c = 0;
    const char *server = argv[c++];
    const char *table_name = argv[c++];
    const char *col_name = argv[c++];
    const int period_seconds = std::stoi(argv[c++]);
    auto client = Client::Connect(server);
    auto manager = client.GetManager();

    Run(manager, table_name, col_name, period_seconds);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
    std::exit(1);
  }
  return 0;
}

namespace {

std::string FormatDuration(std::chrono::nanoseconds nanos) {
  using namespace std::chrono;
  auto ns = nanos.count();
  auto h = duration_cast<hours>(nanos);
  ns -= duration_cast<nanoseconds>(h).count();
  auto m = duration_cast<minutes>(nanoseconds(ns));
  ns -= duration_cast<nanoseconds>(m).count();
  auto s = duration_cast<seconds>(nanoseconds(ns));
  ns -= duration_cast<nanoseconds>(s).count();
  auto ms = duration_cast<milliseconds>(nanoseconds(ns));
  ns -= duration_cast<nanoseconds>(ms).count();
  auto us = duration_cast<microseconds>(nanoseconds(ns));
  ns -= duration_cast<nanoseconds>(us).count();

  std::stringstream ss;
  if (h.count() > 0) {
    ss << std::setw(2) << std::setfill('0') << h.count() << 'h';
  }
  if (m.count() > 0 || h.count() > 0) {
    ss << std::setw(2) << std::setfill('0') << m.count() << 'm';
  }
  ss << std::setw(2) << std::setfill('0') << s.count() << '.'
     << std::setw(3) << std::setfill('0') << ms.count()
     << 's'
    ;
  return ss.str();
}

class TrackTimeCallback final : public deephaven::dhcore::ticking::TickingCallback {
public:
  TrackTimeCallback(const char *col_name) : col_name_(col_name) {
    ResetMinMax();
  }

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    std::unique_lock lock(mux_);
    ++tick_count_;
    auto col_index = *update.Current()->GetColumnIndex(col_name_, true);

    ProcessDeltas(*update.AfterAdds(), col_index, update.AddedRows());

    size_t num_modifies = 0;
    if (update.ModifiedRows().size() > col_index) {
      auto row_sequence = update.ModifiedRows()[col_index];
      ProcessDeltas(*update.AfterModifies(), col_index, row_sequence);
    }
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }

  void DumpStats(
      my_time_point &last_time,
      std::uint64_t &last_tick_count) {
    const my_time_point now = my_clock::now();
    DateTime min, max;
    std::uint64_t tick_count = ReadAndResetMinMax(min, max);

    const std::string dt_str = FormatDuration(now - last_time);
    if (last_tick_count == tick_count) {
      std::cerr << "WARNING: No ticks for the last " << dt_str << "\n";  // cerr is auto flushed
    } else {
      std::cout << "Stats for the last " << dt_str
                << ": min=" << min
                << ", max=" << max
                << "."
                // we actually want the flush, so we use endl instead of '\n'.
                << std::endl;
    }
    last_time = now;
    last_tick_count = tick_count;
  }

  void ProcessDeltas(
      const ClientTable &table,
      const size_t col_index,
      std::shared_ptr<RowSequence> rows) {
    const auto generic_datetime_col = table.GetColumn(col_index);
    const auto *typed_datetime_col =
        VerboseCast<const DateTimeColumnSource*>(DEEPHAVEN_LOCATION_EXPR(generic_datetime_col.get()));

    constexpr const size_t kChunkSize = 8192;
    auto data_chunk = DateTimeChunk::Create(kChunkSize);

    while (!rows->Empty()) {
      auto these_rows = rows->Take(kChunkSize);
      auto these_rows_size = these_rows->Size();
      rows = rows->Drop(kChunkSize);

      typed_datetime_col->FillChunk(*these_rows, &data_chunk, nullptr);

      for (size_t i = 0; i < these_rows_size; ++i) {
        auto v = data_chunk.data()[i];
        if (DateTime::isNull(v)) {
          continue;
        }
        if (v < min_) {
          min_ = v;
        } else if (v > max_) {
          max_ = v;
        }
      }
    }
  }

private:
  void ResetMinMax() {
    min_ = DateTime::Max();
    max_ = DateTime::Min();
  }

  std::uint64_t ReadAndResetMinMax(DateTime &min, DateTime &max) {
    std::unique_lock lock(mux_);
    min = min_;
    max = max_;
    ResetMinMax();
    return tick_count_;
  }

private:
  mutable std::mutex mux_;
  const char *col_name_;
  DateTime min_;
  DateTime max_;
  std::uint64_t tick_count_;
};

void Run(
    const TableHandleManager &manager,
    const char *table_name,
    const char *col_name,
    const int period_seconds) {
  auto table = manager.FetchTable(table_name);
  auto callback = std::make_shared<TrackTimeCallback>(col_name);
  auto cookie = table.Subscribe(std::move(callback));
  
  my_time_point last_time = my_clock::now();
  std::uint64_t last_tick_count = 0;
  // Will run until interrupted by kill.
  auto period = std::chrono::seconds(period_seconds);
  while (true) {
    std::this_thread::sleep_for(period);
    callback->DumpStats(last_time, last_tick_count);
  }
}

}  // namespace
