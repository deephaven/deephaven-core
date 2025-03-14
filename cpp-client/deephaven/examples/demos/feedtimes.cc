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
  // avoid absl log initialization warnings
  char grpc_env[] = "GRPC_VERBOSITY=ERROR";
  putenv(grpc_env);
  try {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <host:port> <table-name> <timestamp-column-name> <period-seconds>\n";
        std::exit(1);
      }

    int c = 0;
    const char *server = argv[++c];
    const char *table_name = argv[++c];
    const char *col_name = argv[++c];
    const int period_seconds = std::stoi(argv[++c]);
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

  std::stringstream ss;
  if (h.count() > 0) {
    ss << h.count() << 'h';
  }
  if (m.count() > 0 || h.count() > 0) {
    if (h.count() > 0) {
      ss << std::setw(2) << std::setfill('0');
    }
    ss << m.count() << 'm';
  }
  if (m.count() > 0) {
    ss << std::setw(2) << std::setfill('0');
  }
  ss << s.count() << '.'
     << std::setw(3) << std::setfill('0') << ms.count()
     << 's'
    ;
  return ss.str();
}

DateTime TimePoint2DateTime(const my_time_point &tp) {
  using namespace std::chrono;
  return DateTime::FromNanos(duration_cast<nanoseconds>(tp.time_since_epoch()).count());
}
  
class TrackTimeCallback final : public deephaven::dhcore::ticking::TickingCallback {
public:
  TrackTimeCallback(const char *col_name)
    : mux_()
    , col_name_(col_name)
    , min_(DateTime::Max())
    , max_(DateTime::Min())
    , tick_count_(0UL)
  {}

  virtual ~TrackTimeCallback() {}

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    std::unique_lock lock(mux_);
    ++tick_count_;
    auto col_index = *update.Current()->GetColumnIndex(col_name_, true);

    ProcessDeltas(*update.AfterAdds(), col_index, update.AddedRows());

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
    const DateTime now_datetime = TimePoint2DateTime(now);
    if (last_tick_count == tick_count) {
      std::cerr << "WARNING: No ticks for the last " << dt_str << "\n";  // cerr is auto flushed
    } else {
      std::cout << now_datetime << " Stats for the last " << dt_str;
      try {
        std::cout << ": min(" << col_name_ << ")=" << min;
      } catch (const std::runtime_error &ex) {
        std::cout << "invalid_time(" << min.Nanos() << ")";
      }
      try {
        std::cout << ", max(" << col_name_ << ")=" << max;
      } catch (const std::runtime_error &ex) {
        std::cout << "invalid_time(" << max.Nanos() << ")";
      }
      std::cout << "."
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

      auto data = data_chunk.data();

      DateTime v;
      size_t j;;
      // Find first non_null value
      for (j = 0; j < these_rows_size; ++j) {
        v = data[j];
        if (!DateTime::isNull(v)) {
          break;
        }
      }

      if (j >= these_rows_size) {
        continue;
      }

      DateTime lmin = v;
      DateTime lmax = v;

      for (size_t i = j; i < these_rows_size; ++i) {
        v = data[i];
        if (DateTime::isNull(v)) {
          continue;
        }
        if (v < lmin) {
          lmin = v;
        } else if (v > lmax) {
          lmax = v;
        }
      }

      if (lmin < min_) {
        min_ = lmin;
      }
      if (lmax > max_) {
        max_ = lmax;
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
  auto cookie = table.Subscribe(callback);
  
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
