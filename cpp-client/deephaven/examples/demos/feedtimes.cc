/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <chrono>
#include <condition_variable>
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
using my_duration = my_clock::duration;

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

const char *musecs = "\u03bcs";

std::string FormatMicros(const uint64_t us) {
  const uint64_t ms = us / 1'000UL;
  const uint64_t rest_us = us % 1'000UL;
  std::stringstream ss;
  ss << ms;
  if (rest_us > 0) {
    ss << '.' << std::setw(3) << std::setfill('0') << rest_us;
  }
  ss << "ms (";
  ss << us << musecs << ")";
  return ss.str();
}

DateTime TimePoint2DateTime(const my_time_point &tp) {
  using namespace std::chrono;
  return DateTime::FromNanos(duration_cast<nanoseconds>(tp.time_since_epoch()).count());
}

my_time_point DateTime2TimePoint(const DateTime &dt) {
  using namespace std::chrono;
  const nanoseconds ns(dt.Nanos());
  return my_time_point(ns);
}

uint64_t usecs(const my_duration &d) {
  using namespace std::chrono;
  return duration_cast<microseconds>(d).count();
}

class TrackTimeCallback final : public deephaven::dhcore::ticking::TickingCallback {
public:
  TrackTimeCallback(const char *col_name)
    : col_name_(col_name)
    , tick_count_(0UL)
    , initial_done_(false)
  {
    Reset();
  }

  virtual ~TrackTimeCallback() {}

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    const my_time_point recv_ts = my_clock::now();
    std::unique_lock lock(mux_);
    ++tick_count_;
    auto col_index = *update.Current()->GetColumnIndex(col_name_, true);

    ProcessDeltas(recv_ts, *update.AfterAdds(), col_index, update.AddedRows());

    if (update.ModifiedRows().size() > col_index) {
      auto row_sequence = update.ModifiedRows()[col_index];
      ProcessDeltas(recv_ts, *update.AfterModifies(), col_index, row_sequence);
    }

    if (!initial_done_) {
      initial_done_ = true;
      cond_.notify_all();
    }
  }

  void OnFailure(std::exception_ptr ep) final {
    try {
      std::rethrow_exception(ep);
    } catch (const std::runtime_error &e) {
      std::cout << "Caught error: " << e.what() << std::endl;
    }
  }
  
  void WaitForInitialSnapshot() {
    std::unique_lock lock(mux_);
    cond_.wait(lock, [this] { return initial_done_; });
  }

  void DumpStats(
      my_time_point &last_time,
      std::uint64_t &last_tick_count) {
    const my_time_point now = my_clock::now();
    DateTime min, max;
    my_duration min_delay, max_delay;
    std::uint64_t tick_count = ReadAndReset(min, max, min_delay, max_delay);
    const uint64_t min_delay_us = usecs(min_delay);
    const uint64_t max_delay_us = usecs(max_delay);

    const std::string dt_str = FormatDuration(now - last_time);
    const DateTime now_datetime = TimePoint2DateTime(now);
    if (last_tick_count == tick_count) {
      std::cerr << "WARNING: No ticks for the last " << dt_str << "\n";  // cerr is auto flushed
    } else {
      std::cout << now_datetime;
      if (last_tick_count == 0) {
        std::cout << " Initial stats";
      } else {
        std::cout << " Stats for the last " << dt_str;
      }
      try {
        std::cout << ": min(" << col_name_ << ")=" << min;
        if (last_tick_count != 0) {
          std::cout << ", min_delay=" << FormatMicros(min_delay_us);
        }
      } catch (const std::runtime_error &ex) {
        std::cout << "invalid_time(" << min.Nanos() << ")";
      }
      try {
        std::cout << ", max(" << col_name_ << ")=" << max;
        if (last_tick_count != 0) {
          std::cout << ", max_delay=" << FormatMicros(max_delay_us);
        }
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
      const my_time_point recv_ts,
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
        min_delay_ = recv_ts - DateTime2TimePoint(lmin);
      }
      if (lmax > max_) {
        max_ = lmax;
        max_delay_ = recv_ts - DateTime2TimePoint(lmax);
      }
    }
  }

private:
  void Reset() {
    min_ = DateTime::Max();
    max_ = DateTime::Min();
    min_delay_ = my_duration::max();
    max_delay_ = my_duration::min();
  }

  std::uint64_t ReadAndReset(DateTime &min, DateTime &max, my_duration& min_delay, my_duration& max_delay) {
    std::unique_lock lock(mux_);
    min = min_;
    max = max_;
    min_delay = min_delay_;
    max_delay = max_delay_;
    Reset();
    return tick_count_;
  }

private:
  mutable std::mutex mux_;
  mutable std::condition_variable cond_;
  const char *col_name_;
  DateTime min_;
  DateTime max_;
  my_duration min_delay_;
  my_duration max_delay_;
  std::uint64_t tick_count_;
  bool initial_done_;
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
  callback->WaitForInitialSnapshot();
  callback->DumpStats(last_time, last_tick_count);
  while (true) {
    std::this_thread::sleep_for(period);
    callback->DumpStats(last_time, last_tick_count);
  }
}

}  // namespace
