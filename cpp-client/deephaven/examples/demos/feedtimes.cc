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
#include <optional>
#include <sstream>
#include <string>
#include <thread>

#include "deephaven/client/client.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/dhcore/utility/time.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::column::DateTimeColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::utility::VerboseCast;
using deephaven::dhcore::utility::ToDateTime;
using deephaven::dhcore::utility::ToTimePoint;
using deephaven::dhcore::utility::FormatDuration;

using my_clock = std::chrono::high_resolution_clock;
using my_time_point = my_clock::time_point;
using my_duration = my_clock::duration;

namespace {
void Run(const TableHandleManager &manager, const char *table_name, const char *col_name, int period_seconds);
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

uint64_t ToMicros(const my_duration &d) {
  using namespace std::chrono;
  return duration_cast<microseconds>(d).count();
}

std::string FormatMicros(const my_duration &d) {
  return FormatMicros(ToMicros(d));
}
  
class TrackTimeCallback final : public deephaven::dhcore::ticking::TickingCallback {
public:
  explicit TrackTimeCallback(const char *col_name)
    : col_name_(col_name)
    , update_idx_(0UL)
    , initial_done_(false)
  {
    Reset();
  }

  ~TrackTimeCallback() final = default;

  void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
    const my_time_point recv_ts = my_clock::now();
    std::unique_lock lock(mux_);
    ++update_idx_;
    ++nupdates_;
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

  void DumpStats(my_time_point &last_time) {
    const my_time_point now = my_clock::now();
    std::optional<DateTime> min, max;
    my_duration min_delay, max_delay;
    uint64_t nrows, nupdates;
    bool initial;
    ReadAndReset(initial, min, max, min_delay, max_delay, nrows, nupdates);

    const std::string dt_str = FormatDuration(now - last_time);
    const DateTime now_datetime = ToDateTime(now);
    std::cout << now_datetime;
    if (nrows == 0) {
      std::cerr << " WARNING: No updates for the last " << dt_str << "\n";  // cerr is auto flushed
    } else {
      if (initial) {
        std::cout << " Initial snapshot stats";
      } else {
        std::cout << " Stats for the last " << dt_str;
      }
      std::cout << ": " << nrows << " row";
      if (nrows != 1) {
        std::cout << "s";
      }
      if (!initial) {
        std::cout << " in " << nupdates << " update";
        if (nupdates != 1) {
          std::cout << "s";
        }
      }
      try {
        std::cout << ", min(" << col_name_ << ")=" << *min;
        if (!initial) {
          std::cout << ", min_delay=" << FormatMicros(min_delay);
        }
      } catch (const std::runtime_error &ex) {
        std::cout << "invalid_time(" << min->Nanos() << ")";
      }
      try {
        std::cout << ", max(" << col_name_ << ")=" << *max;
        if (!initial) {
          std::cout << ", max_delay=" << FormatMicros(max_delay);
        }
      } catch (const std::runtime_error &ex) {
        std::cout << "invalid_time(" << max->Nanos() << ")";
      }
      std::cout << "."
                // we actually want the flush, so we use endl instead of '\n'.
                << std::endl;
    }
    last_time = now;
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
    auto null_chunk = BooleanChunk::Create(kChunkSize);

    while (!rows->Empty()) {
      auto these_rows = rows->Take(kChunkSize);
      auto these_rows_size = these_rows->Size();
      nrows_ += these_rows_size;
      rows = rows->Drop(kChunkSize);

      typed_datetime_col->FillChunk(*these_rows, &data_chunk, &null_chunk);

      const auto *data = data_chunk.data();
      const auto *nulls = null_chunk.data();

      int64_t v;
      size_t j;
      // Find first non_null value
      for (j = 0; j < these_rows_size; ++j) {
        if (!nulls[j]) {
          v = data[j].Nanos();
          break;
        }
      }

      if (j >= these_rows_size) {
        continue;
      }

      int64_t lmin = v;
      int64_t lmax = v;

      for (size_t i = j; i < these_rows_size; ++i) {
        if (nulls[i]) {
          continue;
        }
        v = data[i].Nanos();
        if (v < lmin) {
          lmin = v;
        } else if (v > lmax) {
          lmax = v;
        }
      }

      if (!min_.has_value() || lmin < min_->Nanos()) {
        min_ = DateTime::FromNanos(lmin);
        min_delay_ = recv_ts - ToTimePoint<my_clock, my_duration>(*min_);
      }
      if (!max_.has_value() || lmax > max_->Nanos()) {
        max_ = DateTime::FromNanos(lmax);
        max_delay_ = recv_ts - ToTimePoint<my_clock, my_duration>(*max_);
      }
    }
  }

private:
  void Reset() {
    min_ = std::nullopt;
    max_ = std::nullopt;
    min_delay_ = my_duration::max();
    max_delay_ = my_duration::min();
    nrows_ = 0UL;
    nupdates_ = 0UL;
  }

  void ReadAndReset(bool &initial,
                    std::optional<DateTime> &min, std::optional<DateTime> &max,
                    my_duration &min_delay, my_duration &max_delay,
                    uint64_t &nrows, uint64_t &nupdates) {
    std::unique_lock lock(mux_);
    initial = update_idx_ == 1UL;
    min = min_;
    max = max_;
    min_delay = min_delay_;
    max_delay = max_delay_;
    nrows = nrows_;
    nupdates = nupdates_;
    Reset();
  }

  mutable std::mutex mux_;
  mutable std::condition_variable cond_;
  const char *col_name_;
  std::optional<DateTime> min_;
  std::optional<DateTime> max_;
  my_duration min_delay_;
  my_duration max_delay_;
  std::uint64_t update_idx_ = 0;
  std::uint64_t nrows_ = 0;
  std::uint64_t nupdates_ = 0;
  bool initial_done_ = false;
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
  // Will run until interrupted by kill.
  auto period = std::chrono::seconds(period_seconds);
  callback->WaitForInitialSnapshot();
  callback->DumpStats(last_time);
  while (true) {
    std::this_thread::sleep_for(period);
    callback->DumpStats(last_time);
  }
}

}  // namespace
