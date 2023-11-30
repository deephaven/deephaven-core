/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/ticking/shift_processor.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"

namespace deephaven::dhcore::subscription {
void ShiftProcessor::ApplyShiftData(const RowSequence &first_index, const RowSequence &last_index,
    const RowSequence &dest_index,
    const std::function<void(uint64_t, uint64_t, uint64_t)> &process_shift) {
  if (first_index.Empty()) {
    return;
  }

  // Loop twice: once in the forward direction (applying negative shifts), and once in the reverse
  // direction (applying positive shifts). Because we don't have a reverse iterator at the moment,
  // we save up the reverse tuples for processing in a separate step.
  std::vector <std::tuple<size_t, size_t, size_t>> positive_shifts;
  auto start_iter = first_index.GetRowSequenceIterator();
  auto end_iter = last_index.GetRowSequenceIterator();
  auto dest_iter = dest_index.GetRowSequenceIterator();
  auto show_message = [](size_t first, size_t last, size_t dest) {
    // Disabled because it's too verbose
    if (false) {
      const char *which = dest >= last ? "positive" : "negative";
      fmt::print(std::cerr, "Processing {} shift src [{}..{}] dest {}\n", which, first, last, dest);
    }
  };
  {
    uint64_t first;
    uint64_t last;
    uint64_t dest;
    while (start_iter.TryGetNext(&first)) {
      if (!end_iter.TryGetNext(&last) || !dest_iter.TryGetNext(&dest)) {
        const char *message = "Sequences not of same Size";
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }
      if (dest >= first) {
        positive_shifts.emplace_back(first, last, dest);
        continue;
      }
      show_message(first, last, dest);
      process_shift(first, last, dest);
    }
  }

  for (auto ip = positive_shifts.rbegin(); ip != positive_shifts.rend(); ++ip) {
    auto first = std::get<0>(*ip);
    auto last = std::get<1>(*ip);
    auto dest = std::get<2>(*ip);
    show_message(first, last, dest);
    process_shift(first, last, dest);
  }
}
}  // namespace deephaven::dhcore::subscription
