/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/shift_processor.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::subscription {
void ShiftProcessor::applyShiftData(const RowSequence &firstIndex, const RowSequence &lastIndex,
    const RowSequence &destIndex,
    const std::function<void(uint64_t, uint64_t, uint64_t)> &processShift) {
  if (firstIndex.empty()) {
    return;
  }

  // Loop twice: once in the forward direction (applying negative shifts), and once in the reverse
  // direction (applying positive shifts). Because we don't have a reverse iterator at the moment,
  // we save up the reverse tuples for processing in a separate step.
  std::vector <std::tuple<size_t, size_t, size_t>> positiveShifts;
  auto startIter = firstIndex.getRowSequenceIterator();
  auto endIter = lastIndex.getRowSequenceIterator();
  auto destIter = destIndex.getRowSequenceIterator();
  auto showMessage = [](size_t first, size_t last, size_t dest) {
//    const char *which = dest >= last ? "positive" : "negative";
//    streamf(std::cerr, "Processing %o shift src [%o..%o] dest %o\n", which, first, last, dest);
  };
  {
    uint64_t first, last, dest;
    while (startIter.tryGetNext(&first)) {
      if (!endIter.tryGetNext(&last) || !destIter.tryGetNext(&dest)) {
        const char *message = "Sequences not of same size";
        throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
      }
      if (dest >= first) {
        positiveShifts.emplace_back(first, last, dest);
        continue;
      }
      showMessage(first, last, dest);
      processShift(first, last, dest);
    }
  }

  for (auto ip = positiveShifts.rbegin(); ip != positiveShifts.rend(); ++ip) {
    auto first = std::get<0>(*ip);
    auto last = std::get<1>(*ip);
    auto dest = std::get<2>(*ip);
    showMessage(first, last, dest);
    processShift(first, last, dest);
  }
}
}  // namespace deephaven::client::subscription
