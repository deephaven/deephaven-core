/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/table/schema.h"
#include "deephaven/dhcore/ticking/ticking.h"

namespace deephaven::dhcore::ticking {
class BarrageUpdateProcessor {
protected:
  typedef deephaven::dhcore::chunk::Chunk Chunk;
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;
  typedef deephaven::dhcore::container::RowSequence RowSequence;
  typedef deephaven::dhcore::table::Schema Schema;
  typedef deephaven::dhcore::table::Table Table;

public:
  BarrageUpdateProcessor();
  virtual ~BarrageUpdateProcessor();

  virtual std::optional<TickingUpdate> processNextSlice(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *beginsp, const std::vector<size_t> &ends) = 0;

private:
};

class BarrageProcessor {
protected:
  typedef deephaven::dhcore::table::Schema Schema;

public:
  static constexpr const uint32_t deephavenMagicNumber = 0x6E687064U;

  static std::vector<uint8_t> createBarrageSubscriptionRequest(const std::vector<int8_t> &ticketBytes);

  static std::shared_ptr<BarrageProcessor> create(std::shared_ptr<Schema> schema);
  BarrageProcessor();
  virtual ~BarrageProcessor();

  virtual std::shared_ptr<BarrageUpdateProcessor> startNextUpdate(const void *metadataBuffer, size_t metadataSize) = 0;
};
}  // namespace deephaven::dhcore::ticking
