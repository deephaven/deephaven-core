/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/ticking/ticking.h"

namespace deephaven::dhcore::ticking {
namespace internal {
class BarrageProcessorImpl;
}  // namespace internal

class BarrageProcessor final {
protected:
  typedef deephaven::dhcore::clienttable::Schema Schema;
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;

public:
  static constexpr const uint32_t deephavenMagicNumber = 0x6E687064U;

  static std::vector<uint8_t> createSubscriptionRequest(const void *ticketBytes, size_t size);
  /**
   * Returning a 'string' type makes life in Cython slightly easier.
   */
  static std::string createSubscriptionRequestCython(const void *ticketBytes, size_t size);

  BarrageProcessor();
  explicit BarrageProcessor(std::shared_ptr<Schema> schema);
  BarrageProcessor(BarrageProcessor &&other) noexcept;
  BarrageProcessor &operator=(BarrageProcessor &&other) noexcept;
  ~BarrageProcessor();

  std::optional<TickingUpdate> processNextChunk(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      const std::vector<size_t> &sizes, const void *metadata, size_t metadataSize);

private:
  std::unique_ptr<internal::BarrageProcessorImpl> impl_;
};
}  // namespace deephaven::dhcore::ticking
