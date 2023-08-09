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
  using Schema = deephaven::dhcore::clienttable::Schema;
  using ColumnSource = deephaven::dhcore::column::ColumnSource;

public:
  static constexpr const uint32_t kDeephavenMagicNumber = 0x6E687064U;

  [[nodiscard]]
  static std::vector<uint8_t> CreateSubscriptionRequest(const void *ticket_bytes, size_t size);
  /**
   * Returning a 'string' type makes life in Cython slightly easier.
   */
  [[nodiscard]]
  static std::string CreateSubscriptionRequestCython(const void *ticket_bytes, size_t size);

  BarrageProcessor();
  explicit BarrageProcessor(std::shared_ptr<Schema> schema);
  BarrageProcessor(BarrageProcessor &&other) noexcept;
  BarrageProcessor &operator=(BarrageProcessor &&other) noexcept;
  ~BarrageProcessor();

  [[nodiscard]]
  std::optional<TickingUpdate> ProcessNextChunk(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      const std::vector<size_t> &sizes, const void *metadata, size_t metadata_size);

private:
  std::unique_ptr<internal::BarrageProcessorImpl> impl_;
};
}  // namespace deephaven::dhcore::ticking
