/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/ticking/barrage_processor.h"

#include <functional>
#include <iostream>
#include <memory>
#include <tuple>
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/immerutil/abstract_flex_vector.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/ticking.h"
#include "deephaven/dhcore/ticking/immer_table_state.h"
#include "deephaven/dhcore/ticking/index_decoder.h"
#include "deephaven/flatbuf/Barrage_generated.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"
#include "deephaven/third_party/fmt/ranges.h"

using deephaven::dhcore::chunk::AnyChunk;
using deephaven::dhcore::chunk::ChunkMaker;
using deephaven::dhcore::chunk::UInt64Chunk;
using deephaven::dhcore::column::MutableColumnSource;
using deephaven::dhcore::column::ColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::immerutil::AbstractFlexVectorBase;
using deephaven::dhcore::clienttable::Schema;
using deephaven::dhcore::clienttable::ClientTable;
using deephaven::dhcore::utility::MakeReservedVector;
using deephaven::dhcore::utility::separatedList;

using io::deephaven::barrage::flatbuf::BarrageMessageType;
using io::deephaven::barrage::flatbuf::BarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::BarrageModColumnMetadata;
using io::deephaven::barrage::flatbuf::BarrageUpdateMetadata;
using io::deephaven::barrage::flatbuf::ColumnConversionMode;
using io::deephaven::barrage::flatbuf::CreateBarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionOptions;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionRequest;

namespace deephaven::dhcore::ticking {

namespace internal {
class BarrageProcessorImpl;

namespace {
enum class State { kAwaitingMetadata, kAwaitingAdds, kAwaitingModifies, kBuildingResult };

class AwaitingMetadata final {
public:
  explicit AwaitingMetadata(std::shared_ptr<Schema> schema);
  ~AwaitingMetadata();

  [[nodiscard]]
  std::optional<TickingUpdate> ProcessNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata,
      size_t metadata_size);

  [[nodiscard]]
  std::tuple<std::shared_ptr<ClientTable>, std::shared_ptr<RowSequence>, std::shared_ptr<ClientTable>>
  ProcessRemoves(const RowSequence &removed_rows);

  size_t num_cols_ = 0;
  ImmerTableState table_state_;
};

class AwaitingAdds final {
public:
  AwaitingAdds();
  ~AwaitingAdds();

  void Init(std::vector<std::shared_ptr<RowSequence>> per_column_modifies,
      std::shared_ptr<ClientTable> prev,
      std::shared_ptr<RowSequence> removed_rows_index_space,
      std::shared_ptr<ClientTable> after_removes,
      std::shared_ptr<RowSequence> added_rows_index_space);

  [[nodiscard]]
  std::optional<TickingUpdate> ProcessNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata,
      size_t metadata_size);

  void Reset();

  bool first_time_ = true;

  std::vector<std::shared_ptr<RowSequence>> per_column_modifies_;
  std::shared_ptr<ClientTable> prev_;
  std::shared_ptr<RowSequence> removed_rows_index_space_;
  std::shared_ptr<ClientTable> after_removes_;
  std::shared_ptr<RowSequence> added_rows_index_space_;

  std::shared_ptr<RowSequence> added_rows_remaining_;
};

class AwaitingModifies final {
public:
  AwaitingModifies();
  ~AwaitingModifies();

  void Init(std::shared_ptr<ClientTable> after_adds);

  [[nodiscard]]
  std::optional<TickingUpdate> ProcessNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata,
      size_t metadata_size);

  void Reset();

  bool first_time_ = true;

  std::shared_ptr<ClientTable> after_adds_;
  std::vector<std::shared_ptr<RowSequence>> modified_rows_remaining_;
  std::vector<std::shared_ptr<RowSequence>> modified_rows_index_space_;
};

class BuildingResult final {
public:
  BuildingResult();
  ~BuildingResult();

  void Init(std::shared_ptr<ClientTable> after_modifies);

  [[nodiscard]]
  std::optional<TickingUpdate> ProcessNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata,
      size_t metadata_size);

  void Reset();

  std::shared_ptr<ClientTable> afterModifies_;
};

bool AllEmpty(const std::vector<std::shared_ptr<RowSequence>> &row_sequences);
void AssertAllSame(size_t val0, size_t val1, size_t val2);
}  // namespace

class BarrageProcessorImpl final {
public:
  explicit BarrageProcessorImpl(std::shared_ptr<Schema> schema);
  ~BarrageProcessorImpl();

  State state_ = State::kAwaitingMetadata;
  AwaitingMetadata awaitingMetadata_;
  AwaitingAdds awaitingAdds_;
  AwaitingModifies awaitingModifies_;
  BuildingResult buildingResult_;

  [[nodiscard]]
  std::optional<TickingUpdate> ProcessNextChunk(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata,
      size_t metadata_size);
};
}  // namespace internal

BarrageProcessor::BarrageProcessor() = default;
BarrageProcessor::BarrageProcessor(BarrageProcessor &&other) noexcept = default;
BarrageProcessor &BarrageProcessor::operator=(BarrageProcessor &&other) noexcept = default;
BarrageProcessor::BarrageProcessor(std::shared_ptr<Schema> schema) {
  impl_ = std::make_unique<internal::BarrageProcessorImpl>(std::move(schema));
}
BarrageProcessor::~BarrageProcessor() = default;

std::vector<uint8_t> BarrageProcessor::CreateSubscriptionRequest(const void *ticket_bytes, size_t size) {
  // Make a BarrageMessageWrapper
  // ...Whose payload is a BarrageSubscriptionRequest
  // ......which has BarrageSubscriptionOptions
  flatbuffers::FlatBufferBuilder payload_builder(4096);

  auto sub_options = CreateBarrageSubscriptionOptions(payload_builder,
      ColumnConversionMode::ColumnConversionMode_Stringify, true, 0, 4096, 0, true);

  auto ticket = payload_builder.CreateVector(static_cast<const int8_t*>(ticket_bytes), size);
  auto subreq = CreateBarrageSubscriptionRequest(payload_builder, ticket, {}, {}, sub_options);
  payload_builder.Finish(subreq);
  // TODO(kosak): fix sad cast
  const auto *payloadp = static_cast<int8_t*>(static_cast<void*>(payload_builder.GetBufferPointer()));
  const auto payload_size = payload_builder.GetSize();

  // TODO: I'd really like to just point this buffer backwards to the thing I just created, rather
  // then copying it. But, eh, version 2.
  flatbuffers::FlatBufferBuilder wrapper_builder(4096);
  auto payload = wrapper_builder.CreateVector(payloadp, payload_size);
  auto message_wrapper = CreateBarrageMessageWrapper(wrapper_builder, kDeephavenMagicNumber,
      BarrageMessageType::BarrageMessageType_BarrageSubscriptionRequest, payload);
  wrapper_builder.Finish(message_wrapper);
  auto wrapper_buffer = wrapper_builder.Release();

  std::vector<uint8_t> result;
  result.resize(wrapper_buffer.size());
  memcpy(result.data(), wrapper_buffer.data(), wrapper_buffer.size());
  return result;
}

std::string BarrageProcessor::CreateSubscriptionRequestCython(const void *ticket_bytes, size_t size) {
  auto vec = CreateSubscriptionRequest(ticket_bytes, size);
  std::string result;
  result.reserve(vec.size());
  for (auto ch : vec) {
    result.push_back(static_cast<char>(ch));
  }
  return result;
}

std::optional<TickingUpdate> BarrageProcessor::ProcessNextChunk(
    const std::vector<std::shared_ptr<ColumnSource>> &sources,
    const std::vector<size_t> &sizes, const void *metadata, size_t metadata_size) {
  std::vector<size_t> begins(sizes.size());  // init to zeroes.
  return impl_->ProcessNextChunk(sources, &begins, sizes, metadata, metadata_size);
}

namespace internal {
BarrageProcessorImpl::BarrageProcessorImpl(std::shared_ptr<Schema> schema) : state_(State::kAwaitingMetadata),
    awaitingMetadata_(std::move(schema)) {}
BarrageProcessorImpl::~BarrageProcessorImpl() = default;

std::optional<TickingUpdate>
BarrageProcessorImpl::ProcessNextChunk(const std::vector<std::shared_ptr<ColumnSource>> &sources,
    std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadata_size) {
  switch (state_) {
    case State::kAwaitingMetadata:
      return awaitingMetadata_.ProcessNextChunk(this, sources, begins, ends, metadata, metadata_size);
    case State::kAwaitingAdds:
      return awaitingAdds_.ProcessNextChunk(this, sources, begins, ends, metadata, metadata_size);
    case State::kAwaitingModifies:
      return awaitingModifies_.ProcessNextChunk(this, sources, begins, ends, metadata, metadata_size);
    case State::kBuildingResult:
      return buildingResult_.ProcessNextChunk(this, sources, begins, ends, metadata, metadata_size);
    default: {
      auto message = fmt::format("Unknown state {}", static_cast<int>(state_));
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}

namespace {
AwaitingMetadata::AwaitingMetadata(std::shared_ptr<Schema> schema) : num_cols_(schema->NumCols()),
    table_state_(std::move(schema)) {
}
AwaitingMetadata::~AwaitingMetadata() = default;

std::optional<TickingUpdate> AwaitingMetadata::ProcessNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &sources, std::vector<size_t> *begins,
    const std::vector<size_t> &ends, const void *metadata, size_t /*metadata_size*/) {
  if (metadata == nullptr) {
    const char *message = "Metadata was required here, but none was received";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  // This metadata buffer comes from code like flightStreamChunk_->app_metadata->data()
  const auto *barrage_wrapper = flatbuffers::GetRoot<BarrageMessageWrapper>(metadata);

  if (barrage_wrapper->magic() != BarrageProcessor::kDeephavenMagicNumber) {
    auto message = fmt::format("Expected magic number {}, got {}",
        BarrageProcessor::kDeephavenMagicNumber,
        barrage_wrapper->magic());
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  if (barrage_wrapper->msg_type() !=
      BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata) {
    auto message = fmt::format("Expected Barrage Message Type {}, got {}",
        static_cast<int>(BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata),
        static_cast<int>(barrage_wrapper->msg_type()));
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  const auto *bmw_raw = barrage_wrapper->msg_payload()->data();
  const auto *bmd = flatbuffers::GetRoot<BarrageUpdateMetadata>(bmw_raw);

  DataInput di_removed(*bmd->removed_rows());
  DataInput di_three_shift_indices(*bmd->shift_data());
  DataInput di_added(*bmd->added_rows());

  auto removed_rows = IndexDecoder::ReadExternalCompressedDelta(&di_removed);
  auto shift_start_index = IndexDecoder::ReadExternalCompressedDelta(&di_three_shift_indices);
  auto shift_end_index = IndexDecoder::ReadExternalCompressedDelta(&di_three_shift_indices);
  auto shift_dest_index = IndexDecoder::ReadExternalCompressedDelta(&di_three_shift_indices);
  auto added_rows = IndexDecoder::ReadExternalCompressedDelta(&di_added);

  // Disabled because it's too verbose
  if (false) {
    fmt::print(std::cout, "adds={}, removes={}, ss={}, %se={}, sd={}\n", *added_rows, *removed_rows,
        *shift_start_index, *shift_end_index, *shift_dest_index);
  }

  const auto &mod_column_nodes = *bmd->mod_column_nodes();

  std::vector<std::shared_ptr<RowSequence>> per_column_modifies;
  per_column_modifies.reserve(mod_column_nodes.size());
  for (flatbuffers::uoffset_t i = 0; i < mod_column_nodes.size(); ++i) {
    const auto &elt = mod_column_nodes.Get(i);
    DataInput di_modified(*elt->modified_rows());
    auto mod_rows = IndexDecoder::ReadExternalCompressedDelta(&di_modified);
    per_column_modifies.push_back(std::move(mod_rows));
  }

  // Correct order to process Barrage info is:
  // 1. removes
  // 2. shifts
  // 3. adds
  // 4. modifies
  // We have not called with add or modify data yet, but we can do removes and shifts now
  // (steps 1 and 2).
  auto [prev, removedRowsIndexSpace, afterRemoves] = ProcessRemoves(*removed_rows);
  table_state_.ApplyShifts(*shift_start_index, *shift_end_index, *shift_dest_index);

  auto added_rows_index_space = table_state_.AddKeys(*added_rows);

  owner->state_ = State::kAwaitingAdds;
  owner->awaitingAdds_.Init(std::move(per_column_modifies), std::move(prev),
      std::move(removedRowsIndexSpace),
      std::move(afterRemoves), std::move(added_rows_index_space));
  return owner->awaitingAdds_.ProcessNextChunk(owner, sources, begins, ends, nullptr, 0);
}

std::tuple<std::shared_ptr<ClientTable>, std::shared_ptr<RowSequence>, std::shared_ptr<ClientTable>>
AwaitingMetadata::ProcessRemoves(const RowSequence &removed_rows) {
  auto prev = table_state_.Snapshot();
  // The reason we special-case "empty" is because when the tables are unchanged, we prefer
  // to indicate this via pointer equality (e.g. beforeRemoves == afterRemoves).
  std::shared_ptr<RowSequence> removed_rows_index_space;
  std::shared_ptr<ClientTable> after_removes;
  if (removed_rows.Empty()) {
    removed_rows_index_space = RowSequence::CreateEmpty();
    after_removes = prev;
  } else {
    removed_rows_index_space = table_state_.Erase(removed_rows);
    after_removes = table_state_.Snapshot();
  }
  return {std::move(prev), std::move(removed_rows_index_space), std::move(after_removes)};
}

AwaitingAdds::AwaitingAdds() = default;
AwaitingAdds::~AwaitingAdds() = default;

void AwaitingAdds::Reset() {
  *this = AwaitingAdds();
}

void AwaitingAdds::Init(std::vector<std::shared_ptr<RowSequence>> per_column_modifies, std::shared_ptr<ClientTable> prev,
    std::shared_ptr<RowSequence> removed_rows_index_space, std::shared_ptr<ClientTable> after_removes,
    std::shared_ptr<RowSequence> added_rows_index_space) {

  auto result = std::make_shared<AwaitingAdds>();
  per_column_modifies_ = std::move(per_column_modifies);
  prev_ = std::move(prev);
  removed_rows_index_space_ = std::move(removed_rows_index_space);
  after_removes_  = std::move(after_removes);
  added_rows_index_space_ = std::move(added_rows_index_space);
}

std::optional<TickingUpdate> AwaitingAdds::ProcessNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &sources,
    std::vector<size_t> *beginsp, const std::vector<size_t> &ends, const void */*metadata*/, size_t /*metadataSize*/) {
  if (first_time_) {
    first_time_ = false;

    if (added_rows_index_space_->Empty()) {
      added_rows_remaining_ = RowSequence::CreateEmpty();

      auto after_adds = after_removes_;
      owner->state_ = State::kAwaitingModifies;
      owner->awaitingModifies_.Init(std::move(after_adds));
      return owner->awaitingModifies_.ProcessNextChunk(owner, sources, beginsp, ends, nullptr, 0);
    }

    if (owner->awaitingMetadata_.num_cols_ == 0) {
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR("!AddedRows.Empty() but numCols == 0"));
    }

    // Working copy that is consumed in the iterations of the loop.
    added_rows_remaining_ = added_rows_index_space_->Drop(0);
  }

  auto &begins = *beginsp;
  AssertAllSame(sources.size(), begins.size(), ends.size());
  auto num_sources = sources.size();

  if (added_rows_remaining_->Empty()) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Impossible: addedRowsRemaining is Empty"));
  }

  if (begins == ends) {
    // Need more data from caller.
    return {};
  }

  auto chunk_size = ends[0] - begins[0];
  for (size_t i = 1; i != num_sources; ++i) {
    auto this_size = ends[i] - begins[i];
    if (this_size != chunk_size) {
      auto message = fmt::format("Chunks have inconsistent sizes: {} vs {}", this_size, chunk_size);
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }

  if (added_rows_remaining_->Size() < chunk_size) {
    const char *message = "There is excess data in the chunk";
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }

  auto index_rows_this_time = added_rows_remaining_->Take(chunk_size);
  added_rows_remaining_ = added_rows_remaining_->Drop(chunk_size);
  owner->awaitingMetadata_.table_state_.AddData(sources, begins, ends, *index_rows_this_time);

  // To indicate to the caller that we've consumed the data here (so it can't e.g. be passed on to modify)
  for (size_t i = 0; i != num_sources; ++i) {
    begins[i] = ends[i];
  }

  if (!added_rows_remaining_->Empty()) {
    // Need more data from caller.
    return {};
  }

  // No more data remaining. Add phase is done.
  auto after_adds = owner->awaitingMetadata_.table_state_.Snapshot();

  owner->state_ = State::kAwaitingModifies;
  owner->awaitingModifies_.Init(std::move(after_adds));
  return owner->awaitingModifies_.ProcessNextChunk(owner, sources, beginsp, ends, nullptr, 0);
}

void AwaitingModifies::Reset() {
  *this = AwaitingModifies();
}

void AwaitingModifies::Init(std::shared_ptr<ClientTable> after_adds) {
  after_adds_ = std::move(after_adds);
}

AwaitingModifies::AwaitingModifies() = default;
AwaitingModifies::~AwaitingModifies() = default;

std::optional<TickingUpdate> AwaitingModifies::ProcessNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &sources, std::vector<size_t> *beginsp,
    const std::vector<size_t> &ends, const void *metadata, size_t metadata_size) {

  if (first_time_) {
    first_time_ = false;

    if (AllEmpty(owner->awaitingAdds_.per_column_modifies_)) {
      modified_rows_index_space_ = {};
      modified_rows_remaining_ = {};
      auto after_modifies = after_adds_;
      owner->state_ = State::kBuildingResult;
      owner->buildingResult_.Init(std::move(after_modifies));
      return owner->buildingResult_.ProcessNextChunk(owner, sources, beginsp, ends, metadata,
          metadata_size);
    }

    auto ncols = owner->awaitingMetadata_.num_cols_;
    modified_rows_index_space_ = MakeReservedVector<std::shared_ptr<RowSequence>>(ncols);
    modified_rows_remaining_ = MakeReservedVector<std::shared_ptr<RowSequence>>(ncols);
    for (size_t i = 0; i < ncols; ++i) {
      auto rs = owner->awaitingMetadata_.table_state_.ConvertKeysToIndices(
          *owner->awaitingAdds_.per_column_modifies_[i]);
      modified_rows_index_space_.push_back(rs->Drop(0));  // make copy
      modified_rows_remaining_.push_back(std::move(rs));
    }
  }

  if (AllEmpty(modified_rows_remaining_)) {
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR("Impossible: modifiedRowsRemaining is empty"));
  }
  auto &begins = *beginsp;

  if (begins == ends) {
    // Need more data from caller.
    return {};
  }

  auto num_sources = sources.size();
  if (num_sources > modified_rows_remaining_.size()) {
    auto message = fmt::format("Number of sources ({}) greater than expected ({})", num_sources,
                           modified_rows_remaining_.size());
    throw std::runtime_error(message);
  }

  for (size_t i = 0; i < num_sources; ++i) {
    auto num_rows_remaining = modified_rows_remaining_[i]->Size();
    auto num_rows_available = ends[i] - begins[i];

    if (num_rows_available > num_rows_remaining) {
      auto message = fmt::format("col {}: numRowsAvailable > numRowsRemaining ({} > {})",
                             i, num_rows_available, num_rows_remaining);
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }

    if (num_rows_available == 0) {
      // Nothing available for this column. Advance to next column.
      continue;
    }

    auto &mr = modified_rows_remaining_[i];
    auto rows_available = mr->Take(num_rows_available);
    mr = mr->Drop(num_rows_available);

    owner->awaitingMetadata_.table_state_.ModifyData(i, *sources[i], begins[i], ends[i],
        *rows_available);
    begins[i] = ends[i];
  }

  for (const auto &mr : modified_rows_remaining_) {
    if (!mr->Empty()) {
      // Return an indication that at least one column is hungry for more data
      return {};
    }
  }

  // No more data. Modify phase is done.
  auto after_modifies = owner->awaitingMetadata_.table_state_.Snapshot();

  owner->state_ = State::kBuildingResult;
  owner->buildingResult_.Init(std::move(after_modifies));
  return owner->buildingResult_.ProcessNextChunk(owner, sources, beginsp, ends, nullptr, 0);
}

BuildingResult::BuildingResult() = default;
BuildingResult::~BuildingResult() = default;

void BuildingResult::Reset() {
  *this = BuildingResult();
}

void BuildingResult::Init(std::shared_ptr<ClientTable> after_modifies) {
  afterModifies_ = std::move(after_modifies);
}

std::optional<TickingUpdate> BuildingResult::ProcessNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &/*sources*/,
    std::vector<size_t> *beginsp, const std::vector<size_t> &ends, const void */*metadata*/, size_t /*metadataSize*/) {
  if (*beginsp != ends) {
    auto message = fmt::format(
        "Barrage logic is done processing but there is leftover caller-provided data. begins = [{}]. ends=[{}]",
        *beginsp, ends);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
  auto *aa = &owner->awaitingAdds_;
  auto *am = &owner->awaitingModifies_;
  auto result = TickingUpdate(std::move(aa->prev_),
      std::move(aa->removed_rows_index_space_), std::move(aa->after_removes_),
      std::move(aa->added_rows_index_space_), std::move(am->after_adds_),
      std::move(am->modified_rows_index_space_), std::move(afterModifies_));
  aa->Reset();
  am->Reset();
  this->Reset();
  owner->state_ = State::kAwaitingMetadata;
  return result;
}

bool AllEmpty(const std::vector<std::shared_ptr<RowSequence>> &row_sequences) {
  return std::all_of(row_sequences.begin(), row_sequences.end(), [](const auto &rs) { return rs->Empty(); });
}

void AssertAllSame(size_t val0, size_t val1, size_t val2) {
  if (val0 != val1 || val0 != val2) {
    auto message = fmt::format("Sizes differ: {} vs {} vs {}", val0, val1, val2);
    throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
  }
}
}  // namespace
}  // namespace internal
}  // namespace deephaven::dhcore::ticking
