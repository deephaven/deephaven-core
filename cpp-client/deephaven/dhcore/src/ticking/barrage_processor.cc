/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
using deephaven::dhcore::utility::makeReservedVector;
using deephaven::dhcore::utility::streamf;
using deephaven::dhcore::utility::stringf;

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
enum class State { AwaitingMetadata, AwaitingAdds, AwaitingModifies, BuildingResult };

class AwaitingMetadata final {
public:
  explicit AwaitingMetadata(std::shared_ptr<Schema> schema);
  ~AwaitingMetadata();

  std::optional<TickingUpdate> processNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadataSize);

  std::tuple<std::shared_ptr<ClientTable>, std::shared_ptr<RowSequence>, std::shared_ptr<ClientTable>> processRemoves(
      const RowSequence &removedRows);

  size_t numCols_ = 0;
  ImmerTableState tableState_;
};

class AwaitingAdds final {
public:
  AwaitingAdds();
  ~AwaitingAdds();

  void init(std::vector<std::shared_ptr<RowSequence>> perColumnModifies,
      std::shared_ptr<ClientTable> prev,
      std::shared_ptr<RowSequence> removedRowsIndexSpace,
      std::shared_ptr<ClientTable> afterRemoves,
      std::shared_ptr<RowSequence> addedRowsIndexSpace);

  std::optional<TickingUpdate> processNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadataSize);

  void reset();

  bool firstTime_ = true;

  std::vector<std::shared_ptr<RowSequence>> perColumnModifies_;
  std::shared_ptr<ClientTable> prev_;
  std::shared_ptr<RowSequence> removedRowsIndexSpace_;
  std::shared_ptr<ClientTable> afterRemoves_;
  std::shared_ptr<RowSequence> addedRowsIndexSpace_;

  std::shared_ptr<RowSequence> addedRowsRemaining_;
};

class AwaitingModifies final {
public:
  AwaitingModifies();
  ~AwaitingModifies();

  void init(std::shared_ptr<ClientTable> afterAdds);

  std::optional<TickingUpdate> processNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadataSize);

  void reset();

  bool firstTime_ = true;

  std::shared_ptr<ClientTable> afterAdds_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRowsRemaining_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRowsIndexSpace_;
};

class BuildingResult final {
public:
  BuildingResult();
  ~BuildingResult();

  void init(std::shared_ptr<ClientTable> afterModifies);

    std::optional<TickingUpdate> processNextChunk(BarrageProcessorImpl *owner,
      const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadataSize);

  void reset();

  std::shared_ptr<ClientTable> afterModifies_;
};

bool allEmpty(const std::vector<std::shared_ptr<RowSequence>> &rowSequences);
void assertAllSame(size_t val0, size_t val1, size_t val2);
}  // namespace

class BarrageProcessorImpl final {
public:
  BarrageProcessorImpl(std::shared_ptr<Schema> schema);
  ~BarrageProcessorImpl();

  State state_ = State::AwaitingMetadata;
  AwaitingMetadata awaitingMetadata_;
  AwaitingAdds awaitingAdds_;
  AwaitingModifies awaitingModifies_;
  BuildingResult buildingResult_;

  std::optional<TickingUpdate> processNextChunk(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadataSize);
};
}  // namespace internal

BarrageProcessor::BarrageProcessor() = default;
BarrageProcessor::BarrageProcessor(BarrageProcessor &&other) noexcept = default;
BarrageProcessor &BarrageProcessor::operator=(BarrageProcessor &&other) noexcept = default;
BarrageProcessor::BarrageProcessor(std::shared_ptr<Schema> schema) {
  impl_ = std::make_unique<internal::BarrageProcessorImpl>(std::move(schema));
}
BarrageProcessor::~BarrageProcessor() = default;

std::vector<uint8_t> BarrageProcessor::createSubscriptionRequest(const void *ticketBytes, size_t size) {
  // Make a BarrageMessageWrapper
  // ...Whose payload is a BarrageSubscriptionRequest
  // ......which has BarrageSubscriptionOptions
  flatbuffers::FlatBufferBuilder payloadBuilder(4096);

  auto subOptions = CreateBarrageSubscriptionOptions(payloadBuilder,
      ColumnConversionMode::ColumnConversionMode_Stringify, true, 0, 4096);

  auto ticket = payloadBuilder.CreateVector(static_cast<const int8_t*>(ticketBytes), size);
  auto subreq = CreateBarrageSubscriptionRequest(payloadBuilder, ticket, {}, {}, subOptions);
  payloadBuilder.Finish(subreq);
  // TODO(kosak): fix sad cast
  const auto *payloadp = (int8_t*)payloadBuilder.GetBufferPointer();
  const auto payloadSize = payloadBuilder.GetSize();

  // TODO: I'd really like to just point this buffer backwards to the thing I just created, rather
  // then copying it. But, eh, version 2.
  flatbuffers::FlatBufferBuilder wrapperBuilder(4096);
  auto payload = wrapperBuilder.CreateVector(payloadp, payloadSize);
  auto messageWrapper = CreateBarrageMessageWrapper(wrapperBuilder, deephavenMagicNumber,
      BarrageMessageType::BarrageMessageType_BarrageSubscriptionRequest, payload);
  wrapperBuilder.Finish(messageWrapper);
  auto wrapperBuffer = wrapperBuilder.Release();

  std::vector<uint8_t> result;
  result.resize(wrapperBuffer.size());
  memcpy(result.data(), wrapperBuffer.data(), wrapperBuffer.size());
  return result;
}

std::string BarrageProcessor::createSubscriptionRequestCython(const void *ticketBytes, size_t size) {
  auto vec = createSubscriptionRequest(ticketBytes, size);
  std::string result;
  result.reserve(vec.size());
  for (auto ch : vec) {
    result.push_back((char)ch);
  }
  return result;
}

std::optional<TickingUpdate> BarrageProcessor::processNextChunk(
    const std::vector<std::shared_ptr<ColumnSource>> &sources,
    const std::vector<size_t> &sizes, const void *metadata, size_t metadataSize) {
  std::vector<size_t> begins(sizes.size());  // init to zeroes.
  return impl_->processNextChunk(sources, &begins, sizes, metadata, metadataSize);
}

namespace internal {
BarrageProcessorImpl::BarrageProcessorImpl(std::shared_ptr<Schema> schema) : state_(State::AwaitingMetadata),
    awaitingMetadata_(std::move(schema)) {}
BarrageProcessorImpl::~BarrageProcessorImpl() = default;

std::optional<TickingUpdate>
BarrageProcessorImpl::processNextChunk(const std::vector<std::shared_ptr<ColumnSource>> &sources,
    std::vector<size_t> *begins, const std::vector<size_t> &ends, const void *metadata, size_t metadataSize) {
  switch (state_) {
    case State::AwaitingMetadata:
      return awaitingMetadata_.processNextChunk(this, sources, begins, ends, metadata, metadataSize);
    case State::AwaitingAdds:
      return awaitingAdds_.processNextChunk(this, sources, begins, ends, metadata, metadataSize);
    case State::AwaitingModifies:
      return awaitingModifies_.processNextChunk(this, sources, begins, ends, metadata, metadataSize);
    case State::BuildingResult:
      return buildingResult_.processNextChunk(this, sources, begins, ends, metadata, metadataSize);
    default: {
      auto message = stringf("Unknown state %o", (int)state_);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }
}

namespace {
AwaitingMetadata::AwaitingMetadata(std::shared_ptr<Schema> schema) : numCols_(schema->numCols()),
    tableState_(std::move(schema)) {
}
AwaitingMetadata::~AwaitingMetadata() = default;

std::optional<TickingUpdate> AwaitingMetadata::processNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &sources, std::vector<size_t> *begins,
    const std::vector<size_t> &ends, const void *metadata, size_t/*metadataSize*/) {
  if (metadata == nullptr) {
    const char *message = "Metadata was required here, but none was received";
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  // This metadata buffer comes from code like flightStreamChunk_->app_metadata->data()
  const auto *barrageWrapper = flatbuffers::GetRoot<BarrageMessageWrapper>(metadata);

  if (barrageWrapper->magic() != BarrageProcessor::deephavenMagicNumber) {
    auto message = stringf("Expected magic number %o, got %o", BarrageProcessor::deephavenMagicNumber,
        barrageWrapper->magic());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  if (barrageWrapper->msg_type() !=
      BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata) {
    auto message = stringf("Expected Barrage Message Type %o, got %o",
        BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata, barrageWrapper->msg_type());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  const auto *bmdRaw = barrageWrapper->msg_payload()->data();
  const auto *bmd = flatbuffers::GetRoot<BarrageUpdateMetadata>(bmdRaw);

  DataInput diRemoved(*bmd->removed_rows());
  DataInput diThreeShiftIndexes(*bmd->shift_data());
  DataInput diAdded(*bmd->added_rows());

  auto removedRows = IndexDecoder::readExternalCompressedDelta(&diRemoved);
  auto shiftStartIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftEndIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftDestIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto addedRows = IndexDecoder::readExternalCompressedDelta(&diAdded);

//  streamf(std::cout, "adds=%o, removes=%o, ss=%o, %se=%o, sd=%o\n", *addedRows, *removedRows,
//      *shiftStartIndex, *shiftEndIndex, *shiftDestIndex);

  const auto &modColumnNodes = *bmd->mod_column_nodes();

  std::vector<std::shared_ptr<RowSequence>> perColumnModifies;
  perColumnModifies.reserve(modColumnNodes.size());
  for (size_t i = 0; i < modColumnNodes.size(); ++i) {
    const auto &elt = modColumnNodes.Get(i);
    DataInput diModified(*elt->modified_rows());
    auto modRows = IndexDecoder::readExternalCompressedDelta(&diModified);
    perColumnModifies.push_back(std::move(modRows));
  }

  // Correct order to process Barrage info is:
  // 1. removes
  // 2. shifts
  // 3. adds
  // 4. modifies
  // We have not called with add or modify data yet, but we can do removes and shifts now
  // (steps 1 and 2).
  auto [prev, removedRowsIndexSpace, afterRemoves] = processRemoves(*removedRows);
  tableState_.applyShifts(*shiftStartIndex, *shiftEndIndex, *shiftDestIndex);

  auto addedRowsIndexSpace = tableState_.addKeys(*addedRows);

  owner->state_ = State::AwaitingAdds;
  owner->awaitingAdds_.init(std::move(perColumnModifies), std::move(prev), std::move(removedRowsIndexSpace),
      std::move(afterRemoves), std::move(addedRowsIndexSpace));
  return owner->awaitingAdds_.processNextChunk(owner, sources, begins, ends, nullptr, 0);
}

std::tuple<std::shared_ptr<ClientTable>, std::shared_ptr<RowSequence>, std::shared_ptr<ClientTable>>
AwaitingMetadata::processRemoves(const RowSequence &removedRows) {
  auto prev = tableState_.snapshot();
  // The reason we special-case "empty" is because when the tables are unchanged, we prefer
  // to indicate this via pointer equality (e.g. beforeRemoves == afterRemoves).
  std::shared_ptr<RowSequence> removedRowsIndexSpace;
  std::shared_ptr<ClientTable> afterRemoves;
  if (removedRows.empty()) {
    removedRowsIndexSpace = RowSequence::createEmpty();
    afterRemoves = prev;
  } else {
    removedRowsIndexSpace = tableState_.erase(removedRows);
    afterRemoves = tableState_.snapshot();
  }
  return {std::move(prev), std::move(removedRowsIndexSpace), std::move(afterRemoves)};
}

AwaitingAdds::AwaitingAdds() = default;
AwaitingAdds::~AwaitingAdds() = default;

void AwaitingAdds::reset() {
  *this = AwaitingAdds();
}

void AwaitingAdds::init(std::vector<std::shared_ptr<RowSequence>> perColumnModifies, std::shared_ptr<ClientTable> prev,
    std::shared_ptr<RowSequence> removedRowsIndexSpace, std::shared_ptr<ClientTable> afterRemoves,
    std::shared_ptr<RowSequence> addedRowsIndexSpace) {

  auto result = std::make_shared<AwaitingAdds>();
  perColumnModifies_ = std::move(perColumnModifies);
  prev_ = std::move(prev);
  removedRowsIndexSpace_ = std::move(removedRowsIndexSpace);
  afterRemoves_  = std::move(afterRemoves);
  addedRowsIndexSpace_ = std::move(addedRowsIndexSpace);
}

std::optional<TickingUpdate> AwaitingAdds::processNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &sources,
    std::vector<size_t> *beginsp, const std::vector<size_t> &ends, const void */*metadata*/, size_t /*metadataSize*/) {
  if (firstTime_) {
    firstTime_ = false;

    if (addedRowsIndexSpace_->empty()) {
      addedRowsRemaining_ = RowSequence::createEmpty();

      auto afterAdds = afterRemoves_;
      owner->state_ = State::AwaitingModifies;
      owner->awaitingModifies_.init(std::move(afterAdds));
      return owner->awaitingModifies_.processNextChunk(owner, sources, beginsp, ends, nullptr, 0);
    }

    if (owner->awaitingMetadata_.numCols_ == 0) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("!addedRows.empty() but numCols == 0"));
    }

    // Working copy that is consumed in the iterations of the loop.
    addedRowsRemaining_ = addedRowsIndexSpace_->drop(0);
  }

  auto &begins = *beginsp;
  assertAllSame(sources.size(), begins.size(), ends.size());
  auto numSources = sources.size();

  if (addedRowsRemaining_->empty()) {
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Impossible: addedRowsRemaining is empty"));
  }

  if (begins == ends) {
    // Need more data from caller.
    return {};
  }

  auto chunkSize = ends[0] - begins[0];
  for (size_t i = 1; i != numSources; ++i) {
    auto thisSize = ends[i] - begins[i];
    if (thisSize != chunkSize) {
      auto message = stringf("Chunks have inconsistent sizes: %o vs %o", thisSize, chunkSize);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
  }

  if (addedRowsRemaining_->size() < chunkSize) {
    const char *message = "There is excess data in the chunk";
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  auto indexRowsThisTime = addedRowsRemaining_->take(chunkSize);
  addedRowsRemaining_ = addedRowsRemaining_->drop(chunkSize);
  owner->awaitingMetadata_.tableState_.addData(sources, begins, ends, *indexRowsThisTime);

  // To indicate to the caller that we've consumed the data here (so it can't e.g. be passed on to modify)
  for (size_t i = 0; i != numSources; ++i) {
    begins[i] = ends[i];
  }

  if (!addedRowsRemaining_->empty()) {
    // Need more data from caller.
    return {};
  }

  // No more data remaining. Add phase is done.
  auto afterAdds = owner->awaitingMetadata_.tableState_.snapshot();

  owner->state_ = State::AwaitingModifies;
  owner->awaitingModifies_.init(std::move(afterAdds));
  return owner->awaitingModifies_.processNextChunk(owner, sources, beginsp, ends, nullptr, 0);
}

void AwaitingModifies::reset() {
  *this = AwaitingModifies();
}

void AwaitingModifies::init(std::shared_ptr<ClientTable> afterAdds) {
  afterAdds_ = std::move(afterAdds);
}

AwaitingModifies::AwaitingModifies() = default;
AwaitingModifies::~AwaitingModifies() = default;

std::optional<TickingUpdate> AwaitingModifies::processNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &sources, std::vector<size_t> *beginsp,
    const std::vector<size_t> &ends, const void *metadata, size_t metadataSize) {

  if (firstTime_) {
    firstTime_ = false;

    if (allEmpty(owner->awaitingAdds_.perColumnModifies_)) {
      modifiedRowsIndexSpace_ = {};
      modifiedRowsRemaining_ = {};
      auto afterModifies = afterAdds_;
      owner->state_ = State::BuildingResult;
      owner->buildingResult_.init(std::move(afterModifies));
      return owner->buildingResult_.processNextChunk(owner, sources, beginsp, ends, metadata, metadataSize);
    }

    auto ncols = owner->awaitingMetadata_.numCols_;
    modifiedRowsIndexSpace_ = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
    modifiedRowsRemaining_ = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
    for (size_t i = 0; i < ncols; ++i) {
      auto rs = owner->awaitingMetadata_.tableState_.convertKeysToIndices(*owner->awaitingAdds_.perColumnModifies_[i]);
      modifiedRowsIndexSpace_.push_back(rs->drop(0));  // make copy
      modifiedRowsRemaining_.push_back(std::move(rs));
    }
  }

  if (allEmpty(modifiedRowsRemaining_)) {
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("Impossible: modifiedRowsRemaining is empty"));
  }
  auto &begins = *beginsp;

  if (begins == ends) {
    // Need more data from caller.
    return {};
  }

  auto numSources = sources.size();
  if (numSources > modifiedRowsRemaining_.size()) {
    auto message = stringf("Number of sources (%o) greater than expected (%o)", numSources,
        modifiedRowsRemaining_.size());
    throw std::runtime_error(message);
  }

  for (size_t i = 0; i < numSources; ++i) {
    auto numRowsRemaining = modifiedRowsRemaining_[i]->size();
    auto numRowsAvailable = ends[i] - begins[i];

    if (numRowsAvailable > numRowsRemaining) {
      auto message = stringf("col %o: numRowsAvailable > numRowsRemaining (%o > %o)",
          i, numRowsAvailable, numRowsRemaining);
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }

    if (numRowsAvailable == 0) {
      // Nothing available for this column. Advance to next column.
      continue;
    }

    auto &mr = modifiedRowsRemaining_[i];
    auto rowsAvailable = mr->take(numRowsAvailable);
    mr = mr->drop(numRowsAvailable);

    owner->awaitingMetadata_.tableState_.modifyData(i, *sources[i], begins[i], ends[i], *rowsAvailable);
    begins[i] = ends[i];
  }

  for (const auto &mr : modifiedRowsRemaining_) {
    if (!mr->empty()) {
      // Need more data from caller.
      return {};
    }
  }

  // No more data. Modify phase is done.
  auto afterModifies = owner->awaitingMetadata_.tableState_.snapshot();

  owner->state_ = State::BuildingResult;
  owner->buildingResult_.init(std::move(afterModifies));
  return owner->buildingResult_.processNextChunk(owner, sources, beginsp, ends, nullptr, 0);
}

BuildingResult::BuildingResult() = default;
BuildingResult::~BuildingResult() = default;

void BuildingResult::reset() {
  *this = BuildingResult();
}

void BuildingResult::init(std::shared_ptr<ClientTable> afterModifies) {
  afterModifies_ = std::move(afterModifies);
}

std::optional<TickingUpdate> BuildingResult::processNextChunk(BarrageProcessorImpl *owner,
    const std::vector<std::shared_ptr<ColumnSource>> &/*sources*/,
    std::vector<size_t> *begins, const std::vector<size_t> &ends, const void */*metadata*/, size_t /*metadataSize*/) {
  if (*begins != ends) {
    const char *message = "Barrage logic is done processing but there is leftover caller-provided data.";
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  auto *aa = &owner->awaitingAdds_;
  auto *am = &owner->awaitingModifies_;
  auto result = TickingUpdate(std::move(aa->prev_),
      std::move(aa->removedRowsIndexSpace_), std::move(aa->afterRemoves_),
      std::move(aa->addedRowsIndexSpace_), std::move(am->afterAdds_),
      std::move(am->modifiedRowsIndexSpace_), std::move(afterModifies_));
  aa->reset();
  am->reset();
  this->reset();
  owner->state_ = State::AwaitingMetadata;
  return result;
}

bool allEmpty(const std::vector<std::shared_ptr<RowSequence>> &rowSequences) {
  return std::all_of(rowSequences.begin(), rowSequences.end(), [](const auto &rs) { return rs->empty(); });
}

void assertAllSame(size_t val0, size_t val1, size_t val2) {
  if (val0 != val1 || val0 != val2) {
    auto message = stringf("Sizes differ: %o vs %o vs %o", val0, val1, val2);
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
}
}  // namespace
}  // namespace internal
}  // namespace deephaven::dhcore::ticking
