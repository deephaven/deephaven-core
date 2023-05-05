/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/ticking/barrage_update_processor.h"

#include <functional>
#include <iostream>
#include <memory>
#include "deephaven/dhcore/chunk/chunk_maker.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/immerutil/abstract_flex_vector.h"
#include "deephaven/dhcore/immerutil/immer_column_source.h"
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
using deephaven::dhcore::table::Schema;
using deephaven::dhcore::table::Table;
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
namespace {
class BarrageProcessorImpl final : public BarrageProcessor {
public:
  explicit BarrageProcessorImpl(std::shared_ptr<Schema> schema);
  ~BarrageProcessorImpl() final;

  std::shared_ptr<BarrageUpdateProcessor> startNextUpdate(const void *metadataBuffer, size_t metadataSize);

private:
  size_t numCols_ = 0;
  std::shared_ptr<ImmerTableState> tableState_;
};

class BarrageUpdateProcessorImpl final : public BarrageUpdateProcessor {
public:
  BarrageUpdateProcessorImpl(size_t numCols,
      std::shared_ptr<ImmerTableState> tableState,
      std::shared_ptr<RowSequence> removedRows,
      std::shared_ptr<RowSequence> shiftStartIndex,
      std::shared_ptr<RowSequence> shiftEndIndex,
      std::shared_ptr<RowSequence> shiftDestIndex,
      std::shared_ptr<RowSequence> addedRows,
      std::vector<std::shared_ptr<RowSequence>> perColumnModifies);
  ~BarrageUpdateProcessorImpl() final;

  std::optional<TickingUpdate> processNextSlice(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *beginsp, const std::vector<size_t> &ends) final;

private:
  std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> processRemoves(std::shared_ptr<Table> prev);
  void processShifts();
  bool processAddSlice(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *beginsp, const std::vector<size_t> &ends);
  bool processModifySlice(const std::vector<std::shared_ptr<ColumnSource>> &sources,
      std::vector<size_t> *beginsp, const std::vector<size_t> &ends);
  TickingUpdate calcResult();

  size_t numCols_ = 0;
  std::shared_ptr<ImmerTableState> tableState_;
  std::shared_ptr<RowSequence> removedRows_;
  std::shared_ptr<RowSequence> shiftStartIndex_;
  std::shared_ptr<RowSequence> shiftEndIndex_;
  std::shared_ptr<RowSequence> shiftDestIndex_;
  std::shared_ptr<RowSequence> addedRows_;
  std::vector<std::shared_ptr<RowSequence>> perColumnModifies_;

  std::shared_ptr<Table> prev_;
  std::shared_ptr<RowSequence> removedRowsIndexSpace_;
  std::shared_ptr<Table> afterRemoves_;
  std::shared_ptr<RowSequence> addedRowsRemaining_;
  std::shared_ptr<RowSequence> addedRowsIndexSpace_;
  std::shared_ptr<Table> afterAdds_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRowsRemaining_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRowsIndexSpace_;
  std::shared_ptr<Table> afterModifies_;
};

bool allEmpty(const std::vector<std::shared_ptr<RowSequence>> &rowSequences);
void assertAllSame(size_t val0, size_t val1, size_t val2);
}  // namespace

std::shared_ptr<BarrageProcessor> BarrageProcessor::create(std::shared_ptr<Schema> schema) {
  return std::make_shared<BarrageProcessorImpl>(std::move(schema));
}

BarrageProcessor::BarrageProcessor() = default;
BarrageProcessor::~BarrageProcessor() = default;

std::vector<uint8_t> BarrageProcessor::createBarrageSubscriptionRequest(const std::vector<int8_t> &ticketBytes) {
  // Make a BarrageMessageWrapper
  // ...Whose payload is a BarrageSubscriptionRequest
  // ......which has BarrageSubscriptionOptions
  flatbuffers::FlatBufferBuilder payloadBuilder(4096);

  auto subOptions = CreateBarrageSubscriptionOptions(payloadBuilder,
      ColumnConversionMode::ColumnConversionMode_Stringify, true, 0, 4096);

  auto ticket = payloadBuilder.CreateVector(ticketBytes);
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

BarrageUpdateProcessor::BarrageUpdateProcessor() = default;
BarrageUpdateProcessor::~BarrageUpdateProcessor() = default;

namespace {
BarrageProcessorImpl::BarrageProcessorImpl(std::shared_ptr<Schema> schema) :
    numCols_(schema->columns().size()), tableState_(std::make_shared<ImmerTableState>(std::move(schema))) {
}
BarrageProcessorImpl::~BarrageProcessorImpl() = default;

std::shared_ptr<BarrageUpdateProcessor> BarrageProcessorImpl::startNextUpdate(const void *metadataBuffer,
    size_t/*metadataSize*/) {
  // const auto *barrageWrapperRaw = flightStreamChunk_->app_metadata->data();
  const auto *barrageWrapper = flatbuffers::GetRoot<BarrageMessageWrapper>(metadataBuffer);

  if (barrageWrapper->magic() != deephavenMagicNumber) {
    auto message = stringf("Expected magic number %o, got %o", deephavenMagicNumber,
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

  DataInput diAdded(*bmd->added_rows());
  DataInput diRemoved(*bmd->removed_rows());
  DataInput diThreeShiftIndexes(*bmd->shift_data());

  auto addedRows = IndexDecoder::readExternalCompressedDelta(&diAdded);
  auto removedRows = IndexDecoder::readExternalCompressedDelta(&diRemoved);
  auto shiftStartIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftEndIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftDestIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);

  const auto &modColumnNodes = *bmd->mod_column_nodes();

  std::vector<std::shared_ptr<RowSequence>> perColumnModifies;
  perColumnModifies.reserve(modColumnNodes.size());
  for (size_t i = 0; i < modColumnNodes.size(); ++i) {
    const auto &elt = modColumnNodes.Get(i);
    DataInput diModified(*elt->modified_rows());
    auto modRows = IndexDecoder::readExternalCompressedDelta(&diModified);
    perColumnModifies.push_back(std::move(modRows));
  }
  // or maybe tableState_.snapshot() or something
  return std::make_shared<BarrageUpdateProcessorImpl>(numCols_, tableState_, std::move(removedRows),
      std::move(shiftStartIndex), std::move(shiftEndIndex), std::move(shiftDestIndex), std::move(addedRows),
      std::move(perColumnModifies));
}

BarrageUpdateProcessorImpl::BarrageUpdateProcessorImpl(size_t numCols, std::shared_ptr<ImmerTableState> tableState,
    std::shared_ptr<RowSequence> removedRows, std::shared_ptr<RowSequence> shiftStartIndex,
    std::shared_ptr<RowSequence> shiftEndIndex, std::shared_ptr<RowSequence> shiftDestIndex,
    std::shared_ptr<RowSequence> addedRows,
    std::vector<std::shared_ptr<RowSequence>> perColumnModifies) : numCols_(numCols),
    tableState_(std::move(tableState)), removedRows_(std::move(removedRows)),
    shiftStartIndex_(std::move(shiftStartIndex)), shiftEndIndex_(std::move(shiftEndIndex)),
    shiftDestIndex_(std::move(shiftDestIndex)), addedRows_(std::move(addedRows)),
    perColumnModifies_(std::move(perColumnModifies)) {
  // Correct order to process all this info is:
  // 1. removes
  // 2. shifts
  // 3. adds
  // 4. modifies
  // We can do removes and shifts now.
  prev_ = tableState_->snapshot();
  auto [rris, ar] = processRemoves(prev_);
  removedRowsIndexSpace_ = std::move(rris);
  afterRemoves_ = std::move(ar);
  processShifts();
}

BarrageUpdateProcessorImpl::~BarrageUpdateProcessorImpl() = default;

std::optional<TickingUpdate> BarrageUpdateProcessorImpl::processNextSlice(
    const std::vector<std::shared_ptr<ColumnSource>> &sources, std::vector<size_t> *beginsp,
    const std::vector<size_t> &ends) {
  if (processAddSlice(sources, beginsp, ends)) {
    // Add wants more chunks.
    return {};
  }
  if (processModifySlice(sources, beginsp, ends)) {
    // Modify wants more chunks.
    return {};
  }
  if (*beginsp != ends) {
    const char *message = "Barrage logic is done processing but there is leftover caller-provided data.";
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  return calcResult();
}

std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> BarrageUpdateProcessorImpl::processRemoves(
    std::shared_ptr<Table> prev) {
  // The reason we special-case "empty" is because when the tables are unchanged, we prefer
  // to indicate this via pointer equality (e.g. beforeRemoves == afterRemoves).
  std::shared_ptr<RowSequence> removedRowsIndexSpace;
  std::shared_ptr<Table> afterRemoves;
  if (removedRows_->empty()) {
    removedRowsIndexSpace = RowSequence::createEmpty();
    afterRemoves = std::move(prev);
  } else {
    removedRowsIndexSpace = tableState_->erase(*removedRows_);
    afterRemoves = tableState_->snapshot();
  }
  return {std::move(removedRowsIndexSpace), std::move(afterRemoves)};
}

void BarrageUpdateProcessorImpl::processShifts() {
  tableState_->applyShifts(*shiftStartIndex_, *shiftEndIndex_, *shiftDestIndex_);
}

bool BarrageUpdateProcessorImpl::processAddSlice(const std::vector<std::shared_ptr<ColumnSource>> &sources,
    std::vector<size_t> *beginsp, const std::vector<size_t> &ends) {
  auto &begins = *beginsp;
  assertAllSame(sources.size(), begins.size(), ends.size());
  auto numSources = sources.size();
  if (afterAdds_ == nullptr) {  // first time?
    if (addedRows_->empty()) {
      auto empty = RowSequence::createEmpty();
      addedRowsIndexSpace_ = empty;
      addedRowsRemaining_ = std::move(empty);
      afterAdds_ = afterRemoves_;
      // 'false' means don't need another chunk
      return false;
    }

    if (numCols_ == 0) {
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG("!addedRows.empty() but numCols == 0"));
    }

    addedRowsIndexSpace_ = tableState_->addKeys(*addedRows_);
    // Working copy that is consumed in the iterations of the loop.
    addedRowsRemaining_ = addedRowsIndexSpace_->drop(0);
  }

  if (addedRowsRemaining_->empty()) {
    // false means don't need more data.
    return false;
  }

  if (begins == ends) {
    // Need more data but don't have it.
    return true;
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
  tableState_->addData(sources, begins, ends, *indexRowsThisTime);

  // To indicate to the caller that we've consumed the data here (so it can't e.g. be passed on to modify)
  for (size_t i = 0; i != numSources; ++i) {
    begins[i] = ends[i];
  }

  if (!addedRowsRemaining_->empty()) {
    // Caller to provide more data.
    return true;
  }

  // No more data remaining. Add phase is done.
  afterAdds_ = tableState_->snapshot();
  return false;
}

bool BarrageUpdateProcessorImpl::processModifySlice(const std::vector<std::shared_ptr<ColumnSource>> &sources,
    std::vector<size_t> *beginsp, const std::vector<size_t> &ends) {
  auto &begins = *beginsp;
  assertAllSame(sources.size(), begins.size(), ends.size());
  if (afterModifies_ == nullptr) {  // first time?
    if (allEmpty(perColumnModifies_)) {
      modifiedRowsIndexSpace_ = {};
      modifiedRowsRemaining_ = {};
      afterModifies_ = afterAdds_;
      // 'false' means don't need another chunk
      return false;
    }

    auto ncols = numCols_;
    modifiedRowsIndexSpace_ = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
    modifiedRowsRemaining_ = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
    for (size_t i = 0; i < ncols; ++i) {
      auto rs = tableState_->convertKeysToIndices(*perColumnModifies_[i]);
      modifiedRowsIndexSpace_.push_back(rs->drop(0));  // make copy
      modifiedRowsRemaining_.push_back(std::move(rs));
    }
  }

  if (allEmpty(modifiedRowsRemaining_)) {
    // false means don't need more data.
    return false;
  }

  if (begins == ends) {
    // Need more data
    return true;
  }

  auto numSources = sources.size();
  if (numSources > modifiedRowsRemaining_.size()) {
    auto message = stringf("Number of sources (%o) greater than expected (%o)", numSources, modifiedRowsRemaining_.size());
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

    tableState_->modifyData(i, *sources[i], begins[i], ends[i], *rowsAvailable);
    begins[i] = ends[i];
  }

  for (const auto &mr : modifiedRowsRemaining_) {
    if (!mr->empty()) {
      // Still hungry for more data.
      return true;
    }
  }

  // No more data remaining. Add phase is done.
  afterModifies_ = tableState_->snapshot();
  return false;
}

TickingUpdate BarrageUpdateProcessorImpl::calcResult() {
  return TickingUpdate(std::move(prev_),
          std::move(removedRowsIndexSpace_), std::move(afterRemoves_),
          std::move(addedRowsIndexSpace_), std::move(afterAdds_),
          std::move(modifiedRowsIndexSpace_), std::move(afterModifies_));
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
}  // namespace deephaven::dhcore::ticking
