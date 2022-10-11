/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/update_processor.h"

#include <functional>
#include <iostream>
#include <memory>
#include "deephaven/client/chunk/chunk_filler.h"
#include "deephaven/client/chunk/chunk_maker.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/immerutil/abstract_flex_vector.h"
#include "deephaven/client/immerutil/immer_column_source.h"
#include "deephaven/client/subscription/classic_table_state.h"
#include "deephaven/client/subscription/immer_table_state.h"
#include "deephaven/client/subscription/index_decoder.h"
#include "deephaven/client/utility/utility.h"
#include "deephaven/client/ticking.h"
#include "deephaven/flatbuf/Barrage_generated.h"

using deephaven::client::chunk::ChunkFiller;
using deephaven::client::chunk::ChunkMaker;
using deephaven::client::chunk::UInt64Chunk;
using deephaven::client::column::MutableColumnSource;
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::immerutil::AbstractFlexVectorBase;
using deephaven::client::table::Table;
using deephaven::client::utility::ColumnDefinitions;
using deephaven::client::utility::makeReservedVector;
using deephaven::client::utility::okOrThrow;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

using io::deephaven::barrage::flatbuf::BarrageMessageType;
using io::deephaven::barrage::flatbuf::BarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::BarrageModColumnMetadata;
using io::deephaven::barrage::flatbuf::BarrageUpdateMetadata;
using io::deephaven::barrage::flatbuf::ColumnConversionMode;
using io::deephaven::barrage::flatbuf::CreateBarrageMessageWrapper;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionOptions;
using io::deephaven::barrage::flatbuf::CreateBarrageSubscriptionRequest;

namespace deephaven::client::subscription {
namespace {
class FlightStreamState final {
public:
  FlightStreamState(size_t numCols, arrow::flight::FlightStreamReader *fsr,
      arrow::flight::FlightStreamChunk *flightStreamChunk, ImmerTableState *tableState);
  ~FlightStreamState();

  void readFirst();
  void readNext();

  void processMetadata();

  std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> processRemoves(
      const std::shared_ptr<Table> &beforeRemoves);

  void processShifts();

  std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> processAdds(
      const std::shared_ptr<Table> &beforeAdds);

  std::pair<std::vector<std::shared_ptr<RowSequence>>, std::shared_ptr<Table>> processModifies(
      const std::shared_ptr<Table> &beforeModifies);

private:
  size_t numCols_ = 0;
  // Does not own.
  arrow::flight::FlightStreamReader *fsr_ = nullptr;
  arrow::flight::FlightStreamChunk *flightStreamChunk_ = nullptr;
  ImmerTableState *tableState_ = nullptr;
  std::vector<size_t> srcChunkOffset_;
  std::vector<size_t> srcChunkSize_;
  std::shared_ptr<RowSequence> removedRows_;
  std::shared_ptr<RowSequence> shiftStartIndex_;
  std::shared_ptr<RowSequence> shiftEndIndex_;
  std::shared_ptr<RowSequence> shiftDestIndex_;
  std::shared_ptr<RowSequence> addedRows_;
  std::vector<std::shared_ptr<RowSequence>> perColumnModifies_;
};
}  // namespace

std::shared_ptr<UpdateProcessor> UpdateProcessor::startThread(
    std::unique_ptr<arrow::flight::FlightStreamWriter> fsw,
    std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs,
    std::shared_ptr<TickingCallback> callback) {
  auto result = std::make_shared<UpdateProcessor>(std::move(fsw), std::move(fsr),
      std::move(colDefs), std::move(callback));
  std::thread t(&UpdateProcessor::runForever, result);
  t.detach();
  return result;
}

UpdateProcessor::UpdateProcessor(std::unique_ptr<arrow::flight::FlightStreamWriter> fsw,
    std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback) :
    fsw_(std::move(fsw)), fsr_(std::move(fsr)), colDefs_(std::move(colDefs)),
    callback_(std::move(callback)), cancelled_(false) {}

UpdateProcessor::~UpdateProcessor() = default;

void UpdateProcessor::cancel() {
  cancelled_ = true;
  fsr_->Cancel();
}

void UpdateProcessor::runForever(const std::shared_ptr<UpdateProcessor> &self) {
  try {
    self->runForeverHelper();
  } catch (...) {
    // If the thread was been cancelled via explicit user action, then swallow all errors.
    if (!self->cancelled_) {
      self->callback_->onFailure(std::current_exception());
    }
  }
}

void UpdateProcessor::runForeverHelper() {
  ImmerTableState tableState(colDefs_);
  auto numCols = colDefs_->vec().size();

  // Reuse the chunk for efficicency
  arrow::flight::FlightStreamChunk flightStreamChunk;
  // In this loop we process Arrow Flight messages until error or cancellation.
  while (true) {
    FlightStreamState flightStreamState(numCols, fsr_.get(), &flightStreamChunk, &tableState);
    // Get the first message of the Barrage batch. The first message always includes metadata,
    // and it may also potentially include Arrow data.
    flightStreamState.readFirst();

    // Correct order to process all this info is:
    // 1. removes
    // 2. shifts
    // 3. adds
    // 4. modifies
    auto prev = tableState.snapshot();

    // 1. Removes
    auto [removes, afterRemoves] = flightStreamState.processRemoves(prev);

    // 2. Shifts
    flightStreamState.processShifts();

    auto [adds, afterAdds] = flightStreamState.processAdds(afterRemoves);

    auto [modifies, afterModifies] = flightStreamState.processModifies(afterAdds);

    TickingUpdate update(std::move(prev),
        std::move(removes), std::move(afterRemoves),
        std::move(adds), std::move(afterAdds),
        std::move(modifies), std::move(afterModifies));
    callback_->onTick(std::move(update));
  }
}

namespace {
FlightStreamState::FlightStreamState(size_t numCols, arrow::flight::FlightStreamReader *fsr,
    arrow::flight::FlightStreamChunk *flightStreamChunk, ImmerTableState *tableState)
    : numCols_(numCols), fsr_(fsr), flightStreamChunk_(flightStreamChunk), tableState_(tableState) {
  srcChunkOffset_.reserve(numCols);
  srcChunkSize_.reserve(numCols);
}

FlightStreamState::~FlightStreamState() = default;

void FlightStreamState::readFirst() {
  readNext();
  processMetadata();
}

void FlightStreamState::readNext() {
  okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(flightStreamChunk_)));

  const auto &cols = flightStreamChunk_->data->columns();

  if (cols.size() != numCols_) {
    auto message = stringf("Expected %o colummns, got %o", numCols_, cols.size());
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }

  srcChunkOffset_.clear();
  srcChunkSize_.clear();
  for (size_t i = 0; i != numCols_; ++i) {
    srcChunkOffset_.push_back(0);
    srcChunkSize_.push_back(cols[i]->length());
  }
}

void FlightStreamState::processMetadata() {
  // Read the metadata from the first chunk.
  if (flightStreamChunk_->app_metadata == nullptr) {
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(
        "Unexpected: flightStreamChunk_.app_metdata == nullptr"));
  }

  const auto *barrageWrapperRaw = flightStreamChunk_->app_metadata->data();
  const auto *barrageWrapper = flatbuffers::GetRoot<BarrageMessageWrapper>(barrageWrapperRaw);

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

  addedRows_ = IndexDecoder::readExternalCompressedDelta(&diAdded);
  removedRows_ = IndexDecoder::readExternalCompressedDelta(&diRemoved);
  shiftStartIndex_ = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  shiftEndIndex_ = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  shiftDestIndex_ = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);

  const auto &modColumnNodes = *bmd->mod_column_nodes();
  perColumnModifies_.reserve(modColumnNodes.size());
  for (size_t i = 0; i < modColumnNodes.size(); ++i) {
    const auto &elt = modColumnNodes.Get(i);
    DataInput diModified(*elt->modified_rows());
    auto modRows = IndexDecoder::readExternalCompressedDelta(&diModified);
    perColumnModifies_.push_back(std::move(modRows));
  }
}

std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> FlightStreamState::processRemoves(
    const std::shared_ptr<Table> &beforeRemoves) {
  // The reason we special-case "empty" is because when the tables are unchanged, we prefer
  // to indicate this via pointer equality (e.g. beforeRemoves == afterRemoves).
  if (removedRows_->empty()) {
    auto empty = RowSequence::createEmpty();
    auto afterRemoves = beforeRemoves;
    return {std::move(empty), std::move(afterRemoves)};
  }

  auto removedRowsIndexSpace = tableState_->erase(*removedRows_);
  auto afterRemoves = tableState_->snapshot();
  return {std::move(removedRowsIndexSpace), std::move(afterRemoves)};
}

void FlightStreamState::processShifts() {
  tableState_->applyShifts(*shiftStartIndex_, *shiftEndIndex_, *shiftDestIndex_);
}

std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> FlightStreamState::processAdds(
    const std::shared_ptr<Table> &beforeAdds) {
  // The reason we special-case "empty" is because when the tables are unchanged, we prefer
  // to indicate this via pointer equality (e.g. beforeAdds == afterAdds).
  if (addedRows_->empty()) {
    auto empty = RowSequence::createEmpty();
    auto afterAdds = beforeAdds;
    return {std::move(empty), std::move(afterAdds)};
  }

  if (numCols_ == 0) {
    throw std::runtime_error("numRows != 0 but numCols == 0");
  }

  auto addedRowsIndexSpace = tableState_->addKeys(*addedRows_);
  // Working copy that is consumed in the iterations of the loop.
  auto addedRowsRemaining = addedRowsIndexSpace->drop(0);
  while (true) {
    auto numRowsToProcess = addedRowsRemaining->size();
    if (numRowsToProcess == 0) {
      break;
    }
    size_t numColsWithData = 0;
    for (size_t i = 0; i != numCols_; ++i) {
      auto thisNumRowsAvailable = srcChunkSize_[i] - srcChunkOffset_[i];
      if (thisNumRowsAvailable != 0) {
        ++numColsWithData;
      }
      numRowsToProcess = std::min(numRowsToProcess, thisNumRowsAvailable);
    }

    if (numRowsToProcess == 0) {
      if (numColsWithData != 0) {
        throw std::runtime_error(
            "Inconsistency in 'Add' case: Some columns in this chunk were exhausted but others "
            "still have data.");
      }
      readNext();
      continue;
    }

    auto indexRowsThisTime = addedRowsRemaining->take(numRowsToProcess);
    addedRowsRemaining = addedRowsRemaining->drop(numRowsToProcess);
    const auto &cols = flightStreamChunk_->data->columns();
    // In the add case, all the srcChunkOffsets have the same values. We do it this way
    // to keep the code simple with respect to the 'modify' case, where they can start to diverge.
    tableState_->addData(cols, srcChunkOffset_[0], *indexRowsThisTime);

    for (size_t i = 0; i < numCols_; ++i) {
      srcChunkOffset_[i] += numRowsToProcess;
    }
  }

  auto afterAdds = tableState_->snapshot();
  return {std::move(addedRowsIndexSpace), std::move(afterAdds)};
}

std::pair<std::vector<std::shared_ptr<RowSequence>>, std::shared_ptr<Table>>
FlightStreamState::processModifies(const std::shared_ptr<Table> &beforeModifies) {
  auto wantModifies = std::any_of(perColumnModifies_.begin(), perColumnModifies_.end(),
      [](const std::shared_ptr<RowSequence> &rs) { return !rs->empty(); });
  if (!wantModifies) {
    auto afterModifies = beforeModifies;
    return {{}, std::move(afterModifies)};
  }

  auto ncols = perColumnModifies_.size();
  auto modifiedRowsIndexSpace = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
  auto modifiedRowsRemaining = makeReservedVector<std::shared_ptr<RowSequence>>(ncols);
  for (size_t i = 0; i < ncols; ++i) {
    auto rs = tableState_->convertKeysToIndices(*perColumnModifies_[i]);
    modifiedRowsIndexSpace.push_back(rs->drop(0));  // make copy
    modifiedRowsRemaining.push_back(std::move(rs));
  }

  while (true) {
    const auto &cols = flightStreamChunk_->data->columns();
    auto completelySatisfied = true;
    for (size_t i = 0; i < ncols; ++i) {
      auto numRowsRemaining = modifiedRowsRemaining[i]->size();
      auto numRowsAvailable = srcChunkSize_[i] - srcChunkOffset_[i];

      if (numRowsAvailable > numRowsRemaining) {
        auto message = stringf("col %o: numRowsAvailable > numRowsRemaining (%o > %o)",
            numRowsAvailable, numRowsRemaining);
        throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
      }

      if (numRowsAvailable < numRowsRemaining) {
        // Set a flag saying to advance Flight and run the outer loop again.
        completelySatisfied = false;
      }

      if (numRowsAvailable == 0) {
        // Nothing needed for this column. Advance to next column.
        continue;
      }

      auto &mr = modifiedRowsRemaining[i];
      auto rowsAvailable = mr->take(numRowsAvailable);
      mr = mr->drop(numRowsAvailable);

      tableState_->modifyData(i, *cols[i], srcChunkOffset_[i], *rowsAvailable);
      // This doesn't *really* matter because it will be reset by the readNext().
      srcChunkOffset_[i] += numRowsAvailable;
    }

    if (completelySatisfied) {
      // All done!
      break;
    }

    readNext();
  }

  auto afterModifies = tableState_->snapshot();
  return {std::move(modifiedRowsIndexSpace), std::move(afterModifies)};
}
}  // namespace
}  // namespace deephaven::client::subscription
