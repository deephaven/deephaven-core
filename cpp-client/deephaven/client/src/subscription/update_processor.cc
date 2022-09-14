/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/update_processor.h"

#include <iostream>
#include <memory>
#include "deephaven/client/chunk/chunk_filler.h"
#include "deephaven/client/chunk/chunk_maker.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/immerutil/abstract_flex_vector.h"
#include "deephaven/client/immerutil/immer_column_source.h"
#include "deephaven/client/subscription/batch_parser.h"
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
using deephaven::client::subscription::BatchParser;
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
struct ExtractedMetadata {
  ExtractedMetadata(size_t numAdds,
      size_t numMods,
      std::shared_ptr<RowSequence> removedRows,
      std::shared_ptr<RowSequence> shiftStartIndex,
      std::shared_ptr<RowSequence> shiftEndIndex,
      std::shared_ptr<RowSequence> shiftDestIndex,
      std::shared_ptr<RowSequence> addedRows,
      std::vector<std::shared_ptr<RowSequence>> modifiedRows);
  ~ExtractedMetadata();

  size_t numAdds_ = 0;
  size_t numMods_ = 0;
  std::shared_ptr<RowSequence> removedRows_;
  std::shared_ptr<RowSequence> shiftStartIndex_;
  std::shared_ptr<RowSequence> shiftEndIndex_;
  std::shared_ptr<RowSequence> shiftDestIndex_;
  std::shared_ptr<RowSequence> addedRows_;
  std::vector<std::shared_ptr<RowSequence>> modifiedRows_;
};

std::optional<ExtractedMetadata> extractMetadata(
    const arrow::flight::FlightStreamChunk &flightStreamChunk);
}  // namespace

std::shared_ptr<UpdateProcessor> UpdateProcessor::startThread(
    std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs,
    std::shared_ptr<TickingCallback> callback) {
  auto result = std::make_shared<UpdateProcessor>(std::move(fsr),
      std::move(colDefs), std::move(callback));
  std::thread t(&UpdateProcessor::runForever, result);
  t.detach();
  return result;
}

UpdateProcessor::UpdateProcessor(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback) :
    fsr_(std::move(fsr)), colDefs_(std::move(colDefs)), callback_(std::move(callback)),
    cancelled_(false) {}

UpdateProcessor::~UpdateProcessor() = default;

void UpdateProcessor::cancel() {
  cancelled_ = true;
  fsr_->Cancel();
}

void UpdateProcessor::runForever(const std::shared_ptr<UpdateProcessor> &self) {
  std::cerr << "UpdateProcessor is starting.\n";
  std::exception_ptr eptr;
  try {
    self->runForeverHelper();
  } catch (...) {
    // If the thread was been cancelled via explicit user action, then swallow all errors.
    if (!self->cancelled_) {
      eptr = std::current_exception();
      self->callback_->onFailure(eptr);
    }
  }
}

std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> processRemoves(
    const std::shared_ptr<Table> &beforeRemoves, ImmerTableState *state,
    const ExtractedMetadata &md) {
  if (md.removedRows_->empty()) {
    auto empty = RowSequence::createEmpty();
    auto afterRemoves = beforeRemoves;
    return {std::move(empty), std::move(afterRemoves)};
  }

  auto removedRowsIndexSpace = state->erase(*md.removedRows_);
  auto afterRemoves = state->snapshot();
  return {std::move(removedRowsIndexSpace), std::move(afterRemoves)};
}

std::pair<std::shared_ptr<RowSequence>, std::shared_ptr<Table>> processAdds(
    const std::shared_ptr<Table> &beforeAdds, ImmerTableState *state,
    const ExtractedMetadata &md, size_t numCols,
    arrow::flight::FlightStreamReader *fsr, arrow::flight::FlightStreamChunk *flightStreamChunk) {
  if (md.numAdds_ == 0) {
    auto empty = RowSequence::createEmpty();
    auto afterAdds = beforeAdds;
    return {std::move(empty), std::move(afterAdds)};
  }

  auto addedRowsIndexSpace = state->addKeys(*md.addedRows_);

  // Copy everything.
  auto rowsRemaining = addedRowsIndexSpace->take(addedRowsIndexSpace->size());

  auto processAddBatch = [state, &rowsRemaining](
      const std::vector<std::shared_ptr<arrow::Array>> &data) {
    if (data.empty()) {
      return;
    }
    auto size = data[0]->length();
    auto rowsToAddThisTime = rowsRemaining->take(size);
    rowsRemaining = rowsRemaining->drop(size);
    state->addData(data, *rowsToAddThisTime);
  };
  BatchParser::parseBatches(numCols, md.numAdds_, false, fsr, flightStreamChunk,
      processAddBatch);

  if (md.numMods_ != 0) {
    // Currently the FlightStreamReader is pointing to the last add record. We need to advance
    // it so it points to the first mod record.
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->Next(flightStreamChunk)));
  }
  auto afterAdds = state->snapshot();
  return {std::move(addedRowsIndexSpace), std::move(afterAdds)};
}

std::pair<std::vector<std::shared_ptr<RowSequence>>, std::shared_ptr<Table>> processModifies(
    const std::shared_ptr<Table> &beforeModifies, ImmerTableState *state,
    const ExtractedMetadata &md, size_t numCols, arrow::flight::FlightStreamReader *fsr,
    arrow::flight::FlightStreamChunk *flightStreamChunk) {
  if (md.numMods_ == 0) {
    auto afterModifies = beforeModifies;
    return {{}, std::move(afterModifies)};
  }
  auto modifiedRowsIndexSpace = state->modifyKeys(md.modifiedRows_);
  // Local copy of modifiedRowsIndexSpace
  auto keysRemaining = makeReservedVector<std::shared_ptr<RowSequence>>(numCols);
  for (const auto &keys: modifiedRowsIndexSpace) {
    keysRemaining.push_back(keys->take(keys->size()));
  }

  std::vector<std::shared_ptr<RowSequence>> keysToModifyThisTime(numCols);

  auto processModifyBatch = [state, &keysRemaining, &keysToModifyThisTime, numCols](
      const std::vector<std::shared_ptr<arrow::Array>> &data) {
    if (data.size() != numCols) {
      auto message = stringf("Expected % cols, got %o", numCols, data.size());
      throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
    }
    for (size_t i = 0; i < data.size(); ++i) {
      const auto &src = data[i];
      auto &krm = keysRemaining[i];
      keysToModifyThisTime[i] = krm->take(src->length());
      krm = krm->drop(src->length());
    }
    state->modifyData(data, keysToModifyThisTime);
  };

  BatchParser::parseBatches(numCols, md.numMods_, true, fsr, flightStreamChunk, processModifyBatch);
  auto afterModifies = state->snapshot();
  return {std::move(modifiedRowsIndexSpace), std::move(afterModifies)};
}

void UpdateProcessor::runForeverHelper() {
  ImmerTableState state(colDefs_);

  // In this loop we process Arrow Flight messages until error or cancellation.
  arrow::flight::FlightStreamChunk flightStreamChunk;
  while (true) {
    okOrThrow(DEEPHAVEN_EXPR_MSG(fsr_->Next(&flightStreamChunk)));

    // Parse all the metadata out of the Barrage message before we advance the cursor past it.
    auto mdo = extractMetadata(flightStreamChunk);
    if (!mdo.has_value()) {
      continue;
    }
    auto &md = *mdo;

    // Correct order to process all this info is:
    // 1. removes
    // 2. shifts
    // 3. adds
    // 4. modifies

    auto prev = state.snapshot();

    // 1. Removes
    auto [removes, afterRemoves] = processRemoves(prev, &state, md);

    // 2. Shifts
    state.applyShifts(*md.shiftStartIndex_, *md.shiftEndIndex_, *md.shiftDestIndex_);

    // 3. Adds
    auto [adds, afterAdds] = processAdds(afterRemoves, &state, md, colDefs_->size(), fsr_.get(),
        &flightStreamChunk);

    // 4. Modifies
    auto [modifies, afterModifies] = processModifies(afterAdds, &state, md, colDefs_->size(),
        fsr_.get(), &flightStreamChunk);

    // These are convenience copies of the user which might
    TickingUpdate update(std::move(prev),
        std::move(removes), std::move(afterRemoves),
        std::move(adds), std::move(afterAdds),
        std::move(modifies), std::move(afterModifies));
    callback_->onTick(std::move(update));
  }
}

namespace {
std::optional<ExtractedMetadata> extractMetadata(
    const arrow::flight::FlightStreamChunk &flightStreamChunk) {
  if (flightStreamChunk.app_metadata == nullptr) {
    std::cerr << "TODO(kosak) -- unexpected - chunk.app_metdata == nullptr\n";
    return {};
  }

  const auto *barrageWrapperRaw = flightStreamChunk.app_metadata->data();
  const auto *barrageWrapper = flatbuffers::GetRoot<BarrageMessageWrapper>(barrageWrapperRaw);
  if (barrageWrapper->magic() != deephavenMagicNumber) {
    return {};
  }
  if (barrageWrapper->msg_type() !=
      BarrageMessageType::BarrageMessageType_BarrageUpdateMetadata) {
    return {};
  }

  const auto *bmdRaw = barrageWrapper->msg_payload()->data();
  const auto *bmd = flatbuffers::GetRoot<BarrageUpdateMetadata>(bmdRaw);
  auto numAdds = bmd->num_add_batches();
  auto numMods = bmd->num_mod_batches();
  // streamf(std::cerr, "num_add_batches=%o, num_mod_batches=%o\n", numAdds, numMods);
  // streamf(std::cerr, "FYI, my row sequence is currently %o\n", *tickingTable->getRowSequence());

  DataInput diAdded(*bmd->added_rows());
  DataInput diRemoved(*bmd->removed_rows());
  DataInput diThreeShiftIndexes(*bmd->shift_data());

  auto addedRows = IndexDecoder::readExternalCompressedDelta(&diAdded);
  auto removedRows = IndexDecoder::readExternalCompressedDelta(&diRemoved);
  auto shiftStartIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftEndIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);
  auto shiftDestIndex = IndexDecoder::readExternalCompressedDelta(&diThreeShiftIndexes);

  std::vector<std::shared_ptr<RowSequence>> perColumnModifies;
  if (numMods != 0) {
    const auto &modColumnNodes = *bmd->mod_column_nodes();
    perColumnModifies.reserve(modColumnNodes.size());
    for (size_t i = 0; i < modColumnNodes.size(); ++i) {
      const auto &elt = modColumnNodes.Get(i);
      DataInput diModified(*elt->modified_rows());
      auto modRows = IndexDecoder::readExternalCompressedDelta(&diModified);
      perColumnModifies.push_back(std::move(modRows));
    }
  }
//  streamf(std::cerr, "RemovedRows: {%o}\n", *removedRows);
//  streamf(std::cerr, "AddedRows: {%o}\n", *addedRows);
//  streamf(std::cerr, "shift start: {%o}\n", *shiftStartIndex);
//  streamf(std::cerr, "shift end: {%o}\n", *shiftEndIndex);
//  streamf(std::cerr, "shift dest: {%o}\n", *shiftDestIndex);

  return ExtractedMetadata(numAdds,
      numMods,
      std::move(removedRows),
      std::move(shiftStartIndex),
      std::move(shiftEndIndex),
      std::move(shiftDestIndex),
      std::move(addedRows),
      std::move(perColumnModifies));
}

ExtractedMetadata::ExtractedMetadata(size_t numAdds, size_t numMods,
    std::shared_ptr<RowSequence> removedRows, std::shared_ptr<RowSequence> shiftStartIndex,
    std::shared_ptr<RowSequence> shiftEndIndex, std::shared_ptr<RowSequence> shiftDestIndex,
    std::shared_ptr<RowSequence> addedRows,
    std::vector<std::shared_ptr<RowSequence>> modifiedRows) :
    numAdds_(numAdds), numMods_(numMods),
    removedRows_(std::move(removedRows)), shiftStartIndex_(std::move(shiftStartIndex)),
    shiftEndIndex_(std::move(shiftEndIndex)), shiftDestIndex_(std::move(shiftDestIndex)),
    addedRows_(std::move(addedRows)), modifiedRows_(std::move(modifiedRows)) {}

ExtractedMetadata::~ExtractedMetadata() = default;

//ThreadNubbin::ThreadNubbin(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
//    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback) :
//    fsr_(std::move(fsr)), colDefs_(std::move(colDefs)), callback_(std::move(callback)) {}
}  // namespace
}  // namespace deephaven::client::subscription
